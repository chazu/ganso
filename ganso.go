package ganso

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// Database is the central handle for all Ganso primitives.
// It owns a dedicated writer connection and a pool of reader connections.
type Database struct {
	path     string
	pool     *sqlitex.Pool
	writer   *sqlite.Conn
	writerMu sync.Mutex
	watcher  *UpdateWatcher

	pollInterval time.Duration

	queues  sync.Map // string -> *Queue
	streams sync.Map // string -> *Stream
	outboxes sync.Map // string -> *Outbox

	closed atomic.Bool
}

// Tx wraps a write transaction on the dedicated writer connection.
type Tx struct {
	conn *sqlite.Conn
	db   *Database
}

// Open opens (or creates) a Ganso database at the given path.
func Open(path string, opts ...OpenOption) (*Database, error) {
	cfg := defaultOpenConfig()
	for _, o := range opts {
		o(&cfg)
	}

	// Open the dedicated writer connection.
	writer, err := sqlite.OpenConn(path,
		sqlite.OpenReadWrite|sqlite.OpenCreate|sqlite.OpenWAL|sqlite.OpenURI,
	)
	if err != nil {
		return nil, fmt.Errorf("ganso: open writer: %w", err)
	}

	// Apply pragmas on writer (must be outside any transaction).
	if err := applyPragmas(writer); err != nil {
		writer.Close()
		return nil, fmt.Errorf("ganso: writer pragmas: %w", err)
	}

	// Bootstrap the schema on the writer.
	if err := bootstrapSchema(writer); err != nil {
		writer.Close()
		return nil, fmt.Errorf("ganso: bootstrap schema: %w", err)
	}

	// Open the reader pool.
	pool, err := sqlitex.NewPool(path, sqlitex.PoolOptions{
		Flags:    sqlite.OpenReadOnly | sqlite.OpenWAL | sqlite.OpenURI,
		PoolSize: cfg.maxReaders,
		PrepareConn: func(conn *sqlite.Conn) error {
			return applyPragmas(conn)
		},
	})
	if err != nil {
		writer.Close()
		return nil, fmt.Errorf("ganso: open reader pool: %w", err)
	}

	db := &Database{
		path:         path,
		pool:         pool,
		writer:       writer,
		pollInterval: cfg.pollInterval,
	}

	// Create the update watcher (nil-safe; callers check before use).
	db.watcher = NewUpdateWatcher(db)

	return db, nil
}

// Close shuts down the database, releasing all connections.
func (db *Database) Close() error {
	if !db.closed.CompareAndSwap(false, true) {
		return ErrClosed
	}

	if db.watcher != nil {
		db.watcher.Stop()
	}

	var firstErr error
	if err := db.pool.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := db.writer.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// WithTx executes fn inside an IMMEDIATE write transaction on the dedicated
// writer connection. The transaction is committed if fn returns nil and rolled
// back otherwise.
func (db *Database) WithTx(fn func(tx *Tx) error) error {
	if db.closed.Load() {
		return ErrClosed
	}

	db.writerMu.Lock()
	defer db.writerMu.Unlock()

	endFn, err := sqlitex.ImmediateTransaction(db.writer)
	if err != nil {
		return fmt.Errorf("ganso: begin tx: %w", err)
	}

	txErr := fn(&Tx{conn: db.writer, db: db})
	endFn(&txErr)
	return txErr
}

// Execute runs a single SQL statement with named parameters inside a transaction.
func (tx *Tx) Execute(query string, named map[string]any) error {
	return sqlitex.Execute(tx.conn, query, &sqlitex.ExecOptions{
		Named: named,
	})
}

// Query runs a SQL query with named parameters and collects all result rows.
func (tx *Tx) Query(query string, named map[string]any) ([]map[string]any, error) {
	var rows []map[string]any
	err := sqlitex.Execute(tx.conn, query, &sqlitex.ExecOptions{
		Named: named,
		ResultFunc: func(stmt *sqlite.Stmt) error {
			row := make(map[string]any, stmt.ColumnCount())
			for i := 0; i < stmt.ColumnCount(); i++ {
				name := stmt.ColumnName(i)
				switch stmt.ColumnType(i) {
				case sqlite.TypeInteger:
					row[name] = stmt.ColumnInt64(i)
				case sqlite.TypeFloat:
					row[name] = stmt.ColumnFloat(i)
				case sqlite.TypeText:
					row[name] = stmt.ColumnText(i)
				case sqlite.TypeBlob:
					buf := make([]byte, stmt.ColumnLen(i))
					stmt.ColumnBytes(i, buf)
					row[name] = buf
				case sqlite.TypeNull:
					row[name] = nil
				}
			}
			rows = append(rows, row)
			return nil
		},
	})
	return rows, err
}

// Notify inserts a notification into the _ganso_notifications table.
func (tx *Tx) Notify(channel, payload string) error {
	return tx.Execute(
		`INSERT INTO _ganso_notifications (channel, payload) VALUES (:channel, :payload)`,
		map[string]any{
			":channel": channel,
			":payload": payload,
		},
	)
}

// Query takes a reader connection from the pool, executes the query, and
// returns all result rows as []map[string]any.
func (db *Database) Query(ctx context.Context, query string, named map[string]any) ([]map[string]any, error) {
	if db.closed.Load() {
		return nil, ErrClosed
	}

	conn, err := db.pool.Take(ctx)
	if err != nil {
		return nil, fmt.Errorf("ganso: take reader conn: %w", err)
	}
	defer db.pool.Put(conn)

	var rows []map[string]any
	err = sqlitex.Execute(conn, query, &sqlitex.ExecOptions{
		Named: named,
		ResultFunc: func(stmt *sqlite.Stmt) error {
			row := make(map[string]any, stmt.ColumnCount())
			for i := 0; i < stmt.ColumnCount(); i++ {
				name := stmt.ColumnName(i)
				switch stmt.ColumnType(i) {
				case sqlite.TypeInteger:
					row[name] = stmt.ColumnInt64(i)
				case sqlite.TypeFloat:
					row[name] = stmt.ColumnFloat(i)
				case sqlite.TypeText:
					row[name] = stmt.ColumnText(i)
				case sqlite.TypeBlob:
					buf := make([]byte, stmt.ColumnLen(i))
					stmt.ColumnBytes(i, buf)
					row[name] = buf
				case sqlite.TypeNull:
					row[name] = nil
				}
			}
			rows = append(rows, row)
			return nil
		},
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// Queue returns a named Queue, creating it on first access. The Queue is
// memoized for the lifetime of the Database.
func (db *Database) Queue(name string, opts ...QueueOption) *Queue {
	cfg := defaultQueueConfig()
	for _, o := range opts {
		o(&cfg)
	}

	actual, _ := db.queues.LoadOrStore(name, &Queue{
		db:                db,
		Name:              name,
		visibilityTimeout: cfg.visibilityTimeout,
		maxAttempts:       cfg.maxAttempts,
	})
	return actual.(*Queue)
}

// Stream returns a named Stream, creating it on first access. The Stream is
// memoized for the lifetime of the Database.
func (db *Database) Stream(name string) *Stream {
	actual, _ := db.streams.LoadOrStore(name, &Stream{
		db:   db,
		Name: name,
	})
	return actual.(*Stream)
}

// Outbox returns a named Outbox, creating it on first access. The Outbox is
// memoized for the lifetime of the Database.
func (db *Database) Outbox(name string, opts ...OutboxOption) *Outbox {
	cfg := defaultOutboxConfig()
	for _, o := range opts {
		o(&cfg)
	}

	actual, _ := db.outboxes.LoadOrStore(name, &Outbox{
		db:                db,
		Name:              name,
		maxAttempts:       cfg.maxAttempts,
		baseBackoff:       cfg.baseBackoff,
		visibilityTimeout: cfg.visibilityTimeout,
	})
	return actual.(*Outbox)
}
