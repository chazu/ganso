package honker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// Stream is a durable, append-only event stream backed by SQLite.
type Stream struct {
	db   *Database
	Name string
}

// Event is a single event from a Stream.
type Event struct {
	Offset    int64
	Topic     string
	Key       string
	Payload   json.RawMessage
	CreatedAt string
}

// publishOnConn is the shared helper that inserts an event and its notification
// on a given connection (either the dedicated writer or a transaction conn).
func (s *Stream) publishOnConn(conn *sqlite.Conn, payloadBytes []byte, cfg publishConfig) (int64, error) {
	var offset int64

	err := sqlitex.Execute(conn,
		`INSERT INTO _honker_stream (topic, key, payload) VALUES (?, ?, ?) RETURNING "offset"`,
		&sqlitex.ExecOptions{
			Args: []any{s.Name, cfg.key, string(payloadBytes)},
			ResultFunc: func(stmt *sqlite.Stmt) error {
				offset = stmt.ColumnInt64(0)
				return nil
			},
		},
	)
	if err != nil {
		return 0, fmt.Errorf("honker: stream publish: %w", err)
	}

	// Insert notification so watcher-based subscribers wake up.
	channel := "honker:stream:" + s.Name
	err = sqlitex.Execute(conn,
		`INSERT INTO _honker_notifications (channel, payload) VALUES (?, 'new')`,
		&sqlitex.ExecOptions{
			Args: []any{channel},
		},
	)
	if err != nil {
		return 0, fmt.Errorf("honker: stream publish notify: %w", err)
	}

	return offset, nil
}

// Publish appends an event to the stream and returns its monotonically
// increasing offset. The payload is JSON-marshalled before storage.
func (s *Stream) Publish(payload any, opts ...PublishOption) (int64, error) {
	if s.db.closed.Load() {
		return 0, ErrClosed
	}

	var cfg publishConfig
	for _, o := range opts {
		o(&cfg)
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("honker: marshal payload: %w", err)
	}

	s.db.writerMu.Lock()
	defer s.db.writerMu.Unlock()

	endFn, err := sqlitex.ImmediateTransaction(s.db.writer)
	if err != nil {
		return 0, fmt.Errorf("honker: begin tx: %w", err)
	}
	defer endFn(&err)

	offset, err := s.publishOnConn(s.db.writer, payloadBytes, cfg)
	return offset, err
}

// PublishTx appends an event to the stream within an existing transaction.
// The caller must not hold writerMu; the transaction already owns the writer.
func (s *Stream) PublishTx(tx *Tx, payload any, opts ...PublishOption) (int64, error) {
	var cfg publishConfig
	for _, o := range opts {
		o(&cfg)
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("honker: marshal payload: %w", err)
	}

	return s.publishOnConn(tx.conn, payloadBytes, cfg)
}

// Subscribe returns a channel that yields events as they arrive on the stream.
// The channel is closed when ctx is cancelled. If a consumer name is provided
// via Consumer(), the subscription resumes from the last saved offset.
func (s *Stream) Subscribe(ctx context.Context, opts ...SubscribeOption) <-chan Event {
	cfg := defaultSubscribeConfig()
	for _, o := range opts {
		o(&cfg)
	}

	// Subscribe to the watcher BEFORE reading, so we never miss a commit
	// that happens between our read and our wait.
	wakeCh, unsub := s.db.watcher.Subscribe()

	ch := make(chan Event, 64)

	go func() {
		defer func() {
			// Recover from any panic to guarantee channel closure and cleanup.
			recover()
			close(ch)
			unsub()
		}()

		// Resolve starting offset.
		cursor := cfg.fromOffset
		if cfg.consumer != "" && cfg.fromOffset == 0 {
			saved, err := s.GetOffset(ctx, cfg.consumer)
			if err != nil {
				return
			}
			if saved > cursor {
				cursor = saved
			}
		}

		eventsSinceSave := 0
		lastSaveTime := time.Now()
		const batchSize = 100

		for {
			// 1. Read a batch from the reader pool.
			events, err := s.readBatch(ctx, cursor, batchSize)
			if err != nil {
				return
			}

			// 2. Send each event on the channel.
			for _, ev := range events {
				select {
				case ch <- ev:
					cursor = ev.Offset
					eventsSinceSave++

					// 3. Periodically save the consumer offset.
					if cfg.consumer != "" &&
						(eventsSinceSave >= cfg.saveEveryN ||
							time.Since(lastSaveTime) >= cfg.saveEveryDur) {
						_ = s.SaveOffset(cfg.consumer, cursor)
						eventsSinceSave = 0
						lastSaveTime = time.Now()
					}
				case <-ctx.Done():
					// Save final offset on exit.
					if cfg.consumer != "" && eventsSinceSave > 0 {
						_ = s.SaveOffset(cfg.consumer, cursor)
					}
					return
				}
			}

			// 4. If the batch was full, loop immediately for more.
			if len(events) == batchSize {
				continue
			}

			// 5. Batch was empty or partial: wait for new events.
			select {
			case <-ctx.Done():
				if cfg.consumer != "" && eventsSinceSave > 0 {
					_ = s.SaveOffset(cfg.consumer, cursor)
				}
				return
			case <-wakeCh:
				// Watcher detected a commit; loop back and read.
			}
		}
	}()

	return ch
}

// SaveOffset persists a consumer's current offset using a monotonic upsert
// (the stored offset only moves forward).
func (s *Stream) SaveOffset(consumer string, offset int64) error {
	if s.db.closed.Load() {
		return ErrClosed
	}

	s.db.writerMu.Lock()
	defer s.db.writerMu.Unlock()

	return sqlitex.Execute(s.db.writer,
		`INSERT INTO _honker_stream_consumers (name, topic, "offset")
		 VALUES (?, ?, ?)
		 ON CONFLICT (name, topic) DO UPDATE SET "offset" = MAX(excluded."offset", _honker_stream_consumers."offset")`,
		&sqlitex.ExecOptions{
			Args: []any{consumer, s.Name, offset},
		},
	)
}

// GetOffset retrieves a consumer's last saved offset. Returns 0 if the
// consumer has never been saved.
func (s *Stream) GetOffset(ctx context.Context, consumer string) (int64, error) {
	if s.db.closed.Load() {
		return 0, ErrClosed
	}

	conn, err := s.db.pool.Take(ctx)
	if err != nil {
		return 0, fmt.Errorf("honker: take reader conn: %w", err)
	}
	defer s.db.pool.Put(conn)

	var offset int64
	err = sqlitex.Execute(conn,
		`SELECT "offset" FROM _honker_stream_consumers WHERE name = ? AND topic = ?`,
		&sqlitex.ExecOptions{
			Args: []any{consumer, s.Name},
			ResultFunc: func(stmt *sqlite.Stmt) error {
				offset = stmt.ColumnInt64(0)
				return nil
			},
		},
	)
	if err != nil {
		return 0, fmt.Errorf("honker: stream get offset: %w", err)
	}
	return offset, nil
}

// readBatch reads up to limit events from the stream with offset > afterOffset
// using a connection from the reader pool.
func (s *Stream) readBatch(ctx context.Context, afterOffset int64, limit int) ([]Event, error) {
	conn, err := s.db.pool.Take(ctx)
	if err != nil {
		return nil, fmt.Errorf("honker: take reader conn: %w", err)
	}
	defer s.db.pool.Put(conn)

	var events []Event
	err = sqlitex.Execute(conn,
		`SELECT "offset", topic, key, payload, created_at
		 FROM _honker_stream
		 WHERE topic = ? AND "offset" > ?
		 ORDER BY "offset"
		 LIMIT ?`,
		&sqlitex.ExecOptions{
			Args: []any{s.Name, afterOffset, limit},
			ResultFunc: func(stmt *sqlite.Stmt) error {
				events = append(events, Event{
					Offset:    stmt.ColumnInt64(0),
					Topic:     stmt.ColumnText(1),
					Key:       stmt.ColumnText(2),
					Payload:   json.RawMessage(stmt.ColumnText(3)),
					CreatedAt: stmt.ColumnText(4),
				})
				return nil
			},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("honker: stream read: %w", err)
	}
	return events, nil
}
