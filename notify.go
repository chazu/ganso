package ganso

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// Notification is a single message retrieved from the _ganso_notifications table.
type Notification struct {
	ID        int64
	Channel   string
	Payload   json.RawMessage
	CreatedAt string // ISO-8601 timestamp as stored in SQLite
}

// Listener receives notifications on a given channel. Create one via
// Database.Listen. Call Next in a loop to consume notifications and Close
// when done.
type Listener struct {
	db           *Database
	channel      string
	lastID       int64
	updateCh     <-chan struct{}
	unsubscribe  func()
	fallbackPoll time.Duration
	closed       atomic.Bool
}

// Listen creates a Listener for the given notification channel. The Listener
// subscribes to the UpdateWatcher for low-latency wake-ups and falls back to
// polling at the configured interval.
//
// Notifications that already exist at the time Listen is called are not
// replayed; only new notifications are delivered.
func (db *Database) Listen(channel string, opts ...ListenOption) (*Listener, error) {
	if db.closed.Load() {
		return nil, ErrClosed
	}

	cfg := defaultListenConfig()
	for _, o := range opts {
		o(&cfg)
	}

	// Subscribe to the watcher for commit signals.
	updateCh, unsubscribe := db.watcher.Subscribe()

	// Find the current max notification ID so we only deliver new ones.
	lastID, err := db.maxNotificationID(channel)
	if err != nil {
		unsubscribe()
		return nil, fmt.Errorf("ganso: listen: %w", err)
	}

	return &Listener{
		db:           db,
		channel:      channel,
		lastID:       lastID,
		updateCh:     updateCh,
		unsubscribe:  unsubscribe,
		fallbackPoll: cfg.fallbackPoll,
	}, nil
}

// maxNotificationID returns the highest notification ID for a channel, or 0 if
// none exist.
func (db *Database) maxNotificationID(channel string) (int64, error) {
	ctx := context.Background()
	conn, err := db.pool.Take(ctx)
	if err != nil {
		return 0, err
	}
	defer db.pool.Put(conn)

	var maxID int64
	err = sqlitex.Execute(conn,
		`SELECT COALESCE(MAX(id), 0) FROM _ganso_notifications WHERE channel = ?`,
		&sqlitex.ExecOptions{
			Args: []any{channel},
			ResultFunc: func(stmt *sqlite.Stmt) error {
				maxID = stmt.ColumnInt64(0)
				return nil
			},
		},
	)
	return maxID, err
}

// Next blocks until the next notification is available on this Listener's
// channel, the context is cancelled, or the Listener is closed.
func (l *Listener) Next(ctx context.Context) (Notification, error) {
	for {
		if l.closed.Load() {
			return Notification{}, ErrClosed
		}

		// Try to fetch the next unseen notification.
		n, ok, err := l.poll(ctx)
		if err != nil {
			return Notification{}, err
		}
		if ok {
			l.lastID = n.ID
			return n, nil
		}

		// No notification available; wait for a wake signal.
		fallback := time.NewTimer(l.fallbackPoll)
		select {
		case <-ctx.Done():
			fallback.Stop()
			return Notification{}, ctx.Err()
		case <-l.updateCh:
			fallback.Stop()
			// Watcher detected a commit; loop back and poll.
		case <-fallback.C:
			// Fallback timer fired; loop back and poll.
		}
	}
}

// poll queries for a single notification with id > lastID on the listener's
// channel. Returns (notification, true, nil) if found, or (zero, false, nil)
// if none are available.
func (l *Listener) poll(ctx context.Context) (Notification, bool, error) {
	if l.db.closed.Load() {
		return Notification{}, false, ErrClosed
	}

	conn, err := l.db.pool.Take(ctx)
	if err != nil {
		return Notification{}, false, fmt.Errorf("ganso: listener poll: %w", err)
	}
	defer l.db.pool.Put(conn)

	var n Notification
	var found bool

	err = sqlitex.Execute(conn,
		`SELECT id, channel, payload, created_at
		 FROM _ganso_notifications
		 WHERE channel = ? AND id > ?
		 ORDER BY id
		 LIMIT 1`,
		&sqlitex.ExecOptions{
			Args: []any{l.channel, l.lastID},
			ResultFunc: func(stmt *sqlite.Stmt) error {
				n.ID = stmt.ColumnInt64(0)
				n.Channel = stmt.ColumnText(1)
				n.Payload = json.RawMessage(stmt.ColumnText(2))
				n.CreatedAt = stmt.ColumnText(3)
				found = true
				return nil
			},
		},
	)
	if err != nil {
		return Notification{}, false, fmt.Errorf("ganso: listener poll: %w", err)
	}
	return n, found, nil
}

// Close stops the Listener. Any blocked Next call will return ErrClosed.
func (l *Listener) Close() {
	if l.closed.CompareAndSwap(false, true) {
		l.unsubscribe()
	}
}

// Notify inserts a notification and returns its ID. This replaces the simpler
// version in ganso.go that does not return an ID. It uses RETURNING to
// retrieve the auto-generated rowid.
func (tx *Tx) NotifyReturningID(channel, payload string) (int64, error) {
	var id int64
	err := sqlitex.Execute(tx.conn,
		`INSERT INTO _ganso_notifications (channel, payload) VALUES (?, ?) RETURNING id`,
		&sqlitex.ExecOptions{
			Args: []any{channel, payload},
			ResultFunc: func(stmt *sqlite.Stmt) error {
				id = stmt.ColumnInt64(0)
				return nil
			},
		},
	)
	if err != nil {
		return 0, fmt.Errorf("ganso: notify: %w", err)
	}
	return id, nil
}

// PruneNotifications removes old notifications according to the given options.
// By default it prunes notifications older than 7 days and keeps at most 1000.
func (db *Database) PruneNotifications(opts ...PruneOption) error {
	cfg := defaultPruneConfig()
	for _, o := range opts {
		o(&cfg)
	}

	return db.WithTx(func(tx *Tx) error {
		// Phase 1: delete by age.
		if cfg.olderThan > 0 {
			cutoff := time.Now().Add(-cfg.olderThan).UTC().Format("2006-01-02T15:04:05.000Z")
			err := sqlitex.Execute(tx.conn,
				`DELETE FROM _ganso_notifications WHERE created_at < ?`,
				&sqlitex.ExecOptions{
					Args: []any{cutoff},
				},
			)
			if err != nil {
				return fmt.Errorf("ganso: prune by age: %w", err)
			}
		}

		// Phase 2: keep at most maxKeep rows (delete oldest beyond the limit).
		if cfg.maxKeep > 0 {
			err := sqlitex.Execute(tx.conn,
				`DELETE FROM _ganso_notifications
				 WHERE id NOT IN (
				     SELECT id FROM _ganso_notifications
				     ORDER BY id DESC
				     LIMIT ?
				 )`,
				&sqlitex.ExecOptions{
					Args: []any{cfg.maxKeep},
				},
			)
			if err != nil {
				return fmt.Errorf("ganso: prune by count: %w", err)
			}
		}

		return nil
	})
}
