package honker

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// Lock represents an acquired distributed lock backed by SQLite.
type Lock struct {
	db       *Database
	name     string
	ttl      int64 // seconds
	owner    string
	acquired bool
}

// Release releases the lock. Only the owner that acquired it can release it.
func (l *Lock) Release() error {
	if !l.acquired {
		return nil
	}
	return l.db.WithTx(func(tx *Tx) error {
		return sqlitex.Execute(tx.conn,
			`DELETE FROM _honker_locks WHERE name = ? AND owner = ?`,
			&sqlitex.ExecOptions{
				Args: []any{l.name, l.owner},
			},
		)
	})
}

// Lock acquires a named distributed lock. Returns ErrLockHeld if the lock is
// already held by another owner. The ctx parameter is accepted for API
// consistency but is not currently used for cancellation during acquisition.
func (db *Database) Lock(ctx context.Context, name string, opts ...LockOption) (*Lock, error) {
	cfg := defaultLockConfig()
	for _, o := range opts {
		o(&cfg)
	}
	owner := cfg.owner
	if owner == "" {
		owner = uuid.New().String()
	}
	ttlSec := int64(cfg.ttl.Seconds())
	if ttlSec <= 0 {
		ttlSec = 60
	}

	var lock *Lock
	err := db.WithTx(func(tx *Tx) error {
		// 1. Delete expired locks for this name.
		if err := sqlitex.Execute(tx.conn,
			`DELETE FROM _honker_locks WHERE name = ? AND expires_at < unixepoch()`,
			&sqlitex.ExecOptions{
				Args: []any{name},
			},
		); err != nil {
			return fmt.Errorf("delete expired: %w", err)
		}

		// 2. Try to insert the lock.
		if err := sqlitex.Execute(tx.conn,
			`INSERT OR IGNORE INTO _honker_locks (name, owner, expires_at) VALUES (?, ?, unixepoch() + ?)`,
			&sqlitex.ExecOptions{
				Args: []any{name, owner, ttlSec},
			},
		); err != nil {
			return fmt.Errorf("insert lock: %w", err)
		}

		// 3. Check who owns the lock.
		var currentOwner string
		if err := sqlitex.Execute(tx.conn,
			`SELECT owner FROM _honker_locks WHERE name = ?`,
			&sqlitex.ExecOptions{
				Args: []any{name},
				ResultFunc: func(stmt *sqlite.Stmt) error {
					currentOwner = stmt.ColumnText(0)
					return nil
				},
			},
		); err != nil {
			return fmt.Errorf("check owner: %w", err)
		}

		if currentOwner == owner {
			lock = &Lock{
				db:       db,
				name:     name,
				ttl:      ttlSec,
				owner:    owner,
				acquired: true,
			}
			return nil
		}
		return ErrLockHeld
	})
	if err != nil {
		return nil, err
	}
	return lock, nil
}

// TryLock is a convenience wrapper around Lock using context.Background().
func (db *Database) TryLock(name string, opts ...LockOption) (*Lock, error) {
	return db.Lock(context.Background(), name, opts...)
}

// WithLock acquires the named lock, executes fn, then releases the lock.
func (db *Database) WithLock(ctx context.Context, name string, fn func() error, opts ...LockOption) error {
	l, err := db.Lock(ctx, name, opts...)
	if err != nil {
		return err
	}
	defer l.Release()
	return fn()
}

// TryRateLimit checks whether the caller is within the rate limit. It returns
// true if the request is allowed (count was incremented), false if the limit
// has been reached.
//
// The rate limit uses fixed windows: limit requests per perSec-second window.
func (db *Database) TryRateLimit(name string, limit, perSec int) (bool, error) {
	var allowed bool
	err := db.WithTx(func(tx *Tx) error {
		// UPSERT: insert with count=1, or increment only if under limit.
		if err := sqlitex.Execute(tx.conn,
			`INSERT INTO _honker_rate_limits (name, window_start, count)
			 VALUES (?, (unixepoch() / ?) * ?, 1)
			 ON CONFLICT(name, window_start) DO UPDATE SET count = count + 1
			 WHERE count < ?`,
			&sqlitex.ExecOptions{
				Args: []any{name, perSec, perSec, limit},
			},
		); err != nil {
			return fmt.Errorf("rate limit upsert: %w", err)
		}

		// Check if the upsert actually changed a row.
		var changed int64
		if err := sqlitex.Execute(tx.conn,
			`SELECT changes()`,
			&sqlitex.ExecOptions{
				ResultFunc: func(stmt *sqlite.Stmt) error {
					changed = stmt.ColumnInt64(0)
					return nil
				},
			},
		); err != nil {
			return fmt.Errorf("rate limit changes: %w", err)
		}
		allowed = changed > 0
		return nil
	})
	return allowed, err
}

// SweepRateLimits removes rate limit windows older than olderThanSec seconds.
// It returns the number of rows deleted.
func (db *Database) SweepRateLimits(olderThanSec int) (int, error) {
	var count int
	err := db.WithTx(func(tx *Tx) error {
		if err := sqlitex.Execute(tx.conn,
			`DELETE FROM _honker_rate_limits WHERE window_start < unixepoch() - ?`,
			&sqlitex.ExecOptions{
				Args: []any{olderThanSec},
			},
		); err != nil {
			return fmt.Errorf("sweep rate limits: %w", err)
		}

		var changed int64
		if err := sqlitex.Execute(tx.conn,
			`SELECT changes()`,
			&sqlitex.ExecOptions{
				ResultFunc: func(stmt *sqlite.Stmt) error {
					changed = stmt.ColumnInt64(0)
					return nil
				},
			},
		); err != nil {
			return fmt.Errorf("sweep changes: %w", err)
		}
		count = int(changed)
		return nil
	})
	return count, err
}
