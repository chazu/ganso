package ganso

import (
	"context"
	"fmt"
	"time"

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
			`DELETE FROM _ganso_locks WHERE name = ? AND owner = ?`,
			&sqlitex.ExecOptions{
				Args: []any{l.name, l.owner},
			},
		)
	})
}

// tryLockOnce attempts to acquire the lock without blocking.
func (db *Database) tryLockOnce(name string, cfg lockConfig) (*Lock, error) {
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
		if err := sqlitex.Execute(tx.conn,
			`DELETE FROM _ganso_locks WHERE name = ? AND expires_at < unixepoch()`,
			&sqlitex.ExecOptions{
				Args: []any{name},
			},
		); err != nil {
			return fmt.Errorf("delete expired: %w", err)
		}

		if err := sqlitex.Execute(tx.conn,
			`INSERT OR IGNORE INTO _ganso_locks (name, owner, expires_at) VALUES (?, ?, unixepoch() + ?)`,
			&sqlitex.ExecOptions{
				Args: []any{name, owner, ttlSec},
			},
		); err != nil {
			return fmt.Errorf("insert lock: %w", err)
		}

		var currentOwner string
		if err := sqlitex.Execute(tx.conn,
			`SELECT owner FROM _ganso_locks WHERE name = ?`,
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

// TryLock attempts to acquire a named lock without blocking.
// Returns ErrLockHeld if the lock is already held by another owner.
func (db *Database) TryLock(name string, opts ...LockOption) (*Lock, error) {
	cfg := defaultLockConfig()
	for _, o := range opts {
		o(&cfg)
	}
	return db.tryLockOnce(name, cfg)
}

// Lock acquires a named lock, blocking until available or ctx is cancelled.
func (db *Database) Lock(ctx context.Context, name string, opts ...LockOption) (*Lock, error) {
	cfg := defaultLockConfig()
	for _, o := range opts {
		o(&cfg)
	}

	l, err := db.tryLockOnce(name, cfg)
	if err == nil {
		return l, nil
	}
	if err != ErrLockHeld {
		return nil, err
	}

	wakeCh, unsub := db.watcher.Subscribe()
	defer unsub()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-wakeCh:
		case <-time.After(time.Second):
		}

		l, err := db.tryLockOnce(name, cfg)
		if err == nil {
			return l, nil
		}
		if err != ErrLockHeld {
			return nil, err
		}
	}
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
			`INSERT INTO _ganso_rate_limits (name, window_start, count)
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
			`DELETE FROM _ganso_rate_limits WHERE window_start < unixepoch() - ?`,
			&sqlitex.ExecOptions{
				Args: []any{olderThanSec},
			},
		); err != nil {
			return fmt.Errorf("sweep rate limits: %w", err)
		}
		count = tx.conn.Changes()
		return nil
	})
	return count, err
}
