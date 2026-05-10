package ganso

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// Scheduler dispatches periodic tasks via cron or fixed-interval schedules.
// Only one scheduler process is active at a time thanks to leader-lock
// election via _ganso_locks.
type Scheduler struct {
	db       *Database
	lockName string
}

// ScheduleInfo describes a registered schedule and its current state.
type ScheduleInfo struct {
	Name       string          `json:"name"`
	Queue      string          `json:"queue"`
	CronExpr   string          `json:"cron_expr"`
	Payload    json.RawMessage `json:"payload"`
	Priority   int             `json:"priority"`
	ExpiresS   int             `json:"expires_s"`
	NextFireAt string          `json:"next_fire_at"`
	Enabled    bool            `json:"enabled"`
}

// Scheduler returns a Scheduler handle for this database.
func (db *Database) Scheduler(opts ...SchedulerOption) *Scheduler {
	cfg := defaultSchedulerConfig()
	for _, o := range opts {
		o(&cfg)
	}
	return &Scheduler{
		db:       db,
		lockName: cfg.lockName,
	}
}

// Add registers a periodic task. If a schedule with the same name exists it is
// replaced entirely.
func (s *Scheduler) Add(name, queueName string, schedule Schedule, opts ...ScheduleTaskOption) error {
	cfg := defaultScheduleTaskConfig()
	for _, o := range opts {
		o(&cfg)
	}

	payloadBytes, err := json.Marshal(cfg.payload)
	if err != nil {
		return fmt.Errorf("ganso: scheduler add: marshal payload: %w", err)
	}

	nextFire := schedule.NextAfter(time.Now())
	nextFireStr := nextFire.UTC().Format(time.RFC3339Nano)

	expiresS := 0
	if cfg.expires > 0 {
		expiresS = int(cfg.expires.Seconds())
	}

	return s.db.WithTx(func(tx *Tx) error {
		return sqlitex.Execute(tx.conn,
			`INSERT OR REPLACE INTO _ganso_scheduler_tasks
			 (name, queue, cron_expr, payload, priority, expires_s, next_fire_at, enabled)
			 VALUES (?, ?, ?, ?, ?, ?, ?, 1)`,
			&sqlitex.ExecOptions{
				Args: []any{
					name, queueName, schedule.Expr(), string(payloadBytes),
					cfg.priority, expiresS, nextFireStr,
				},
			},
		)
	})
}

// Remove unregisters a periodic task. Returns true if a row was deleted.
func (s *Scheduler) Remove(name string) (bool, error) {
	var removed bool
	err := s.db.WithTx(func(tx *Tx) error {
		if err := sqlitex.Execute(tx.conn,
			`DELETE FROM _ganso_scheduler_tasks WHERE name = ?`,
			&sqlitex.ExecOptions{Args: []any{name}},
		); err != nil {
			return err
		}
		removed = tx.conn.Changes() > 0
		return nil
	})
	return removed, err
}

// Pause disables a schedule so it stops firing. Returns true if a row was
// paused (false if not found or already paused).
func (s *Scheduler) Pause(name string) (bool, error) {
	var changed bool
	err := s.db.WithTx(func(tx *Tx) error {
		if err := sqlitex.Execute(tx.conn,
			`UPDATE _ganso_scheduler_tasks SET enabled = 0 WHERE name = ? AND enabled = 1`,
			&sqlitex.ExecOptions{Args: []any{name}},
		); err != nil {
			return err
		}
		changed = tx.conn.Changes() > 0
		return nil
	})
	return changed, err
}

// Resume re-enables a paused schedule, recomputing next_fire_at from now.
// Returns true if a row was resumed.
func (s *Scheduler) Resume(name string) (bool, error) {
	var changed bool
	err := s.db.WithTx(func(tx *Tx) error {
		var cronExpr string
		var found bool
		if err := sqlitex.Execute(tx.conn,
			`SELECT cron_expr FROM _ganso_scheduler_tasks WHERE name = ? AND enabled = 0`,
			&sqlitex.ExecOptions{
				Args: []any{name},
				ResultFunc: func(stmt *sqlite.Stmt) error {
					cronExpr = stmt.ColumnText(0)
					found = true
					return nil
				},
			},
		); err != nil {
			return err
		}
		if !found {
			return nil
		}

		sched, err := ParseSchedule(cronExpr)
		if err != nil {
			return fmt.Errorf("parse schedule %q: %w", cronExpr, err)
		}
		nextFire := sched.NextAfter(time.Now()).UTC().Format(time.RFC3339Nano)

		if err := sqlitex.Execute(tx.conn,
			`UPDATE _ganso_scheduler_tasks SET enabled = 1, next_fire_at = ? WHERE name = ?`,
			&sqlitex.ExecOptions{Args: []any{nextFire, name}},
		); err != nil {
			return err
		}
		changed = tx.conn.Changes() > 0
		return nil
	})
	return changed, err
}

// List returns every registered schedule with its current state.
func (s *Scheduler) List() ([]ScheduleInfo, error) {
	ctx := context.Background()
	conn, err := s.db.pool.Take(ctx)
	if err != nil {
		return nil, fmt.Errorf("ganso: scheduler list: %w", err)
	}
	defer s.db.pool.Put(conn)

	var infos []ScheduleInfo
	err = sqlitex.Execute(conn,
		`SELECT name, queue, cron_expr, payload, priority, expires_s, next_fire_at, enabled
		 FROM _ganso_scheduler_tasks
		 ORDER BY name`,
		&sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error {
				infos = append(infos, ScheduleInfo{
					Name:       stmt.ColumnText(0),
					Queue:      stmt.ColumnText(1),
					CronExpr:   stmt.ColumnText(2),
					Payload:    json.RawMessage(stmt.ColumnText(3)),
					Priority:   stmt.ColumnInt(4),
					ExpiresS:   stmt.ColumnInt(5),
					NextFireAt: stmt.ColumnText(6),
					Enabled:    stmt.ColumnInt(7) != 0,
				})
				return nil
			},
		},
	)
	return infos, err
}

// Run blocks, running the scheduler loop until ctx is cancelled.
// It acquires a leader lock; if another scheduler already holds it,
// ErrLockHeld is returned immediately.
func (s *Scheduler) Run(ctx context.Context) error {
	lock, err := s.db.Lock(ctx, s.lockName, WithTTL(60*time.Second))
	if err != nil {
		return err
	}
	defer lock.Release()

	// Heartbeat goroutine refreshes lock TTL every 30s.
	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	go s.heartbeat(hbCtx, lock)

	// Subscribe to watcher for commit wake-ups.
	watchCh, unsub := s.db.watcher.Subscribe()
	defer unsub()

	for {
		now := time.Now()

		if err := s.tick(now); err != nil {
			return fmt.Errorf("ganso: scheduler tick: %w", err)
		}

		soonest, err := s.soonest()
		if err != nil {
			return fmt.Errorf("ganso: scheduler soonest: %w", err)
		}

		sleepDur := 60 * time.Second
		if !soonest.IsZero() {
			sleepDur = time.Until(soonest)
			if sleepDur < 100*time.Millisecond {
				sleepDur = 100 * time.Millisecond
			}
			if sleepDur > 60*time.Second {
				sleepDur = 60 * time.Second
			}
		}

		timer := time.NewTimer(sleepDur)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-watchCh:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// heartbeat refreshes the leader lock's TTL periodically.
func (s *Scheduler) heartbeat(ctx context.Context, lock *Lock) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = s.db.WithTx(func(tx *Tx) error {
				return sqlitex.Execute(tx.conn,
					`UPDATE _ganso_locks SET expires_at = unixepoch() + ? WHERE name = ?`,
					&sqlitex.ExecOptions{Args: []any{lock.ttl, lock.name}},
				)
			})
		}
	}
}

// tick fires all due tasks.
func (s *Scheduler) tick(now time.Time) error {
	nowStr := now.UTC().Format(time.RFC3339Nano)

	return s.db.WithTx(func(tx *Tx) error {
		type dueTask struct {
			name       string
			queue      string
			cronExpr   string
			payload    string
			priority   int
			expiresS   int
			nextFireAt string
		}
		var tasks []dueTask

		if err := sqlitex.Execute(tx.conn,
			`SELECT name, queue, cron_expr, payload, priority, expires_s, next_fire_at
			 FROM _ganso_scheduler_tasks
			 WHERE enabled = 1 AND next_fire_at <= ?`,
			&sqlitex.ExecOptions{
				Args: []any{nowStr},
				ResultFunc: func(stmt *sqlite.Stmt) error {
					tasks = append(tasks, dueTask{
						name:       stmt.ColumnText(0),
						queue:      stmt.ColumnText(1),
						cronExpr:   stmt.ColumnText(2),
						payload:    stmt.ColumnText(3),
						priority:   stmt.ColumnInt(4),
						expiresS:   stmt.ColumnInt(5),
						nextFireAt: stmt.ColumnText(6),
					})
					return nil
				},
			},
		); err != nil {
			return err
		}

		for _, t := range tasks {
			var expiresAt interface{}
			if t.expiresS > 0 {
				ea := now.Add(time.Duration(t.expiresS) * time.Second).UTC().Format(time.RFC3339Nano)
				expiresAt = ea
			}

			jobID := uuid.New().String()
			runAt := now.UTC().Format(time.RFC3339Nano)

			if err := sqlitex.Execute(tx.conn,
				`INSERT INTO _ganso_live (id, queue, payload, run_at, priority, max_attempts, expires_at)
				 VALUES (?, ?, ?, ?, ?, 3, ?)`,
				&sqlitex.ExecOptions{
					Args: []any{jobID, t.queue, t.payload, runAt, t.priority, expiresAt},
				},
			); err != nil {
				return fmt.Errorf("enqueue for %q: %w", t.name, err)
			}

			if err := sqlitex.Execute(tx.conn,
				`INSERT INTO _ganso_notifications (channel, payload) VALUES (?, 'new')`,
				&sqlitex.ExecOptions{Args: []any{"ganso:" + t.queue}},
			); err != nil {
				return fmt.Errorf("notify for %q: %w", t.name, err)
			}

			// Advance next_fire_at, catching up past missed boundaries.
			sched, err := ParseSchedule(t.cronExpr)
			if err != nil {
				return fmt.Errorf("parse schedule %q for %q: %w", t.cronExpr, t.name, err)
			}

			fireTime, parseErr := time.Parse(time.RFC3339Nano, t.nextFireAt)
			if parseErr != nil {
				fireTime = now
			}
			newNext := sched.NextAfter(fireTime)
			for newNext.Before(now) {
				newNext = sched.NextAfter(newNext)
			}

			if err := sqlitex.Execute(tx.conn,
				`UPDATE _ganso_scheduler_tasks SET next_fire_at = ? WHERE name = ?`,
				&sqlitex.ExecOptions{Args: []any{newNext.UTC().Format(time.RFC3339Nano), t.name}},
			); err != nil {
				return fmt.Errorf("advance next_fire_at for %q: %w", t.name, err)
			}
		}
		return nil
	})
}

// soonest returns the earliest next_fire_at across all enabled tasks.
func (s *Scheduler) soonest() (time.Time, error) {
	ctx := context.Background()
	conn, err := s.db.pool.Take(ctx)
	if err != nil {
		return time.Time{}, err
	}
	defer s.db.pool.Put(conn)

	var result time.Time
	err = sqlitex.Execute(conn,
		`SELECT MIN(next_fire_at) FROM _ganso_scheduler_tasks WHERE enabled = 1`,
		&sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error {
				raw := stmt.ColumnText(0)
				if raw != "" {
					t, err := time.Parse(time.RFC3339Nano, raw)
					if err == nil {
						result = t
					}
				}
				return nil
			},
		},
	)
	return result, err
}
