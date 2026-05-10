package ganso

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"time"

	"github.com/google/uuid"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// Queue is a durable work queue backed by SQLite.
type Queue struct {
	db                *Database
	Name              string
	visibilityTimeout time.Duration
	maxAttempts       int
}

// Job represents a claimed or pending job in the queue.
type Job struct {
	ID             string
	QueueName      string
	Payload        json.RawMessage
	State          string
	Priority       int
	RunAt          string
	WorkerID       string
	ClaimExpiresAt string
	Attempts       int
	MaxAttempts    int
	CreatedAt      string
	ExpiresAt      *string

	queue *Queue
}

// Ack acknowledges a job, removing it from the queue.
func (j *Job) Ack() error {
	_, err := j.queue.Ack(j.ID, j.WorkerID)
	return err
}

// Retry retries a job with the given delay and error message.
func (j *Job) Retry(delaySec int, errMsg string) error {
	_, err := j.queue.Retry(j.ID, j.WorkerID, delaySec, errMsg)
	return err
}

// Fail moves a job to the dead-letter queue.
func (j *Job) Fail(errMsg string) error {
	_, err := j.queue.Fail(j.ID, j.WorkerID, errMsg)
	return err
}

// Heartbeat extends the claim on a job.
func (j *Job) Heartbeat(extendSec int) error {
	_, err := j.queue.Heartbeat(j.ID, j.WorkerID, extendSec)
	return err
}

// now returns the current time as an ISO-8601 string matching SQLite's format.
func now() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
}

// nowPlus returns the current time plus d as an ISO-8601 string.
func nowPlus(d time.Duration) string {
	return time.Now().UTC().Add(d).Format("2006-01-02T15:04:05.000Z")
}

type deadLetterRow struct {
	id          string
	queue       string
	payload     string
	priority    int
	runAt       string
	attempts    int
	maxAttempts int
	createdAt   string
}

func insertDead(conn *sqlite.Conn, r deadLetterRow, errMsg string) error {
	return sqlitex.Execute(conn,
		`INSERT INTO _ganso_dead (id, queue, payload, priority, run_at, attempts, max_attempts, last_error, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		&sqlitex.ExecOptions{
			Args: []any{r.id, r.queue, r.payload, r.priority, r.runAt, r.attempts, r.maxAttempts, errMsg, r.createdAt},
		},
	)
}

// ---------------------------------------------------------------------------
// enqueueOnConn - shared helper for Enqueue and EnqueueTx
// ---------------------------------------------------------------------------

func (q *Queue) enqueueOnConn(conn *sqlite.Conn, payloadBytes []byte, cfg enqueueConfig) (string, error) {
	id := uuid.New().String()

	// Compute effective run_at.
	var runAt string
	switch {
	case cfg.delay > 0:
		runAt = nowPlus(cfg.delay)
	case cfg.runAt != nil:
		runAt = cfg.runAt.UTC().Format("2006-01-02T15:04:05.000Z")
	default:
		runAt = now()
	}

	// Compute effective expires_at.
	var expiresAt *string
	if cfg.expires > 0 {
		s := nowPlus(cfg.expires)
		expiresAt = &s
	}

	// Insert into _ganso_live.
	err := sqlitex.Execute(conn,
		`INSERT INTO _ganso_live (id, queue, payload, run_at, priority, max_attempts, expires_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		&sqlitex.ExecOptions{
			Args: []any{id, q.Name, string(payloadBytes), runAt, cfg.priority, q.maxAttempts, expiresAt},
		},
	)
	if err != nil {
		return "", fmt.Errorf("ganso: enqueue insert: %w", err)
	}

	// Insert notification to wake consumers.
	err = sqlitex.Execute(conn,
		`INSERT INTO _ganso_notifications (channel, payload) VALUES (?, 'new')`,
		&sqlitex.ExecOptions{
			Args: []any{"ganso:" + q.Name},
		},
	)
	if err != nil {
		return "", fmt.Errorf("ganso: enqueue notify: %w", err)
	}

	return id, nil
}

// ---------------------------------------------------------------------------
// Enqueue
// ---------------------------------------------------------------------------

// Enqueue adds a job to the queue and returns its ID.
func (q *Queue) Enqueue(payload any, opts ...EnqueueOption) (string, error) {
	if q.db.closed.Load() {
		return "", ErrClosed
	}

	var cfg enqueueConfig
	for _, o := range opts {
		o(&cfg)
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("ganso: marshal payload: %w", err)
	}

	q.db.writerMu.Lock()
	defer q.db.writerMu.Unlock()

	endFn, err := sqlitex.ImmediateTransaction(q.db.writer)
	if err != nil {
		return "", fmt.Errorf("ganso: begin tx: %w", err)
	}
	defer endFn(&err)

	id, err := q.enqueueOnConn(q.db.writer, payloadBytes, cfg)
	return id, err
}

// ---------------------------------------------------------------------------
// EnqueueTx
// ---------------------------------------------------------------------------

// EnqueueTx adds a job to the queue within an existing transaction.
func (q *Queue) EnqueueTx(tx *Tx, payload any, opts ...EnqueueOption) (string, error) {
	var cfg enqueueConfig
	for _, o := range opts {
		o(&cfg)
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("ganso: marshal payload: %w", err)
	}

	return q.enqueueOnConn(tx.conn, payloadBytes, cfg)
}

// ---------------------------------------------------------------------------
// ClaimBatch
// ---------------------------------------------------------------------------

// ClaimBatch atomically claims up to n jobs for the given worker.
// Jobs are returned in priority-descending, run_at-ascending order.
func (q *Queue) ClaimBatch(workerID string, n int) ([]*Job, error) {
	if q.db.closed.Load() {
		return nil, ErrClosed
	}

	nowStr := now()
	claimExpires := nowPlus(q.visibilityTimeout)

	q.db.writerMu.Lock()
	defer q.db.writerMu.Unlock()

	// Step 1: SELECT candidate IDs in priority order.
	var orderedIDs []string
	err := sqlitex.Execute(q.db.writer,
		`SELECT id FROM _ganso_live
		 WHERE queue = ?
		   AND state IN ('pending', 'processing')
		   AND (expires_at IS NULL OR expires_at > ?)
		   AND ((state = 'pending' AND run_at <= ?)
		     OR (state = 'processing' AND claim_expires_at < ?))
		 ORDER BY priority DESC, run_at ASC, id ASC
		 LIMIT ?`,
		&sqlitex.ExecOptions{
			Args: []any{q.Name, nowStr, nowStr, nowStr, n},
			ResultFunc: func(stmt *sqlite.Stmt) error {
				orderedIDs = append(orderedIDs, stmt.ColumnText(0))
				return nil
			},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("ganso: claim batch select: %w", err)
	}
	if len(orderedIDs) == 0 {
		return nil, nil
	}

	// Step 2: UPDATE the selected rows and collect results.
	idsJSON, _ := json.Marshal(orderedIDs)
	jobMap := make(map[string]*Job, len(orderedIDs))

	err = sqlitex.Execute(q.db.writer,
		`UPDATE _ganso_live
		 SET state = 'processing',
		     worker_id = ?,
		     claim_expires_at = ?,
		     attempts = attempts + 1
		 WHERE id IN (SELECT value FROM json_each(?))
		 RETURNING id, queue, payload, worker_id, attempts, claim_expires_at`,
		&sqlitex.ExecOptions{
			Args: []any{workerID, claimExpires, string(idsJSON)},
			ResultFunc: func(stmt *sqlite.Stmt) error {
				job := &Job{
					ID:             stmt.ColumnText(0),
					QueueName:      stmt.ColumnText(1),
					Payload:        json.RawMessage(stmt.ColumnText(2)),
					State:          "processing",
					WorkerID:       stmt.ColumnText(3),
					Attempts:       stmt.ColumnInt(4),
					ClaimExpiresAt: stmt.ColumnText(5),
					queue:          q,
				}
				jobMap[job.ID] = job
				return nil
			},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("ganso: claim batch update: %w", err)
	}

	// Step 3: Return jobs in the original priority order.
	jobs := make([]*Job, 0, len(orderedIDs))
	for _, id := range orderedIDs {
		if job, ok := jobMap[id]; ok {
			jobs = append(jobs, job)
		}
	}

	return jobs, nil
}

// ---------------------------------------------------------------------------
// ClaimOne
// ---------------------------------------------------------------------------

// ClaimOne claims a single job, returning nil if none are available.
func (q *Queue) ClaimOne(workerID string) (*Job, error) {
	jobs, err := q.ClaimBatch(workerID, 1)
	if err != nil {
		return nil, err
	}
	if len(jobs) == 0 {
		return nil, nil
	}
	return jobs[0], nil
}

// ---------------------------------------------------------------------------
// Claims
// ---------------------------------------------------------------------------

// Claims returns a channel that yields jobs as they become available.
// The channel is closed when the context is cancelled.
func (q *Queue) Claims(ctx context.Context, workerID string, opts ...ClaimOption) <-chan *Job {
	cfg := defaultClaimConfig()
	for _, o := range opts {
		o(&cfg)
	}

	// Subscribe to watcher BEFORE first claim (subscribe-before-snapshot).
	wakeCh, unsub := q.db.watcher.Subscribe()

	ch := make(chan *Job, 1)

	go func() {
		defer close(ch)
		defer unsub()

		for {
			// Try to claim a job.
			job, err := q.ClaimOne(workerID)
			if err != nil {
				// On error (e.g. db closed), stop.
				return
			}

			if job != nil {
				select {
				case ch <- job:
				case <-ctx.Done():
					return
				}
				continue // Immediately try to claim another.
			}

			// No job available. Compute how long to wait.
			deadline := q.nextClaimAt()
			var timer *time.Timer
			if deadline != "" {
				t, err := time.Parse("2006-01-02T15:04:05.000Z", deadline)
				if err == nil {
					d := time.Until(t)
					if d <= 0 {
						// Deadline already passed, loop immediately.
						continue
					}
					if d > cfg.idlePoll {
						d = cfg.idlePoll
					}
					timer = time.NewTimer(d)
				} else {
					timer = time.NewTimer(cfg.idlePoll)
				}
			} else {
				timer = time.NewTimer(cfg.idlePoll)
			}

			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-wakeCh:
				timer.Stop()
				// Watcher detected a commit; loop back.
			case <-timer.C:
				// Idle poll timer fired; loop back.
			}
		}
	}()

	return ch
}

// ClaimIter returns a range-over-func iterator that yields jobs as they
// become available. Cancel the context to stop iteration.
func (q *Queue) ClaimIter(ctx context.Context, workerID string, opts ...ClaimOption) iter.Seq[*Job] {
	return func(yield func(*Job) bool) {
		ch := q.Claims(ctx, workerID, opts...)
		for job := range ch {
			if !yield(job) {
				return
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Ack
// ---------------------------------------------------------------------------

// Ack acknowledges a job, removing it from the queue. Returns true if the
// job was successfully acked (i.e., it existed, belonged to the worker, and
// the claim had not expired).
func (q *Queue) Ack(jobID string, workerID string) (bool, error) {
	if q.db.closed.Load() {
		return false, ErrClosed
	}

	q.db.writerMu.Lock()
	defer q.db.writerMu.Unlock()

	err := sqlitex.Execute(q.db.writer,
		`DELETE FROM _ganso_live WHERE id = ? AND worker_id = ? AND claim_expires_at >= ?`,
		&sqlitex.ExecOptions{
			Args: []any{jobID, workerID, now()},
		},
	)
	if err != nil {
		return false, fmt.Errorf("ganso: ack: %w", err)
	}

	return q.db.writer.Changes() > 0, nil
}

// ---------------------------------------------------------------------------
// AckBatch
// ---------------------------------------------------------------------------

// AckBatch acknowledges multiple jobs at once. Returns the number of jobs
// successfully acked.
func (q *Queue) AckBatch(jobIDs []string, workerID string) (int, error) {
	if q.db.closed.Load() {
		return 0, ErrClosed
	}

	idsJSON, err := json.Marshal(jobIDs)
	if err != nil {
		return 0, fmt.Errorf("ganso: marshal job ids: %w", err)
	}

	q.db.writerMu.Lock()
	defer q.db.writerMu.Unlock()

	err = sqlitex.Execute(q.db.writer,
		`DELETE FROM _ganso_live
		 WHERE id IN (SELECT value FROM json_each(?))
		   AND worker_id = ?
		   AND claim_expires_at >= ?`,
		&sqlitex.ExecOptions{
			Args: []any{string(idsJSON), workerID, now()},
		},
	)
	if err != nil {
		return 0, fmt.Errorf("ganso: ack batch: %w", err)
	}

	return q.db.writer.Changes(), nil
}

// ---------------------------------------------------------------------------
// Retry
// ---------------------------------------------------------------------------

// Retry returns a job to the queue for re-processing after a delay. If the
// job has exhausted its max attempts, it is moved to the dead-letter queue.
// Returns true if the job was found and acted upon.
func (q *Queue) Retry(jobID string, workerID string, delaySec int, errMsg string) (bool, error) {
	if q.db.closed.Load() {
		return false, ErrClosed
	}

	q.db.writerMu.Lock()
	defer q.db.writerMu.Unlock()

	endFn, err := sqlitex.ImmediateTransaction(q.db.writer)
	if err != nil {
		return false, fmt.Errorf("ganso: begin tx: %w", err)
	}
	defer endFn(&err)

	// Fetch the job to check attempts vs max_attempts.
	var row *deadLetterRow

	err = sqlitex.Execute(q.db.writer,
		`SELECT id, queue, payload, priority, run_at, max_attempts, attempts, created_at
		 FROM _ganso_live
		 WHERE id = ? AND worker_id = ? AND claim_expires_at >= ? AND state = 'processing'`,
		&sqlitex.ExecOptions{
			Args: []any{jobID, workerID, now()},
			ResultFunc: func(stmt *sqlite.Stmt) error {
				row = &deadLetterRow{
					id:          stmt.ColumnText(0),
					queue:       stmt.ColumnText(1),
					payload:     stmt.ColumnText(2),
					priority:    stmt.ColumnInt(3),
					runAt:       stmt.ColumnText(4),
					maxAttempts: stmt.ColumnInt(5),
					attempts:    stmt.ColumnInt(6),
					createdAt:   stmt.ColumnText(7),
				}
				return nil
			},
		},
	)
	if err != nil {
		return false, fmt.Errorf("ganso: retry select: %w", err)
	}
	if row == nil {
		return false, nil
	}

	if row.attempts >= row.maxAttempts {
		err = sqlitex.Execute(q.db.writer,
			`DELETE FROM _ganso_live WHERE id = ?`,
			&sqlitex.ExecOptions{Args: []any{row.id}},
		)
		if err != nil {
			return false, fmt.Errorf("ganso: retry delete: %w", err)
		}
		if err = insertDead(q.db.writer, *row, errMsg); err != nil {
			return false, fmt.Errorf("ganso: retry dead insert: %w", err)
		}
		return true, nil
	}

	// Still have attempts left: return to pending.
	newRunAt := nowPlus(time.Duration(delaySec) * time.Second)
	err = sqlitex.Execute(q.db.writer,
		`UPDATE _ganso_live
		 SET state = 'pending', run_at = ?, worker_id = NULL, claim_expires_at = NULL
		 WHERE id = ?`,
		&sqlitex.ExecOptions{
			Args: []any{newRunAt, row.id},
		},
	)
	if err != nil {
		return false, fmt.Errorf("ganso: retry update: %w", err)
	}

	// Notify so consumers wake up.
	err = sqlitex.Execute(q.db.writer,
		`INSERT INTO _ganso_notifications (channel, payload) VALUES (?, 'retry')`,
		&sqlitex.ExecOptions{
			Args: []any{"ganso:" + q.Name},
		},
	)
	if err != nil {
		return false, fmt.Errorf("ganso: retry notify: %w", err)
	}

	return true, nil
}

// ---------------------------------------------------------------------------
// Fail
// ---------------------------------------------------------------------------

// Fail moves a job to the dead-letter queue. Returns true if the job was
// found and moved.
func (q *Queue) Fail(jobID string, workerID string, errMsg string) (bool, error) {
	if q.db.closed.Load() {
		return false, ErrClosed
	}

	q.db.writerMu.Lock()
	defer q.db.writerMu.Unlock()

	endFn, err := sqlitex.ImmediateTransaction(q.db.writer)
	if err != nil {
		return false, fmt.Errorf("ganso: begin tx: %w", err)
	}
	defer endFn(&err)

	var row *deadLetterRow

	err = sqlitex.Execute(q.db.writer,
		`DELETE FROM _ganso_live
		 WHERE id = ? AND worker_id = ? AND claim_expires_at >= ?
		 RETURNING id, queue, payload, priority, run_at, attempts, max_attempts, created_at`,
		&sqlitex.ExecOptions{
			Args: []any{jobID, workerID, now()},
			ResultFunc: func(stmt *sqlite.Stmt) error {
				row = &deadLetterRow{
					id:          stmt.ColumnText(0),
					queue:       stmt.ColumnText(1),
					payload:     stmt.ColumnText(2),
					priority:    stmt.ColumnInt(3),
					runAt:       stmt.ColumnText(4),
					attempts:    stmt.ColumnInt(5),
					maxAttempts: stmt.ColumnInt(6),
					createdAt:   stmt.ColumnText(7),
				}
				return nil
			},
		},
	)
	if err != nil {
		return false, fmt.Errorf("ganso: fail delete: %w", err)
	}
	if row == nil {
		return false, nil
	}

	if err = insertDead(q.db.writer, *row, errMsg); err != nil {
		return false, fmt.Errorf("ganso: fail dead insert: %w", err)
	}

	return true, nil
}

// ---------------------------------------------------------------------------
// Heartbeat
// ---------------------------------------------------------------------------

// Heartbeat extends the claim on a job by extendSec seconds. Returns true if
// the job was found and the claim was extended.
func (q *Queue) Heartbeat(jobID string, workerID string, extendSec int) (bool, error) {
	if q.db.closed.Load() {
		return false, ErrClosed
	}

	newExpiry := nowPlus(time.Duration(extendSec) * time.Second)

	q.db.writerMu.Lock()
	defer q.db.writerMu.Unlock()

	err := sqlitex.Execute(q.db.writer,
		`UPDATE _ganso_live SET claim_expires_at = ?
		 WHERE id = ? AND worker_id = ? AND claim_expires_at >= ?`,
		&sqlitex.ExecOptions{
			Args: []any{newExpiry, jobID, workerID, now()},
		},
	)
	if err != nil {
		return false, fmt.Errorf("ganso: heartbeat: %w", err)
	}

	return q.db.writer.Changes() > 0, nil
}

// ---------------------------------------------------------------------------
// Cancel
// ---------------------------------------------------------------------------

// Cancel removes a job from the queue regardless of state (pending or
// processing). Returns true if a job was deleted.
func (q *Queue) Cancel(jobID string) (bool, error) {
	if q.db.closed.Load() {
		return false, ErrClosed
	}

	q.db.writerMu.Lock()
	defer q.db.writerMu.Unlock()

	err := sqlitex.Execute(q.db.writer,
		`DELETE FROM _ganso_live WHERE id = ? AND state IN ('pending', 'processing')`,
		&sqlitex.ExecOptions{
			Args: []any{jobID},
		},
	)
	if err != nil {
		return false, fmt.Errorf("ganso: cancel: %w", err)
	}

	return q.db.writer.Changes() > 0, nil
}

// ---------------------------------------------------------------------------
// GetJob
// ---------------------------------------------------------------------------

// GetJob fetches a job by ID from the live queue using the reader pool.
// Returns nil if not found.
func (q *Queue) GetJob(ctx context.Context, jobID string) (*Job, error) {
	if q.db.closed.Load() {
		return nil, ErrClosed
	}

	conn, err := q.db.pool.Take(ctx)
	if err != nil {
		return nil, fmt.Errorf("ganso: take reader: %w", err)
	}
	defer q.db.pool.Put(conn)

	var job *Job

	err = sqlitex.Execute(conn,
		`SELECT id, queue, payload, state, priority, run_at, worker_id,
		        claim_expires_at, attempts, max_attempts, created_at, expires_at
		 FROM _ganso_live WHERE id = ?`,
		&sqlitex.ExecOptions{
			Args: []any{jobID},
			ResultFunc: func(stmt *sqlite.Stmt) error {
				job = &Job{
					ID:             stmt.ColumnText(0),
					QueueName:      stmt.ColumnText(1),
					Payload:        json.RawMessage(stmt.ColumnText(2)),
					State:          stmt.ColumnText(3),
					Priority:       stmt.ColumnInt(4),
					RunAt:          stmt.ColumnText(5),
					WorkerID:       stmt.ColumnText(6),
					ClaimExpiresAt: stmt.ColumnText(7),
					Attempts:       stmt.ColumnInt(8),
					MaxAttempts:    stmt.ColumnInt(9),
					CreatedAt:      stmt.ColumnText(10),
					queue:          q,
				}
				if stmt.ColumnType(11) != sqlite.TypeNull {
					s := stmt.ColumnText(11)
					job.ExpiresAt = &s
				}
				return nil
			},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("ganso: get job: %w", err)
	}

	return job, nil
}

// ---------------------------------------------------------------------------
// SweepExpired
// ---------------------------------------------------------------------------

// SweepExpired removes expired jobs from the live queue and moves them to the
// dead-letter queue. Returns the number of jobs swept.
func (q *Queue) SweepExpired() (int, error) {
	if q.db.closed.Load() {
		return 0, ErrClosed
	}

	q.db.writerMu.Lock()
	defer q.db.writerMu.Unlock()

	endFn, err := sqlitex.ImmediateTransaction(q.db.writer)
	if err != nil {
		return 0, fmt.Errorf("ganso: begin tx: %w", err)
	}
	defer endFn(&err)

	var expired []deadLetterRow

	err = sqlitex.Execute(q.db.writer,
		`DELETE FROM _ganso_live
		 WHERE queue = ? AND expires_at IS NOT NULL AND expires_at < ?
		 RETURNING id, queue, payload, priority, run_at, max_attempts, attempts, created_at`,
		&sqlitex.ExecOptions{
			Args: []any{q.Name, now()},
			ResultFunc: func(stmt *sqlite.Stmt) error {
				expired = append(expired, deadLetterRow{
					id:          stmt.ColumnText(0),
					queue:       stmt.ColumnText(1),
					payload:     stmt.ColumnText(2),
					priority:    stmt.ColumnInt(3),
					runAt:       stmt.ColumnText(4),
					maxAttempts: stmt.ColumnInt(5),
					attempts:    stmt.ColumnInt(6),
					createdAt:   stmt.ColumnText(7),
				})
				return nil
			},
		},
	)
	if err != nil {
		return 0, fmt.Errorf("ganso: sweep delete: %w", err)
	}

	for _, r := range expired {
		if err = insertDead(q.db.writer, r, "expired"); err != nil {
			return 0, fmt.Errorf("ganso: sweep dead insert: %w", err)
		}
	}

	return len(expired), nil
}

// ---------------------------------------------------------------------------
// SaveResult
// ---------------------------------------------------------------------------

// SaveResult stores a result value for a job with the given TTL in seconds.
func (q *Queue) SaveResult(jobID string, value any, ttlSec int) error {
	if q.db.closed.Load() {
		return ErrClosed
	}

	valBytes, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("ganso: marshal result: %w", err)
	}

	expiresAt := nowPlus(time.Duration(ttlSec) * time.Second)

	q.db.writerMu.Lock()
	defer q.db.writerMu.Unlock()

	err = sqlitex.Execute(q.db.writer,
		`INSERT INTO _ganso_results (job_id, value, expires_at)
		 VALUES (?, ?, ?)
		 ON CONFLICT(job_id) DO UPDATE SET value=excluded.value, created_at=strftime('%Y-%m-%dT%H:%M:%fZ','now'), expires_at=excluded.expires_at`,
		&sqlitex.ExecOptions{
			Args: []any{jobID, string(valBytes), expiresAt},
		},
	)
	if err != nil {
		return fmt.Errorf("ganso: save result: %w", err)
	}

	return nil
}

// ---------------------------------------------------------------------------
// GetResult
// ---------------------------------------------------------------------------

// GetResult retrieves a stored result for a job. Returns (value, true, nil)
// if found and not expired, or (nil, false, nil) if not found.
func (q *Queue) GetResult(ctx context.Context, jobID string) (json.RawMessage, bool, error) {
	if q.db.closed.Load() {
		return nil, false, ErrClosed
	}

	conn, err := q.db.pool.Take(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("ganso: take reader: %w", err)
	}
	defer q.db.pool.Put(conn)

	var result json.RawMessage
	var found bool

	err = sqlitex.Execute(conn,
		`SELECT value FROM _ganso_results WHERE job_id = ? AND (expires_at IS NULL OR expires_at > ?)`,
		&sqlitex.ExecOptions{
			Args: []any{jobID, now()},
			ResultFunc: func(stmt *sqlite.Stmt) error {
				result = json.RawMessage(stmt.ColumnText(0))
				found = true
				return nil
			},
		},
	)
	if err != nil {
		return nil, false, fmt.Errorf("ganso: get result: %w", err)
	}

	return result, found, nil
}

// ---------------------------------------------------------------------------
// WaitResult
// ---------------------------------------------------------------------------

// WaitResult blocks until a result is available for the given job or the
// context is cancelled.
func (q *Queue) WaitResult(ctx context.Context, jobID string) (json.RawMessage, error) {
	if q.db.closed.Load() {
		return nil, ErrClosed
	}

	wakeCh, unsub := q.db.watcher.Subscribe()
	defer unsub()

	for {
		result, found, err := q.GetResult(ctx, jobID)
		if err != nil {
			return nil, err
		}
		if found {
			return result, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-wakeCh:
			// Watcher detected a commit; check again.
		}
	}
}

// ---------------------------------------------------------------------------
// nextClaimAt (unexported)
// ---------------------------------------------------------------------------

// nextClaimAt returns the ISO-8601 timestamp of the earliest point at which
// a job in this queue could become claimable, or "" if none exist.
func (q *Queue) nextClaimAt() string {
	ctx := context.Background()
	conn, err := q.db.pool.Take(ctx)
	if err != nil {
		return ""
	}
	defer q.db.pool.Put(conn)

	nowStr := now()
	var deadline string

	err = sqlitex.Execute(conn,
		`SELECT MIN(deadline) FROM (
		   SELECT MIN(run_at) AS deadline FROM _ganso_live
		   WHERE queue = ? AND state = 'pending'
		     AND (expires_at IS NULL OR expires_at > ?)
		     AND run_at > ?
		   UNION ALL
		   SELECT MIN(claim_expires_at) AS deadline FROM _ganso_live
		   WHERE queue = ? AND state = 'processing'
		     AND (expires_at IS NULL OR expires_at > ?)
		     AND claim_expires_at >= ?
		 )`,
		&sqlitex.ExecOptions{
			Args: []any{q.Name, nowStr, nowStr, q.Name, nowStr, nowStr},
			ResultFunc: func(stmt *sqlite.Stmt) error {
				if stmt.ColumnType(0) != sqlite.TypeNull {
					deadline = stmt.ColumnText(0)
				}
				return nil
			},
		},
	)
	if err != nil {
		return ""
	}

	return deadline
}
