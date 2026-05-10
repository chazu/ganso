# Honker-Go: Pure-Go Implementation Plan

## Using `zombiezen.com/go/sqlite` for Maximum Performance

---

## Executive Summary

Implement Honker's full feature set as a **pure-Go library** (no CGo, no loadable extension dependency) using `zombiezen.com/go/sqlite`. Unlike the existing `honker-go` binding (which loads `libhonker_ext.dylib` via SQLite's extension API), this reimplements all SQL logic natively in Go, giving us:

- Zero CGo, cross-compilable, data-race-detector compatible
- Prepared statement caching via `conn.Prepare()` (automatic in zombiezen)
- Direct use of `sqlitex.Pool` for reader connections
- `sqlitemigration.Pool` for schema bootstrap
- No runtime dependency on a platform-specific `.dylib`/`.so`

---

## Architecture Overview

```
honker/
  honker.go           # Database, Open(), options
  schema.go           # DDL constants, bootstrap, migrations
  queue.go            # Queue, Job types + all queue operations
  stream.go           # Stream, Event types + pub/sub
  notify.go           # Notification, Listener, ephemeral pub/sub
  lock.go             # Advisory locks, rate limiting
  scheduler.go        # Scheduler, CronSchedule, cron parser
  cron.go             # Cron expression parser + next-fire calculator
  outbox.go           # Outbox transactional side-effect delivery
  watcher.go          # UpdateWatcher (PRAGMA data_version polling)
  worker.go           # Task registry, worker loop, TaskResult
  errors.go           # Sentinel errors (ErrLockHeld, ErrClosed, etc.)
  options.go          # Functional options for Open, Queue, etc.
  honker_test.go      # Core integration tests
  queue_test.go
  stream_test.go
  scheduler_test.go
  cron_test.go
  watcher_test.go
  worker_test.go
```

**Module path:** `github.com/chazu/honker` (or your chosen path)

**Dependencies:**
- `zombiezen.com/go/sqlite` - SQLite bindings + pool + migrations
- Standard library only for everything else (no chrono equivalent needed; Go's `time` package handles timezone-aware cron)

---

## Phase 1: Foundation (Schema + Connection Management)

### 1.1 Schema Constants (`schema.go`)

Port `BOOTSTRAP_HONKER_SQL` verbatim from `honker-core/src/lib.rs`:

```go
const bootstrapSQL = `
CREATE TABLE IF NOT EXISTS _honker_notifications ( ... );
CREATE INDEX IF NOT EXISTS _honker_notifications_recent ...;
CREATE TABLE IF NOT EXISTS _honker_live ( ... );
CREATE INDEX IF NOT EXISTS _honker_live_claim ...;
-- ... all 10 tables + indexes from BOOTSTRAP_HONKER_SQL
`

// Migration for pre-Mantle DBs (enabled column on scheduler_tasks)
const migrateAddEnabledColumn = `
ALTER TABLE _honker_scheduler_tasks ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1
`
```

Use `sqlitemigration.Schema` for versioned migrations:

```go
var schema = sqlitemigration.Schema{
    AppID: 0x484F4E4B, // "HONK" in hex
    Migrations: []string{
        bootstrapSQL,
    },
}
```

### 1.2 Database Handle (`honker.go`)

```go
type Database struct {
    path     string
    pool     *sqlitemigration.Pool  // reader pool (auto-migrating)
    writerMu sync.Mutex            // serialize writes (single-writer WAL)
    writer   *sqlite.Conn          // dedicated writer connection
    watcher  *UpdateWatcher        // PRAGMA data_version poller
    
    queues   sync.Map              // memoized Queue handles
    streams  sync.Map              // memoized Stream handles
    outboxes sync.Map              // memoized Outbox handles
    
    closed   atomic.Bool
}
```

**Key design decisions:**

1. **Single writer connection + mutex** (mirrors Honker's `Writer` slot). WAL allows only one writer; serializing in userspace avoids SQLITE_BUSY retries. Use `sync.Mutex` (Go's equivalent of `parking_lot::Mutex`).

2. **Reader pool via `sqlitemigration.Pool`** - handles schema bootstrap on first connection, then provides a pool of reader connections. Size defaults to `runtime.GOMAXPROCS(0)` (typically = CPU count).

3. **Writer PRAGMAs** applied via `PrepareConn` callback:
   ```go
   func prepareConn(conn *sqlite.Conn) error {
       return sqlitex.ExecuteScript(conn, defaultPragmas, nil)
   }
   
   const defaultPragmas = `
   PRAGMA journal_mode = WAL;
   PRAGMA synchronous = NORMAL;
   PRAGMA busy_timeout = 5000;
   PRAGMA foreign_keys = ON;
   PRAGMA cache_size = -32000;
   PRAGMA temp_store = MEMORY;
   PRAGMA wal_autocheckpoint = 10000;
   `
   ```

### 1.3 Open Function

```go
type OpenOptions struct {
    MaxReaders     int           // default: GOMAXPROCS
    WatcherBackend string        // "polling" only (for now)
    PollInterval   time.Duration // default: 1ms
}

func Open(path string, opts ...OpenOption) (*Database, error)
```

Open sequence:
1. Open dedicated writer `*sqlite.Conn` with PRAGMAs
2. Bootstrap schema on writer (idempotent DDL)
3. Open `sqlitemigration.Pool` for readers with `PrepareConn` applying PRAGMAs
4. Spawn `UpdateWatcher` goroutine
5. Return `*Database`

### 1.4 Transaction API

```go
// WithTx runs fn inside an IMMEDIATE transaction on the writer connection.
// Commits on nil error, rolls back otherwise.
func (db *Database) WithTx(ctx context.Context, fn func(tx *Tx) error) error

type Tx struct {
    conn *sqlite.Conn
    db   *Database
}

func (tx *Tx) Query(sql string, args ...any) ([]map[string]any, error)
func (tx *Tx) Execute(sql string, args ...any) error
```

Use `sqlitex.ImmediateTransaction` under the hood:

```go
func (db *Database) WithTx(ctx context.Context, fn func(tx *Tx) error) error {
    db.writerMu.Lock()
    defer db.writerMu.Unlock()
    
    endFn, err := sqlitex.ImmediateTransaction(db.writer)
    if err != nil {
        return err
    }
    defer endFn(&err)
    
    tx := &Tx{conn: db.writer, db: db}
    err = fn(tx)
    return err
}
```

---

## Phase 2: Update Watcher (`watcher.go`)

The heartbeat of Honker. A goroutine polling `PRAGMA data_version` every 1ms.

```go
type UpdateWatcher struct {
    path     string
    stop     chan struct{}
    subs     sync.Map       // map[uint64]chan struct{}
    subID    atomic.Uint64
    pollInterval time.Duration
}

func newUpdateWatcher(path string, interval time.Duration) (*UpdateWatcher, error)
func (w *UpdateWatcher) Subscribe() (ch <-chan struct{}, unsubscribe func())
func (w *UpdateWatcher) Stop()
```

**Implementation:**

```go
func (w *UpdateWatcher) run() {
    conn, _ := sqlite.OpenConn(w.path, sqlite.OpenReadOnly|sqlite.OpenNoMutex)
    defer conn.Close()
    
    var lastVersion int64
    // Baseline
    sqlitex.Execute(conn, "PRAGMA data_version;", &sqlitex.ExecOptions{
        ResultFunc: func(stmt *sqlite.Stmt) error {
            lastVersion = stmt.ColumnInt64(0)
            return nil
        },
    })
    
    // File identity for dead-man's switch
    initialStat := statIdentity(w.path)
    identityTicker := time.NewTicker(100 * time.Millisecond)
    defer identityTicker.Stop()
    
    ticker := time.NewTicker(w.pollInterval)
    defer ticker.Stop()
    
    for {
        select {
        case <-w.stop:
            return
        case <-ticker.C:
            var version int64
            err := sqlitex.Execute(conn, "PRAGMA data_version;", &sqlitex.ExecOptions{
                ResultFunc: func(stmt *sqlite.Stmt) error {
                    version = stmt.ColumnInt64(0)
                    return nil
                },
            })
            if err != nil {
                // Reconnect path, fire conservative wake
                w.notifyAll()
                continue
            }
            if version != lastVersion {
                lastVersion = version
                w.notifyAll()
            }
        case <-identityTicker.C:
            if id := statIdentity(w.path); id != initialStat {
                panic("honker: database file was replaced underneath us")
            }
        }
    }
}
```

**Fan-out:** Each subscriber gets a buffered `chan struct{}` (cap=1). `notifyAll` does non-blocking send (coalesces bursts, same as Honker's `SyncSender<()>` with cap=1).

```go
func (w *UpdateWatcher) notifyAll() {
    w.subs.Range(func(_, v any) bool {
        ch := v.(chan struct{})
        select {
        case ch <- struct{}{}:
        default: // already has a pending wake, coalesce
        }
        return true
    })
}
```

---

## Phase 3: Ephemeral Pub/Sub (`notify.go`)

### 3.1 Notification Type

```go
type Notification struct {
    ID        int64
    Channel   string
    Payload   json.RawMessage
    CreatedAt int64
}
```

### 3.2 Notify (inside transaction)

```go
func (tx *Tx) Notify(channel, payload string) (int64, error) {
    // INSERT INTO _honker_notifications (channel, payload) VALUES (?, ?)
    // RETURNING id
}
```

### 3.3 Listener

```go
type Listener struct {
    db           *Database
    channel      string
    lastID       int64
    updates      <-chan struct{}
    unsubscribe  func()
    fallbackPoll time.Duration
}

func (db *Database) Listen(channel string, opts ...ListenOption) *Listener
func (l *Listener) Next(ctx context.Context) (Notification, error) // blocks
func (l *Listener) Close()
```

`Next()` logic:
1. Query new notifications: `SELECT * FROM _honker_notifications WHERE channel = ? AND id > ? ORDER BY id`
2. If results, return first, update `lastID`
3. If empty, wait on update watcher channel OR fallback timeout OR ctx.Done()
4. Loop

---

## Phase 4: Queue (`queue.go`)

### 4.1 Types

```go
type Queue struct {
    db                 *Database
    Name               string
    VisibilityTimeout  int // seconds, default 300
    MaxAttempts        int // default 3
}

type Job struct {
    ID              int64
    Queue           string
    Payload         json.RawMessage
    State           string // "pending" | "processing"
    Priority        int
    RunAt           int64
    WorkerID        string
    ClaimExpiresAt  int64
    Attempts        int
    MaxAttempts     int
    CreatedAt       int64
    ExpiresAt       *int64
    
    queue *Queue // back-reference for convenience methods
}

func (j *Job) Ack() error
func (j *Job) Retry(delaySec int, errMsg string) error
func (j *Job) Fail(errMsg string) error
func (j *Job) Heartbeat(extendSec int) error
```

### 4.2 Queue Operations

All SQL ported directly from `honker_ops.rs`. Key operations:

```go
func (q *Queue) Enqueue(payload any, opts ...EnqueueOption) (int64, error)
func (q *Queue) ClaimOne(workerID string) (*Job, error)
func (q *Queue) ClaimBatch(workerID string, n int) ([]*Job, error)
func (q *Queue) Ack(jobID int64, workerID string) (bool, error)
func (q *Queue) AckBatch(jobIDs []int64, workerID string) (int, error)
func (q *Queue) Retry(jobID int64, workerID string, delaySec int, errMsg string) (bool, error)
func (q *Queue) Fail(jobID int64, workerID string, errMsg string) (bool, error)
func (q *Queue) Heartbeat(jobID int64, workerID string, extendSec int) (bool, error)
func (q *Queue) Cancel(jobID int64) (bool, error)
func (q *Queue) GetJob(jobID int64) (*Job, error)
func (q *Queue) SweepExpired() (int, error)
func (q *Queue) SaveResult(jobID int64, value any, ttlSec int) error
func (q *Queue) GetResult(jobID int64) (json.RawMessage, bool, error)
func (q *Queue) WaitResult(ctx context.Context, jobID int64) (json.RawMessage, error)
```

#### Claim SQL (the hot path)

Port the exact `UPDATE ... RETURNING` from `claim_batch()`:

```go
const claimBatchSQL = `
UPDATE _honker_live
SET state = 'processing',
    worker_id = :worker_id,
    claim_expires_at = unixepoch() + :timeout,
    attempts = attempts + 1
WHERE id IN (
  SELECT id FROM _honker_live
  WHERE queue = :queue
    AND state IN ('pending', 'processing')
    AND (expires_at IS NULL OR expires_at > unixepoch())
    AND ((state = 'pending' AND run_at <= unixepoch())
      OR (state = 'processing' AND claim_expires_at < unixepoch()))
  ORDER BY priority DESC, run_at ASC, id ASC
  LIMIT :n
)
RETURNING id, queue, payload, worker_id, attempts, claim_expires_at
`
```

Uses `conn.Prepare(claimBatchSQL)` (cached automatically by zombiezen).

### 4.3 Claim Iterator (Go channels + goroutine)

Go's equivalent of Python's `async for job in queue.claim(worker_id)`:

```go
// Claims returns a channel that yields jobs one at a time.
// Cancel the context to stop claiming.
func (q *Queue) Claims(ctx context.Context, workerID string, opts ...ClaimOption) <-chan *Job

// ClaimIter returns an iterator for use in range-over-func (Go 1.23+).
func (q *Queue) ClaimIter(ctx context.Context, workerID string) iter.Seq[*Job]
```

Internal loop:
1. `ClaimOne()` - if got a job, yield it
2. If empty, wait on:
   - Update watcher channel (new commit)
   - `nextClaimAt` deadline timer (delayed job becoming claimable)
   - Idle poll timeout (fallback)
   - Context cancellation

---

## Phase 5: Stream (`stream.go`)

### 5.1 Types

```go
type Stream struct {
    db   *Database
    Name string
}

type Event struct {
    Offset    int64
    Topic     string
    Key       *string
    Payload   json.RawMessage
    CreatedAt int64
}
```

### 5.2 Operations

```go
func (s *Stream) Publish(payload any, opts ...PublishOption) (int64, error)
func (s *Stream) PublishTx(tx *Tx, payload any, opts ...PublishOption) (int64, error)
func (s *Stream) Subscribe(ctx context.Context, opts ...SubscribeOption) <-chan Event
func (s *Stream) SaveOffset(consumer string, offset int64) error
func (s *Stream) GetOffset(consumer string) (int64, error)
```

SQL from `stream_publish`, `stream_read_since`, `stream_save_offset`, `stream_get_offset` in `honker_ops.rs`.

**Subscribe** internally:
1. Resolve starting offset (`GetOffset` or `fromOffset` option)
2. Read existing events via `SELECT ... FROM _honker_stream WHERE topic = ? AND offset > ? ORDER BY offset LIMIT 100`
3. Yield them
4. Subscribe to update watcher for new events
5. Periodically auto-save offset (every N events or every T seconds)

---

## Phase 6: Locks & Rate Limiting (`lock.go`)

### 6.1 Advisory Locks

```go
type Lock struct {
    db       *Database
    Name     string
    TTL      int
    Owner    string
    acquired bool
}

// Lock acquires and returns a Lock. Use with defer lock.Release().
func (db *Database) Lock(ctx context.Context, name string, opts ...LockOption) (*Lock, error)
func (l *Lock) Release() error

// TryLock is non-blocking. Returns (lock, nil) on success, (nil, ErrLockHeld) if held.
func (db *Database) TryLock(name string, opts ...LockOption) (*Lock, error)

// WithLock is a convenience that acquires, runs fn, and releases.
func (db *Database) WithLock(ctx context.Context, name string, fn func() error, opts ...LockOption) error
```

SQL from `lock_acquire` / `lock_release` in `honker_ops.rs`:

```go
const lockAcquireSQL = `
DELETE FROM _honker_locks WHERE name = :name AND expires_at < unixepoch();
INSERT OR IGNORE INTO _honker_locks (name, owner, expires_at)
VALUES (:name, :owner, unixepoch() + :ttl);
SELECT CASE WHEN owner = :owner THEN 1 ELSE 0 END FROM _honker_locks WHERE name = :name;
`
```

### 6.2 Rate Limiting

```go
func (db *Database) TryRateLimit(name string, limit, perSec int) (bool, error)
func (db *Database) SweepRateLimits(olderThanSec int) (int, error)
```

---

## Phase 7: Cron Parser (`cron.go`)

Port the Rust `cron.rs` to pure Go. Go's `time` package handles timezone-aware calculations natively (no chrono equivalent needed).

```go
type Schedule interface {
    NextAfter(t time.Time) time.Time
}

type CronSchedule struct {
    seconds  []int // sorted set
    minutes  []int
    hours    []int
    days     []int
    months   []int
    dows     []int
}

type IntervalSchedule struct {
    Interval time.Duration
}

func ParseSchedule(expr string) (Schedule, error)  // "* * * * *" or "@every 5s"
func Crontab(expr string) (Schedule, error)         // 5-field or 6-field cron only
func Every(d time.Duration) Schedule                // fixed interval
```

**`NextAfter` algorithm** - direct port of `cron_next_after_naive`:
- Start at `from + 1 second`
- Walk month -> day -> hour -> minute -> second, advancing to next matching field
- Handle day-of-week filtering
- Cap at 100 years to prevent infinite loops
- Use `time.LoadLocation` for timezone-aware evaluation (system local by default)

---

## Phase 8: Scheduler (`scheduler.go`)

```go
type Scheduler struct {
    db       *Database
    lockName string
}

func (db *Database) Scheduler(opts ...SchedulerOption) *Scheduler

func (s *Scheduler) Add(name, queue string, schedule Schedule, opts ...ScheduleTaskOption) error
func (s *Scheduler) Remove(name string) (bool, error)
func (s *Scheduler) Pause(name string) (bool, error)
func (s *Scheduler) Resume(name string) (bool, error)
func (s *Scheduler) List() ([]ScheduleInfo, error)
func (s *Scheduler) Update(name string, opts ...UpdateOption) (bool, error)

// Run blocks, running the scheduler loop until ctx is cancelled.
// Acquires leader lock; returns ErrLockHeld if another scheduler holds it.
func (s *Scheduler) Run(ctx context.Context) error
```

**Run loop** (direct port of Python `_main_loop`):
1. Acquire leader lock via `db.Lock("honker-scheduler", ttl=60)`
2. Start heartbeat goroutine (refreshes lock every 30s)
3. Loop:
   a. `schedulerTick(now)` - for each task where `next_fire_at <= now`, enqueue payload, advance `next_fire_at`
   b. `schedulerSoonest()` - compute sleep duration
   c. `select` on: ctx.Done, update watcher, timer
4. On ctx.Done, release lock

All scheduler SQL (`register`, `unregister`, `tick`, `soonest`, `pause`, `resume`, `list`, `update`) ported from `honker_ops.rs` to prepared Go statements.

---

## Phase 9: Task Registry & Workers (`worker.go`)

Go-idiomatic task registration using function values:

```go
type TaskSpec struct {
    Name        string
    Fn          func(ctx context.Context, payload json.RawMessage) (any, error)
    QueueName   string
    Retries     int
    RetryDelay  time.Duration
    Timeout     time.Duration
    Priority    int
    StoreResult bool
    ResultTTL   time.Duration
}

type TaskRegistry struct {
    mu    sync.RWMutex
    tasks map[string]*TaskSpec
}

func NewRegistry() *TaskRegistry
func (r *TaskRegistry) Register(spec TaskSpec)
func (r *TaskRegistry) Get(name string) (*TaskSpec, bool)
```

### Task Wrapper

```go
// Task registers a function and returns an enqueue helper.
func (q *Queue) Task(name string, fn TaskFunc, opts ...TaskOption) *TaskHandle

type TaskHandle struct {
    Name  string
    queue *Queue
}

// Call enqueues the task. Returns a TaskResult for retrieving the return value.
func (h *TaskHandle) Call(payload any) (*TaskResult, error)

type TaskResult struct {
    JobID int64
    queue *Queue
}

func (r *TaskResult) Get(ctx context.Context) (json.RawMessage, error) // blocks until result ready
```

### Worker Loop

```go
type WorkerOptions struct {
    Queue       string
    Concurrency int  // default: GOMAXPROCS
    Registry    *TaskRegistry
}

// RunWorkers runs worker goroutines that claim and execute tasks.
// Blocks until ctx is cancelled.
func (db *Database) RunWorkers(ctx context.Context, opts WorkerOptions) error
```

Internal:
- Spawns `concurrency` goroutines per queue
- Each goroutine uses `queue.ClaimIter(ctx, workerID)` to get jobs
- Dispatches to registry, handles timeout via `context.WithTimeout`
- On success: save result (if configured), ack
- On error: retry with delay, or fail to dead-letter if max attempts exceeded

---

## Phase 10: Outbox (`outbox.go`)

```go
type Outbox struct {
    db                *Database
    Name              string
    DeliveryFn        func(ctx context.Context, payload json.RawMessage) error
    MaxAttempts       int
    BaseBackoff       time.Duration
    VisibilityTimeout time.Duration
    queue             *Queue // backed by a Queue internally
}

func (db *Database) Outbox(name string, deliveryFn DeliveryFunc, opts ...OutboxOption) *Outbox
func (o *Outbox) Send(tx *Tx, payload any) (int64, error) // enqueue inside caller's tx
func (o *Outbox) Run(ctx context.Context) error            // delivery loop
```

The Outbox is just a Queue + a delivery function. `Send` enqueues within the caller's write transaction (atomic with business writes). `Run` claims jobs and calls the delivery function.

---

## Phase 11: Convenience & Polish

### 11.1 Query Helper

```go
func (db *Database) Query(ctx context.Context, sql string, args ...any) ([]map[string]any, error)
```

Uses a reader from the pool:
```go
func (db *Database) Query(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
    conn, err := db.pool.Take(ctx)
    if err != nil { return nil, err }
    defer db.pool.Put(conn)
    // Execute and collect rows into []map[string]any
}
```

### 11.2 Notification Pruning

```go
func (db *Database) PruneNotifications(opts ...PruneOption) (int, error)
```

### 11.3 Close

```go
func (db *Database) Close() error {
    db.closed.Store(true)
    db.watcher.Stop()
    writerErr := db.writer.Close()
    poolErr := db.pool.Close()
    // return combined error
}
```

---

## Performance Design Decisions

### Why zombiezen/go-sqlite is ideal for this

| Feature | Benefit for Honker |
|---|---|
| **No CGo** | Cross-compile, race detector, simpler builds |
| **`conn.Prepare()` auto-caches** | Claim/ack/enqueue hot-path statements are prepared once, reused forever |
| **`sqlitex.Pool`** | Reader pool with proper connection lifecycle |
| **`sqlitex.ImmediateTransaction`** | Correct write-tx semantics (avoids deferred->immediate upgrade deadlocks) |
| **`sqlitex.Save` (savepoints)** | Nested transactions for outbox pattern |
| **`sqlitemigration.Pool`** | Schema bootstrap on first use, race-safe across processes |
| **`Conn.CreateFunction`** | Could register Go functions as SQL scalars if needed |
| **`Conn.SetBusyTimeout`** | Built-in busy-wait on contention |

### Concurrency Model

```
                    +-----------+
                    | Database  |
                    +-----+-----+
                          |
          +---------------+---------------+
          |               |               |
    +-----+-----+  +-----+-----+  +------+------+
    |  Writer   |  | Reader    |  | Watcher     |
    |  (1 conn) |  | Pool (N)  |  | (1 goroutine|
    |  + Mutex  |  | sqlitex   |  |  1ms poll)  |
    +-----------+  +-----------+  +-------------+
          |                              |
     writes serialized            fan-out to subscribers
     via sync.Mutex               via chan struct{} (cap=1)
```

### Statement Caching Strategy

All hot-path SQL uses `conn.Prepare()` (not `PrepareTransient`). zombiezen's `Conn.Prepare` caches prepared statements by query string, so the second call to `Prepare` with the same SQL returns the cached `*Stmt` immediately. This matches Honker's Rust `prepare_cached()`.

### JSON Handling

Use `encoding/json.RawMessage` for payload storage to avoid unnecessary marshal/unmarshal round-trips. Only serialize when the user provides a Go value, only deserialize when the user reads `.Payload`.

---

## Cross-Process Compatibility

The on-disk schema is **identical** to Honker's. A Go process and a Python/Node/Rust process can share the same `.db` file. The watcher mechanism (`PRAGMA data_version`) is the same. Wake latency characteristics are the same (~1ms polling).

The `notify()` SQL function is NOT registered (it's a Rust extension function). Instead, Go code does `INSERT INTO _honker_notifications` directly, which is functionally identical. The notification table is the shared contract, not the SQL function.

---

## Testing Strategy

### Unit Tests
- Cron parser: port all tests from `cron.rs` (DST edge cases, field validation, intervals)
- Schema bootstrap idempotency
- Each queue operation in isolation

### Integration Tests
- Multi-goroutine claim contention
- Visibility timeout expiry and re-claim
- Stream subscribe-before-publish race
- Scheduler leader election and failover
- Outbox atomic commit with business writes
- Watcher file-replacement detection (dead-man's switch)

### Cross-Process Tests
- Go producer, Go consumer
- Go producer, Python consumer (validates schema compatibility)

### Benchmarks
- Enqueue throughput (single-writer)
- Claim latency (empty queue wake time)
- Stream publish/subscribe throughput
- Watcher overhead (CPU usage of 1ms polling)

---

## Implementation Order & Milestones

| Phase | What | Depends On | Estimated Effort |
|---|---|---|---|
| **1** | Schema + Database + Open/Close + Tx | - | 1 day |
| **2** | UpdateWatcher | Phase 1 | 0.5 day |
| **3** | Notify/Listen (ephemeral pub/sub) | Phase 1, 2 | 0.5 day |
| **4** | Queue (enqueue, claim, ack, retry, fail) | Phase 1, 2 | 1.5 days |
| **5** | Stream (publish, subscribe, offsets) | Phase 1, 2 | 1 day |
| **6** | Locks + Rate Limiting | Phase 1 | 0.5 day |
| **7** | Cron Parser | - | 0.5 day |
| **8** | Scheduler | Phase 4, 6, 7 | 1 day |
| **9** | Task Registry + Workers | Phase 4 | 1 day |
| **10** | Outbox | Phase 4 | 0.5 day |
| **11** | Polish, tests, benchmarks | All | 1.5 days |
| | **Total** | | **~9 days** |

---

## API Quick Reference (What Users See)

```go
package main

import (
    "context"
    "fmt"
    "github.com/chazu/honker"
)

func main() {
    db, _ := honker.Open("app.db")
    defer db.Close()

    // --- Queue ---
    q := db.Queue("emails")
    jobID, _ := q.Enqueue(map[string]any{"to": "alice@example.com"})

    // Atomic enqueue + business write
    db.WithTx(context.Background(), func(tx *honker.Tx) error {
        tx.Execute("INSERT INTO orders (id) VALUES (1)")
        q.EnqueueTx(tx, map[string]any{"order_id": 1})
        return nil
    })

    // Claim loop
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    for job := range q.ClaimIter(ctx, "worker-1") {
        fmt.Println("got job:", job.ID, string(job.Payload))
        job.Ack()
    }

    // --- Stream ---
    s := db.Stream("events")
    s.Publish(map[string]any{"type": "order.created", "id": 1})
    for event := range s.Subscribe(ctx, honker.Consumer("analytics")) {
        fmt.Println("event:", event.Offset, string(event.Payload))
    }

    // --- Scheduler ---
    sched := db.Scheduler()
    sched.Add("nightly-backup", "backups", honker.Crontab("0 3 * * *"))
    sched.Run(ctx) // blocks, runs leader loop

    // --- Lock ---
    lock, _ := db.TryLock("migration")
    defer lock.Release()

    // --- Rate Limit ---
    allowed, _ := db.TryRateLimit("api-calls", 100, 60)
}
```

---

## Appendix A: Complete Public API Surface

Every exported symbol, organized by type. This is the contract users code against.

### Package-Level Functions

```go
func Open(path string, opts ...OpenOption) (*Database, error)
func Crontab(expr string) (Schedule, error)       // 5-field or 6-field cron
func ParseSchedule(expr string) (Schedule, error)  // cron or "@every 5s"
func Every(d time.Duration) Schedule               // fixed interval
func NewRegistry() *TaskRegistry                   // standalone task registry
```

### Types & Interfaces

```go
// Schedule is implemented by CronSchedule and IntervalSchedule.
type Schedule interface {
    NextAfter(t time.Time) time.Time
}

type CronSchedule struct { /* unexported fields */ }
func (c *CronSchedule) NextAfter(t time.Time) time.Time

type IntervalSchedule struct { Interval time.Duration }
func (i IntervalSchedule) NextAfter(t time.Time) time.Time
```

### Database

```go
func (db *Database) Close() error
func (db *Database) WithTx(ctx context.Context, fn func(tx *Tx) error) error
func (db *Database) Query(ctx context.Context, sql string, args ...any) ([]map[string]any, error)

// Subsystem constructors (memoized)
func (db *Database) Queue(name string, opts ...QueueOption) *Queue
func (db *Database) Stream(name string) *Stream
func (db *Database) Outbox(name string, deliveryFn DeliveryFunc, opts ...OutboxOption) *Outbox
func (db *Database) Scheduler(opts ...SchedulerOption) *Scheduler

// Ephemeral pub/sub
func (db *Database) Listen(channel string, opts ...ListenOption) *Listener

// Locking
func (db *Database) Lock(ctx context.Context, name string, opts ...LockOption) (*Lock, error)
func (db *Database) TryLock(name string, opts ...LockOption) (*Lock, error)
func (db *Database) WithLock(ctx context.Context, name string, fn func() error, opts ...LockOption) error

// Rate limiting
func (db *Database) TryRateLimit(name string, limit, perSec int) (bool, error)
func (db *Database) SweepRateLimits(olderThanSec int) (int, error)

// Maintenance
func (db *Database) PruneNotifications(opts ...PruneOption) (int, error)

// Task decorator shortcuts (delegate to db.Queue(queue).Task/PeriodicTask)
func (db *Database) Task(queue, name string, fn TaskFunc, opts ...TaskOption) *TaskHandle
func (db *Database) PeriodicTask(queue, name string, schedule Schedule, fn TaskFunc, opts ...TaskOption) *TaskHandle

// Worker runner
func (db *Database) RunWorkers(ctx context.Context, opts WorkerOptions) error
```

### Tx (Transaction)

```go
func (tx *Tx) Execute(sql string, args ...any) error
func (tx *Tx) Query(sql string, args ...any) ([]map[string]any, error)
func (tx *Tx) Notify(channel, payload string) (int64, error)
```

### Queue

```go
func (q *Queue) Enqueue(payload any, opts ...EnqueueOption) (int64, error)
func (q *Queue) EnqueueTx(tx *Tx, payload any, opts ...EnqueueOption) (int64, error)
func (q *Queue) ClaimOne(workerID string) (*Job, error)
func (q *Queue) ClaimBatch(workerID string, n int) ([]*Job, error)
func (q *Queue) Claims(ctx context.Context, workerID string, opts ...ClaimOption) <-chan *Job
func (q *Queue) ClaimIter(ctx context.Context, workerID string) iter.Seq[*Job]
func (q *Queue) Ack(jobID int64, workerID string) (bool, error)
func (q *Queue) AckBatch(jobIDs []int64, workerID string) (int, error)
func (q *Queue) Retry(jobID int64, workerID string, delaySec int, errMsg string) (bool, error)
func (q *Queue) Fail(jobID int64, workerID string, errMsg string) (bool, error)
func (q *Queue) Heartbeat(jobID int64, workerID string, extendSec int) (bool, error)
func (q *Queue) Cancel(jobID int64) (bool, error)
func (q *Queue) GetJob(jobID int64) (*Job, error)
func (q *Queue) SweepExpired() (int, error)
func (q *Queue) SaveResult(jobID int64, value any, ttlSec int) error
func (q *Queue) GetResult(jobID int64) (json.RawMessage, bool, error)
func (q *Queue) WaitResult(ctx context.Context, jobID int64) (json.RawMessage, error)

// Task decorators
func (q *Queue) Task(name string, fn TaskFunc, opts ...TaskOption) *TaskHandle
func (q *Queue) PeriodicTask(name string, schedule Schedule, fn TaskFunc, opts ...TaskOption) *TaskHandle
```

### Job

```go
type Job struct {
    ID              int64
    Queue           string
    Payload         json.RawMessage
    State           string
    Priority        int
    RunAt           int64
    WorkerID        string
    ClaimExpiresAt  int64
    Attempts        int
    MaxAttempts     int
    CreatedAt       int64
    ExpiresAt       *int64
}

func (j *Job) Ack() error
func (j *Job) Retry(delaySec int, errMsg string) error
func (j *Job) Fail(errMsg string) error
func (j *Job) Heartbeat(extendSec int) error
```

### Stream

```go
func (s *Stream) Publish(payload any, opts ...PublishOption) (int64, error)
func (s *Stream) PublishTx(tx *Tx, payload any, opts ...PublishOption) (int64, error)
func (s *Stream) Subscribe(ctx context.Context, opts ...SubscribeOption) <-chan Event
func (s *Stream) SaveOffset(consumer string, offset int64) error
func (s *Stream) GetOffset(consumer string) (int64, error)
```

### Event

```go
type Event struct {
    Offset    int64
    Topic     string
    Key       *string
    Payload   json.RawMessage
    CreatedAt int64
}
```

### Listener (Ephemeral Pub/Sub)

```go
type Notification struct {
    ID        int64
    Channel   string
    Payload   json.RawMessage
    CreatedAt int64
}

func (l *Listener) Next(ctx context.Context) (Notification, error)
func (l *Listener) Close()
```

### Lock

```go
type Lock struct { /* unexported fields */ }

func (l *Lock) Release() error
```

### Scheduler

```go
func (s *Scheduler) Add(name, queue string, schedule Schedule, opts ...ScheduleTaskOption) error
func (s *Scheduler) Remove(name string) (bool, error)
func (s *Scheduler) Pause(name string) (bool, error)
func (s *Scheduler) Resume(name string) (bool, error)
func (s *Scheduler) List() ([]ScheduleInfo, error)
func (s *Scheduler) Update(name string, opts ...UpdateOption) (bool, error)
func (s *Scheduler) Run(ctx context.Context) error

type ScheduleInfo struct {
    Name       string
    Queue      string
    CronExpr   string
    Payload    json.RawMessage
    Priority   int
    ExpiresS   *int
    NextFireAt int64
    Enabled    bool
}
```

### Outbox

```go
type DeliveryFunc func(ctx context.Context, payload json.RawMessage) error

func (o *Outbox) Send(tx *Tx, payload any) (int64, error)
func (o *Outbox) Run(ctx context.Context) error
```

### Task System

```go
type TaskFunc func(ctx context.Context, payload json.RawMessage) (any, error)

type TaskSpec struct {
    Name        string
    Fn          TaskFunc
    QueueName   string
    Retries     int
    RetryDelay  time.Duration
    Timeout     time.Duration
    Priority    int
    StoreResult bool
    ResultTTL   time.Duration
}

type TaskRegistry struct { /* unexported fields */ }
func (r *TaskRegistry) Register(spec TaskSpec) error
func (r *TaskRegistry) Get(name string) (*TaskSpec, bool)
func (r *TaskRegistry) Names() []string
func (r *TaskRegistry) Queues() []string

type TaskHandle struct { /* unexported fields */ }
func (h *TaskHandle) Call(payload any) (*TaskResult, error)
func (h *TaskHandle) CallLocal(ctx context.Context, payload json.RawMessage) (any, error)

type TaskResult struct { /* unexported fields */ }
func (r *TaskResult) ID() int64
func (r *TaskResult) Get(ctx context.Context) (json.RawMessage, error)

type WorkerOptions struct {
    Queue       string          // empty = all registered queues
    Concurrency int             // default: GOMAXPROCS
    Registry    *TaskRegistry   // default: package-level registry
}
```

### Errors

```go
var (
    ErrLockHeld    = errors.New("honker: lock is already held")
    ErrClosed      = errors.New("honker: database is closed")
    ErrUnknownTask = errors.New("honker: unknown task")
)

// Retryable wraps an error to request retry with a specific delay.
// Workers check errors.As(err, &r) to extract the delay.
type Retryable struct {
    Err    error
    Delay  time.Duration
}
func (r *Retryable) Error() string
func (r *Retryable) Unwrap() error
```

### Functional Options (representative, not exhaustive)

```go
// Open
type OpenOption func(*openConfig)
func WithMaxReaders(n int) OpenOption
func WithPollInterval(d time.Duration) OpenOption

// Queue
type QueueOption func(*queueConfig)
func WithVisibilityTimeout(d time.Duration) QueueOption
func WithMaxAttempts(n int) QueueOption

// Enqueue
type EnqueueOption func(*enqueueConfig)
func RunAt(t time.Time) EnqueueOption
func Delay(d time.Duration) EnqueueOption
func WithPriority(p int) EnqueueOption
func Expires(d time.Duration) EnqueueOption

// Subscribe (stream)
type SubscribeOption func(*subscribeConfig)
func Consumer(name string) SubscribeOption
func FromOffset(offset int64) SubscribeOption
func SaveEveryN(n int) SubscribeOption
func SaveEveryDuration(d time.Duration) SubscribeOption

// Listen
type ListenOption func(*listenConfig)
func FallbackPoll(d time.Duration) ListenOption

// Lock
type LockOption func(*lockConfig)
func WithTTL(d time.Duration) LockOption
func WithOwner(owner string) LockOption

// Task
type TaskOption func(*taskConfig)
func WithRetries(n int) TaskOption
func WithRetryDelay(d time.Duration) TaskOption
func WithTimeout(d time.Duration) TaskOption
func WithStoreResult(b bool) TaskOption
func WithResultTTL(d time.Duration) TaskOption

// Scheduler
type SchedulerOption func(*schedulerConfig)
func WithLockName(name string) SchedulerOption

type ScheduleTaskOption func(*scheduleTaskConfig)
func WithSchedulePriority(p int) ScheduleTaskOption
func WithScheduleExpires(d time.Duration) ScheduleTaskOption

// Outbox
type OutboxOption func(*outboxConfig)
func WithOutboxMaxAttempts(n int) OutboxOption
func WithBaseBackoff(d time.Duration) OutboxOption
func WithOutboxVisibilityTimeout(d time.Duration) OutboxOption
```

---

## Open Questions / Future Work

1. **`iter.Seq` vs channels**: Go 1.23+ range-over-func is cleaner than channels for `ClaimIter`/`Subscribe`. Support both?
2. **Generics for payload**: `Queue[T]` with typed payloads? Or keep `json.RawMessage` + user-side marshal?
3. **Metrics/observability**: Expose queue depth, claim rate, dead-letter count via callbacks or prometheus-style interface?
4. **gRPC/HTTP adapter**: Optional sub-package that exposes Honker operations over network for non-Go consumers?
5. **fsnotify watcher backend**: Go has excellent `fsnotify` support; could be an alternative to polling.
