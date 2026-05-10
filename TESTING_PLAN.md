# Ganso-Go: Examples, Acceptance Tests & Smoke Tests Plan

## Goal

Validate that every feature works end-to-end as a user would actually use it. Current unit tests verify individual operations; this plan covers integration scenarios, realistic examples, and smoke tests that exercise the full stack.

---

## 1. Examples (`examples/`)

Standalone `main.go` programs that double as documentation and smoke tests. Each must compile and run to completion without error.

### 1.1 `examples/queue/main.go` — Job Queue

- Open database
- Create queue with custom visibility timeout
- Enqueue 10 jobs with mixed priorities and delays
- Spawn 3 worker goroutines via `ClaimIter`
- Each worker: unmarshal payload, simulate work (sleep 10ms), ack
- Print completion summary
- Verify all jobs processed (none left in `_ganso_live`)

### 1.2 `examples/stream/main.go` — Event Streaming

- Open database
- Publish 20 events to "orders" stream with keys
- Subscribe with durable consumer, read all events
- Cancel, re-subscribe with same consumer name
- Publish 5 more events
- Verify consumer resumes from saved offset (only gets 5 new)
- Print event payloads

### 1.3 `examples/notify/main.go` — Ephemeral Pub/Sub

- Open database
- Start listener on "alerts" channel
- In separate goroutine: send 5 notifications with 50ms spacing
- Listener receives all 5, prints them
- Prune notifications
- Verify prune removed records

### 1.4 `examples/scheduler/main.go` — Cron Scheduler

- Open database
- Register 2 scheduled tasks: one `@every 1s`, one cron `*/2 * * * * *` (6-field, every 2s)
- Run scheduler + worker in background
- Wait 5 seconds
- Verify each task fired expected number of times (±1)
- Pause one task, wait 2s, verify it stopped firing
- Resume, verify it fires again
- Clean shutdown

### 1.5 `examples/outbox/main.go` — Transactional Outbox

- Open database
- Create a "users" table
- In one transaction: insert user row + outbox.Send(welcome email payload)
- Run outbox worker with delivery function that appends to slice
- Verify delivery happened
- Test rollback: insert + send in tx that returns error
- Verify no delivery for rolled-back tx

### 1.6 `examples/locks/main.go` — Advisory Locks & Rate Limiting

- Open database
- Acquire lock "migration", hold for 1s
- Spawn goroutine that tries TryLock (should get ErrLockHeld)
- Spawn goroutine that uses Lock (blocking) — should acquire after release
- Rate limit: call TryRateLimit("api", 5, 10) 7 times, verify 5 allowed, 2 denied
- Sweep rate limits

### 1.7 `examples/workers/main.go` — Task Registry & Workers

- Open database
- Register 3 tasks: "add" (returns sum), "fail-once" (fails first attempt, succeeds second), "slow" (sleeps 200ms)
- Enqueue each via TaskHandle.Call
- Run workers with concurrency=2
- Wait for all results via TaskResult.Get
- Verify "add" returned correct sum
- Verify "fail-once" retried and succeeded
- Verify "slow" completed within timeout

---

## 2. Acceptance Tests (`acceptance_test.go`)

Integration tests in the main package that test realistic multi-component scenarios.

### 2.1 Queue → Worker → Result Pipeline

```
Enqueue task → Worker claims → Executes handler → Saves result → Caller reads result via WaitResult
```
- 50 tasks, 4 workers, verify all 50 results correct
- Verify no jobs left in `_ganso_live`
- Verify dead letter table empty

### 2.2 Scheduler → Queue → Worker Pipeline

```
Scheduler fires cron → Job appears in queue → Worker processes → Result saved
```
- Register periodic task with `@every 500ms`
- Run scheduler + workers for 3s
- Verify at least 4 executions happened
- Verify scheduler correctly advanced `next_fire_at`

### 2.3 Transactional Outbox Atomicity

```
Business write + outbox.Send in same tx → Both committed or both rolled back
```
- Test 1: Commit path — both visible
- Test 2: Rollback path — neither visible
- Test 3: Delivery failure + retry — verify exponential backoff timing

### 2.4 Stream Consumer Groups

```
Two consumers reading same stream, each tracking independent offsets
```
- Publish 100 events
- Consumer A reads all 100, saves offset
- Consumer B reads first 50, saves offset
- Publish 50 more
- Consumer A resumes → gets 50 new
- Consumer B resumes → gets 100 (50 remaining + 50 new)

### 2.5 Lock Contention Under Load

```
N goroutines competing for same lock
```
- 10 goroutines each try to acquire "counter-lock"
- Inside lock: read counter, increment, write back
- After all complete: verify counter == 10 (no lost updates)
- Measure total time to verify serialization happened

### 2.6 Rate Limiter Accuracy

```
Burst of requests against rate limiter, verify window enforcement
```
- Window: 10 requests per 2 seconds
- Fire 20 requests rapidly
- Verify exactly 10 allowed, 10 denied
- Wait for window to roll over
- Fire 5 more, verify all allowed

### 2.7 Notify Fan-Out

```
Multiple listeners on same channel, all receive same notification
```
- 5 listeners on "events" channel
- Send 1 notification
- All 5 receive it
- Send 10 rapid notifications
- Each listener receives all 10 (order preserved per listener)

### 2.8 Mixed Workload

```
Queue + Stream + Notify + Scheduler all running concurrently
```
- Scheduler firing tasks every 500ms
- Stream publishing events in worker handlers
- Notifications sent on task completion
- Listener collecting notifications
- Run for 3s, verify:
  - Tasks executed
  - Events published
  - Notifications received
  - No panics, no races, no deadlocks

### 2.9 Graceful Shutdown

```
All subsystems shut down cleanly on context cancellation
```
- Start: scheduler, 4 workers, stream subscriber, 2 listeners, outbox worker
- Enqueue work in progress
- Cancel root context
- Verify all goroutines exit within 2s
- Verify db.Close() succeeds
- Verify consumer offsets saved on exit

### 2.10 Visibility Timeout & Re-Claim

```
Job claimed but not acked → expires → re-claimed by another worker
```
- Enqueue 1 job, visibility timeout = 1s
- Worker A claims, does NOT ack
- Wait 1.5s
- Worker B claims same job (should succeed)
- Worker B acks
- Verify job processed exactly once to completion

---

## 3. Smoke Tests (`smoke_test.go`)

Fast (<5s total) tests that verify the critical path for each feature. Run these in CI on every commit.

### 3.1 `TestSmoke_OpenCloseReopen`
Open, write, close, reopen same file, read data back.

### 3.2 `TestSmoke_EnqueueClaimAck`
Single enqueue → claim → ack cycle. Verify job removed.

### 3.3 `TestSmoke_StreamPublishSubscribe`
Publish 1 event, subscribe, receive it.

### 3.4 `TestSmoke_NotifyListen`
Send notification, listener receives it.

### 3.5 `TestSmoke_LockUnlock`
TryLock, verify held, release, verify released.

### 3.6 `TestSmoke_RateLimit`
Allow 1 request, deny 2nd.

### 3.7 `TestSmoke_CronParse`
Parse "0 3 * * *", verify NextAfter returns 3AM.

### 3.8 `TestSmoke_SchedulerAddRemove`
Add task, list (1 result), remove, list (0 results).

### 3.9 `TestSmoke_TaskCallResult`
Register task, call, run worker, get result.

### 3.10 `TestSmoke_OutboxSendDeliver`
Send in tx, run outbox, verify delivery.

### 3.11 `TestSmoke_ClaimIter`
Use `ClaimIter` with range-over-func to process 3 jobs.

### 3.12 `TestSmoke_WatcherWake`
Subscribe to watcher, write via WithTx, verify wake received.

---

## 4. Implementation Order

| Step | What | Files | Effort |
|------|------|-------|--------|
| 1 | Smoke tests | `smoke_test.go` | 1 hr |
| 2 | Acceptance tests | `acceptance_test.go` | 3 hr |
| 3 | Examples | `examples/*/main.go` | 2 hr |
| 4 | CI integration | `Makefile` or `justfile` targets | 30 min |

### CI Targets

```makefile
test:           go test -race -timeout 60s
smoke:          go test -race -run TestSmoke -timeout 30s
acceptance:     go test -race -run TestAcceptance -timeout 120s
bench:          go test -bench=. -benchmem -timeout 120s -run=NONE
examples:       for d in examples/*/; do (cd $$d && go run .); done
all:            test smoke acceptance bench examples
```

---

## 5. Success Criteria

- All smoke tests pass in <5s
- All acceptance tests pass in <30s with `-race`
- All examples compile and run without error
- Zero race conditions detected
- No goroutine leaks (all background goroutines exit on context cancel)
- Dead letter table only contains intentionally failed jobs
- Consumer offsets accurately tracked across restart
