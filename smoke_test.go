package honker_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/chazu/honker"
)

func TestSmoke_OpenCloseReopen(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := honker.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	err = db.WithTx(func(tx *honker.Tx) error {
		return tx.Notify("test", "hello")
	})
	if err != nil {
		t.Fatalf("WithTx: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := honker.Open(dbPath)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()

	rows, err := db2.Query(context.Background(),
		`SELECT payload FROM _honker_notifications WHERE channel = :ch`,
		map[string]any{":ch": "test"},
	)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows) != 1 || rows[0]["payload"] != "hello" {
		t.Fatalf("expected 1 row with payload 'hello', got %v", rows)
	}
}

func TestSmoke_EnqueueClaimAck(t *testing.T) {
	db := openTestDB(t)
	q := db.Queue("tasks")

	id, err := q.Enqueue(map[string]string{"job": "smoke"})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	job, err := q.ClaimOne("w1")
	if err != nil {
		t.Fatalf("ClaimOne: %v", err)
	}
	if job == nil {
		t.Fatal("ClaimOne returned nil")
	}
	if job.ID != id {
		t.Fatalf("job ID = %s, want %s", job.ID, id)
	}

	if err := job.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	got, err := q.GetJob(context.Background(), id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got != nil {
		t.Fatal("job should be gone after ack")
	}
}

func TestSmoke_StreamPublishSubscribe(t *testing.T) {
	db := openTestDB(t)
	s := db.Stream("events")

	_, err := s.Publish(map[string]string{"event": "created"}, honker.WithKey("k1"))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ch := s.Subscribe(ctx)
	select {
	case ev := <-ch:
		var payload map[string]string
		if err := json.Unmarshal(ev.Payload, &payload); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if payload["event"] != "created" {
			t.Fatalf("payload = %v, want created", payload)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for event")
	}
}

func TestSmoke_NotifyListen(t *testing.T) {
	db := openTestDB(t)

	listener, err := db.Listen("alerts", honker.FallbackPoll(50*time.Millisecond))
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer listener.Close()

	err = db.WithTx(func(tx *honker.Tx) error {
		return tx.Notify("alerts", "fire")
	})
	if err != nil {
		t.Fatalf("Notify: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	n, err := listener.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if string(n.Payload) != `"fire"` && string(n.Payload) != "fire" {
		t.Fatalf("payload = %s, want fire", n.Payload)
	}
}

func TestSmoke_LockUnlock(t *testing.T) {
	db := openTestDB(t)

	lock, err := db.TryLock("migration", honker.WithTTL(10*time.Second))
	if err != nil {
		t.Fatalf("TryLock: %v", err)
	}

	_, err = db.TryLock("migration")
	if err != honker.ErrLockHeld {
		t.Fatalf("second TryLock = %v, want ErrLockHeld", err)
	}

	if err := lock.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}

	lock2, err := db.TryLock("migration")
	if err != nil {
		t.Fatalf("TryLock after release: %v", err)
	}
	lock2.Release()
}

func TestSmoke_RateLimit(t *testing.T) {
	db := openTestDB(t)

	allowed, err := db.TryRateLimit("api", 1, 60)
	if err != nil {
		t.Fatalf("TryRateLimit: %v", err)
	}
	if !allowed {
		t.Fatal("first request should be allowed")
	}

	allowed, err = db.TryRateLimit("api", 1, 60)
	if err != nil {
		t.Fatalf("TryRateLimit: %v", err)
	}
	if allowed {
		t.Fatal("second request should be denied")
	}
}

func TestSmoke_CronParse(t *testing.T) {
	sched, err := honker.ParseSchedule("0 3 * * *")
	if err != nil {
		t.Fatalf("ParseSchedule: %v", err)
	}

	ref := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	next := sched.NextAfter(ref)
	if next.Hour() != 3 || next.Minute() != 0 {
		t.Fatalf("NextAfter = %v, want 03:00", next)
	}
}

func TestSmoke_SchedulerAddRemove(t *testing.T) {
	db := openTestDB(t)
	sched := db.Scheduler()

	schedule, err := honker.ParseSchedule("@every 60s")
	if err != nil {
		t.Fatalf("ParseSchedule: %v", err)
	}

	if err := sched.Add("cleanup", "tasks", schedule); err != nil {
		t.Fatalf("Add: %v", err)
	}

	list, err := sched.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 1 || list[0].Name != "cleanup" {
		t.Fatalf("List = %v, want 1 entry named cleanup", list)
	}

	removed, err := sched.Remove("cleanup")
	if err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if !removed {
		t.Fatal("Remove should return true")
	}

	list, err = sched.List()
	if err != nil {
		t.Fatalf("List after remove: %v", err)
	}
	if len(list) != 0 {
		t.Fatalf("List after remove = %v, want empty", list)
	}
}

func TestSmoke_TaskCallResult(t *testing.T) {
	db := openTestDB(t)
	q := db.Queue("tasks")

	handle := q.Task("add", func(ctx context.Context, payload json.RawMessage) (any, error) {
		var nums []int
		if err := json.Unmarshal(payload, &nums); err != nil {
			return nil, err
		}
		sum := 0
		for _, n := range nums {
			sum += n
		}
		return sum, nil
	}, honker.WithStoreResult(true), honker.WithResultTTL(time.Minute))

	result, err := handle.Call([]int{1, 2, 3})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}

	workerCtx, workerCancel := context.WithCancel(context.Background())
	workerDone := make(chan struct{})
	go func() {
		db.RunWorkers(workerCtx, honker.WorkerOptions{Queue: "tasks", Concurrency: 1})
		close(workerDone)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	raw, err := result.Get(ctx)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	var sum int
	if err := json.Unmarshal(raw, &sum); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if sum != 6 {
		t.Fatalf("sum = %d, want 6", sum)
	}

	workerCancel()
	<-workerDone
}

func TestSmoke_OutboxSendDeliver(t *testing.T) {
	db := openTestDB(t)
	ob := db.Outbox("emails")

	var delivered []string

	err := db.WithTx(func(tx *honker.Tx) error {
		_, err := ob.Send(tx, map[string]string{"to": "user@example.com"})
		return err
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		ob.Run(ctx, func(_ context.Context, payload json.RawMessage) error {
			var msg map[string]string
			json.Unmarshal(payload, &msg)
			delivered = append(delivered, msg["to"])
			cancel()
			return nil
		})
		close(done)
	}()

	<-done
	if len(delivered) != 1 || delivered[0] != "user@example.com" {
		t.Fatalf("delivered = %v, want [user@example.com]", delivered)
	}
}

func TestSmoke_ClaimIter(t *testing.T) {
	db := openTestDB(t)
	q := db.Queue("iter-q")

	for i := 0; i < 3; i++ {
		if _, err := q.Enqueue(i); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var processed int
	for job := range q.ClaimIter(ctx, "w1") {
		if err := job.Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
		processed++
		if processed == 3 {
			cancel()
		}
	}
	if processed != 3 {
		t.Fatalf("processed = %d, want 3", processed)
	}
}

func TestSmoke_WatcherWake(t *testing.T) {
	db := openTestDB(t)

	q := db.Queue("watcher-q")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ch := q.Claims(ctx, "w1")

	go func() {
		time.Sleep(50 * time.Millisecond)
		q.Enqueue(map[string]string{"test": "wake"})
	}()

	select {
	case job := <-ch:
		if job == nil {
			t.Fatal("received nil job")
		}
		job.Ack()
	case <-ctx.Done():
		t.Fatal("timeout: watcher did not wake consumer")
	}
}
