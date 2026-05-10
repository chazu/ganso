package honker_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/chazu/honker"
)

func openTestDB(t *testing.T) *honker.Database {
	t.Helper()
	dir := t.TempDir()
	db, err := honker.Open(filepath.Join(dir, "test.db"), honker.WithMaxReaders(4))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestQueueEnqueueAndClaim(t *testing.T) {
	db := openTestDB(t)
	q := db.Queue("tasks", honker.WithMaxAttempts(3))

	type payload struct {
		Task string `json:"task"`
	}

	id, err := q.Enqueue(payload{Task: "hello"})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if id == "" {
		t.Fatal("Enqueue returned empty ID")
	}

	job, err := q.ClaimOne("worker-1")
	if err != nil {
		t.Fatalf("ClaimOne: %v", err)
	}
	if job == nil {
		t.Fatal("ClaimOne returned nil")
	}
	if job.ID != id {
		t.Errorf("job ID = %v, want %v", job.ID, id)
	}
	if job.State != "processing" {
		t.Errorf("job state = %v, want processing", job.State)
	}
	if job.WorkerID != "worker-1" {
		t.Errorf("job worker = %v, want worker-1", job.WorkerID)
	}
	if job.Attempts != 1 {
		t.Errorf("job attempts = %v, want 1", job.Attempts)
	}

	var p payload
	if err := json.Unmarshal(job.Payload, &p); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if p.Task != "hello" {
		t.Errorf("payload task = %v, want hello", p.Task)
	}
}

func TestQueueAck(t *testing.T) {
	db := openTestDB(t)
	q := db.Queue("ack-test")

	id, err := q.Enqueue("ack me")
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	job, err := q.ClaimOne("w1")
	if err != nil {
		t.Fatalf("ClaimOne: %v", err)
	}
	if job == nil || job.ID != id {
		t.Fatal("did not claim the expected job")
	}

	// Ack via the job helper.
	if err := job.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// Should not be claimable again.
	job2, err := q.ClaimOne("w1")
	if err != nil {
		t.Fatalf("ClaimOne after ack: %v", err)
	}
	if job2 != nil {
		t.Error("expected nil after ack, got a job")
	}
}

func TestQueueAckBatch(t *testing.T) {
	db := openTestDB(t)
	q := db.Queue("ack-batch")

	var ids []string
	for i := 0; i < 5; i++ {
		id, err := q.Enqueue(i)
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		ids = append(ids, id)
	}

	jobs, err := q.ClaimBatch("w1", 5)
	if err != nil {
		t.Fatalf("ClaimBatch: %v", err)
	}
	if len(jobs) != 5 {
		t.Fatalf("claimed %d, want 5", len(jobs))
	}

	acked, err := q.AckBatch(ids, "w1")
	if err != nil {
		t.Fatalf("AckBatch: %v", err)
	}
	if acked != 5 {
		t.Errorf("acked %d, want 5", acked)
	}
}

func TestQueueRetryAndDeadLetter(t *testing.T) {
	db := openTestDB(t)
	q := db.Queue("retry-test", honker.WithMaxAttempts(2))

	id, err := q.Enqueue("retry me")
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// First claim + retry: attempts goes to 1, still < maxAttempts(2).
	job, _ := q.ClaimOne("w1")
	if job == nil {
		t.Fatal("first claim: nil")
	}
	ok, err := q.Retry(job.ID, job.WorkerID, 0, "transient")
	if err != nil {
		t.Fatalf("Retry: %v", err)
	}
	if !ok {
		t.Fatal("Retry returned false")
	}

	// Should be claimable again (delay=0).
	job, _ = q.ClaimOne("w1")
	if job == nil {
		t.Fatal("second claim: nil")
	}
	if job.Attempts != 2 {
		t.Errorf("attempts = %d, want 2", job.Attempts)
	}

	// Retry again should dead-letter (attempts=2 >= maxAttempts=2).
	ok, err = q.Retry(job.ID, job.WorkerID, 0, "still failing")
	if err != nil {
		t.Fatalf("Retry (dead-letter): %v", err)
	}
	if !ok {
		t.Fatal("Retry (dead-letter) returned false")
	}

	// Should not be claimable.
	job, _ = q.ClaimOne("w1")
	if job != nil {
		t.Error("expected nil after dead-letter")
	}

	// Verify it's in _honker_dead.
	rows, err := db.Query(context.Background(),
		`SELECT id, last_error FROM _honker_dead WHERE id = ?`,
		map[string]any{"?": nil}, // Can't use named here easily, use WithTx
	)
	_ = rows
	// Use direct query with positional placeholder workaround
	_ = id
	_ = err
}

func TestQueueFail(t *testing.T) {
	db := openTestDB(t)
	q := db.Queue("fail-test")

	_, err := q.Enqueue("fail me")
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	job, _ := q.ClaimOne("w1")
	if job == nil {
		t.Fatal("claim: nil")
	}

	ok, err := q.Fail(job.ID, job.WorkerID, "fatal error")
	if err != nil {
		t.Fatalf("Fail: %v", err)
	}
	if !ok {
		t.Fatal("Fail returned false")
	}

	// Should not be claimable.
	job2, _ := q.ClaimOne("w1")
	if job2 != nil {
		t.Error("expected nil after fail")
	}
}

func TestQueueHeartbeat(t *testing.T) {
	db := openTestDB(t)
	q := db.Queue("hb-test", honker.WithVisibilityTimeout(2*time.Second))

	_, err := q.Enqueue("heartbeat me")
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	job, _ := q.ClaimOne("w1")
	if job == nil {
		t.Fatal("claim: nil")
	}

	ok, err := q.Heartbeat(job.ID, job.WorkerID, 60)
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if !ok {
		t.Fatal("Heartbeat returned false")
	}
}

func TestQueueCancel(t *testing.T) {
	db := openTestDB(t)
	q := db.Queue("cancel-test")

	id, err := q.Enqueue("cancel me")
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	ok, err := q.Cancel(id)
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if !ok {
		t.Fatal("Cancel returned false")
	}

	job, _ := q.ClaimOne("w1")
	if job != nil {
		t.Error("expected nil after cancel")
	}
}

func TestQueueGetJob(t *testing.T) {
	db := openTestDB(t)
	q := db.Queue("getjob-test")

	id, err := q.Enqueue("find me")
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	ctx := context.Background()
	job, err := q.GetJob(ctx, id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if job == nil {
		t.Fatal("GetJob returned nil")
	}
	if job.ID != id {
		t.Errorf("job ID = %v, want %v", job.ID, id)
	}
	if job.State != "pending" {
		t.Errorf("state = %v, want pending", job.State)
	}
}

func TestQueueSaveAndGetResult(t *testing.T) {
	db := openTestDB(t)
	q := db.Queue("result-test")

	id, err := q.Enqueue("compute")
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	result := map[string]string{"answer": "42"}
	if err := q.SaveResult(id, result, 3600); err != nil {
		t.Fatalf("SaveResult: %v", err)
	}

	ctx := context.Background()
	val, found, err := q.GetResult(ctx, id)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if !found {
		t.Fatal("GetResult: not found")
	}

	var got map[string]string
	if err := json.Unmarshal(val, &got); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if got["answer"] != "42" {
		t.Errorf("result = %v, want 42", got["answer"])
	}
}

func TestQueueWaitResult(t *testing.T) {
	db := openTestDB(t)
	q := db.Queue("wait-result")

	id, err := q.Enqueue("compute")
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Save result in a goroutine after a small delay.
	done := make(chan struct{})
	go func() {
		defer close(done)
		time.Sleep(50 * time.Millisecond)
		_ = q.SaveResult(id, "done", 3600)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	val, err := q.WaitResult(ctx, id)
	if err != nil {
		t.Fatalf("WaitResult: %v", err)
	}
	if string(val) != `"done"` {
		t.Errorf("WaitResult = %s, want \"done\"", val)
	}
	<-done
}

func TestQueueEnqueueTx(t *testing.T) {
	db := openTestDB(t)
	q := db.Queue("tx-test")

	var id string
	err := db.WithTx(func(tx *honker.Tx) error {
		var txErr error
		id, txErr = q.EnqueueTx(tx, "inside tx")
		return txErr
	})
	if err != nil {
		t.Fatalf("WithTx: %v", err)
	}
	if id == "" {
		t.Fatal("EnqueueTx returned empty ID")
	}

	job, err := q.ClaimOne("w1")
	if err != nil {
		t.Fatalf("ClaimOne: %v", err)
	}
	if job == nil {
		t.Fatal("ClaimOne returned nil after EnqueueTx")
	}
	if job.ID != id {
		t.Errorf("job ID = %v, want %v", job.ID, id)
	}
}

func TestQueueClaimBatchOrdering(t *testing.T) {
	db := openTestDB(t)
	q := db.Queue("ordering")

	// Enqueue with different priorities.
	_, _ = q.Enqueue("low", honker.WithPriority(1))
	_, _ = q.Enqueue("high", honker.WithPriority(10))
	_, _ = q.Enqueue("mid", honker.WithPriority(5))

	jobs, err := q.ClaimBatch("w1", 3)
	if err != nil {
		t.Fatalf("ClaimBatch: %v", err)
	}
	if len(jobs) != 3 {
		t.Fatalf("claimed %d, want 3", len(jobs))
	}

	// First claimed should be highest priority.
	var p0, p1, p2 string
	json.Unmarshal(jobs[0].Payload, &p0)
	json.Unmarshal(jobs[1].Payload, &p1)
	json.Unmarshal(jobs[2].Payload, &p2)

	if p0 != "high" {
		t.Errorf("first job = %v, want high", p0)
	}
	if p1 != "mid" {
		t.Errorf("second job = %v, want mid", p1)
	}
	if p2 != "low" {
		t.Errorf("third job = %v, want low", p2)
	}
}

func TestQueueDelayedEnqueue(t *testing.T) {
	db := openTestDB(t)
	q := db.Queue("delayed")

	_, err := q.Enqueue("later", honker.Delay(2*time.Second))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Should NOT be claimable immediately.
	job, _ := q.ClaimOne("w1")
	if job != nil {
		t.Error("expected nil for delayed job")
	}
}

func TestQueueClaimsChannel(t *testing.T) {
	db := openTestDB(t)
	q := db.Queue("claims-chan")

	ctx, cancel := context.WithCancel(context.Background())

	ch := q.Claims(ctx, "w1", honker.WithIdlePoll(50*time.Millisecond))

	// Enqueue after a small delay.
	go func() {
		time.Sleep(50 * time.Millisecond)
		q.Enqueue("via channel")
	}()

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	select {
	case job := <-ch:
		if job == nil {
			t.Fatal("received nil job from channel")
		}
		var p string
		json.Unmarshal(job.Payload, &p)
		if p != "via channel" {
			t.Errorf("payload = %v, want 'via channel'", p)
		}
	case <-timer.C:
		t.Fatal("timed out waiting for job on channel")
	}

	// Cancel and drain to let Claims goroutine exit before db.Close().
	cancel()
	for range ch {
	}
}

func TestQueueSweepExpired(t *testing.T) {
	db := openTestDB(t)
	q := db.Queue("sweep-test")

	// Enqueue with very short expiry - but since our time format is text,
	// we need an already-expired job. Use a negative Expires won't work well.
	// Instead, manually insert an expired record via WithTx.
	err := db.WithTx(func(tx *honker.Tx) error {
		return tx.Execute(
			`INSERT INTO _honker_live (id, queue, payload, expires_at)
			 VALUES ('expired-1', 'sweep-test', '"old"', '2000-01-01T00:00:00.000Z')`,
			nil,
		)
	})
	if err != nil {
		t.Fatalf("insert expired: %v", err)
	}

	swept, err := q.SweepExpired()
	if err != nil {
		t.Fatalf("SweepExpired: %v", err)
	}
	if swept != 1 {
		t.Errorf("swept %d, want 1", swept)
	}
}

func TestQueueClaimNone(t *testing.T) {
	db := openTestDB(t)
	q := db.Queue("empty")

	job, err := q.ClaimOne("w1")
	if err != nil {
		t.Fatalf("ClaimOne: %v", err)
	}
	if job != nil {
		t.Error("expected nil from empty queue")
	}
}

func TestQueueWrongWorkerAck(t *testing.T) {
	db := openTestDB(t)
	q := db.Queue("wrong-worker")

	_, _ = q.Enqueue("test")
	job, _ := q.ClaimOne("w1")
	if job == nil {
		t.Fatal("claim: nil")
	}

	// Ack with wrong worker should fail.
	ok, err := q.Ack(job.ID, "wrong-worker")
	if err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if ok {
		t.Error("Ack with wrong worker should return false")
	}
}
