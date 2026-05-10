package honker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chazu/honker"
)

func TestAcceptance_QueueWorkerResultPipeline(t *testing.T) {
	db := openTestDB(t)
	q := db.Queue("pipeline")

	q.Task("double", func(ctx context.Context, payload json.RawMessage) (any, error) {
		var n int
		json.Unmarshal(payload, &n)
		return n * 2, nil
	}, honker.WithStoreResult(true), honker.WithResultTTL(time.Minute))

	const total = 50
	results := make([]*honker.TaskResult, total)
	for i := 0; i < total; i++ {
		handle := q.Task("double", func(ctx context.Context, payload json.RawMessage) (any, error) {
			var n int
			json.Unmarshal(payload, &n)
			return n * 2, nil
		}, honker.WithStoreResult(true), honker.WithResultTTL(time.Minute))
		r, err := handle.Call(i)
		if err != nil {
			t.Fatalf("Call %d: %v", i, err)
		}
		results[i] = r
	}

	workerCtx, workerCancel := context.WithCancel(context.Background())
	workerDone := make(chan struct{})
	go func() {
		db.RunWorkers(workerCtx, honker.WorkerOptions{Queue: "pipeline", Concurrency: 4})
		close(workerDone)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	for i, r := range results {
		raw, err := r.Get(ctx)
		if err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
		var val int
		json.Unmarshal(raw, &val)
		if val != i*2 {
			t.Fatalf("result %d = %d, want %d", i, val, i*2)
		}
	}

	workerCancel()
	<-workerDone

	rows, err := db.Query(context.Background(),
		`SELECT COUNT(*) as cnt FROM _honker_live WHERE queue = :q`,
		map[string]any{":q": "pipeline"},
	)
	if err != nil {
		t.Fatalf("query live: %v", err)
	}
	if rows[0]["cnt"].(int64) != 0 {
		t.Fatalf("live queue not empty: %d jobs remaining", rows[0]["cnt"])
	}

	rows, err = db.Query(context.Background(),
		`SELECT COUNT(*) as cnt FROM _honker_dead WHERE queue = :q`,
		map[string]any{":q": "pipeline"},
	)
	if err != nil {
		t.Fatalf("query dead: %v", err)
	}
	if rows[0]["cnt"].(int64) != 0 {
		t.Fatalf("dead letter not empty: %d jobs", rows[0]["cnt"])
	}
}

func TestAcceptance_SchedulerQueueWorkerPipeline(t *testing.T) {
	db := openTestDB(t)
	q := db.Queue("sched-q")

	var count atomic.Int64
	q.PeriodicTask("tick", honker.Every(1*time.Second), func(ctx context.Context, payload json.RawMessage) (any, error) {
		count.Add(1)
		return nil, nil
	})

	sched := db.Scheduler()

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		sched.Run(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		db.RunWorkers(ctx, honker.WorkerOptions{Queue: "sched-q", Concurrency: 2})
	}()

	time.Sleep(5 * time.Second)
	cancel()
	wg.Wait()

	c := count.Load()
	if c < 3 {
		t.Fatalf("expected at least 3 executions, got %d", c)
	}
}

func TestAcceptance_OutboxAtomicity(t *testing.T) {
	db := openTestDB(t)
	ob := db.Outbox("tx-outbox")

	var delivered sync.Map

	// Test 1: Commit path
	err := db.WithTx(func(tx *honker.Tx) error {
		_, err := ob.Send(tx, map[string]string{"action": "commit"})
		return err
	})
	if err != nil {
		t.Fatalf("commit path send: %v", err)
	}

	// Test 2: Rollback path
	err = db.WithTx(func(tx *honker.Tx) error {
		ob.Send(tx, map[string]string{"action": "rollback"})
		return fmt.Errorf("forced rollback")
	})
	if err == nil {
		t.Fatal("expected error from rollback tx")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		ob.Run(ctx, func(_ context.Context, payload json.RawMessage) error {
			var msg map[string]string
			json.Unmarshal(payload, &msg)
			delivered.Store(msg["action"], true)
			return nil
		})
		close(done)
	}()

	time.Sleep(1 * time.Second)
	cancel()
	<-done

	if _, ok := delivered.Load("commit"); !ok {
		t.Fatal("committed message should have been delivered")
	}
	if _, ok := delivered.Load("rollback"); ok {
		t.Fatal("rolled-back message should NOT have been delivered")
	}
}

func TestAcceptance_StreamConsumerGroups(t *testing.T) {
	db := openTestDB(t)
	s := db.Stream("orders")

	for i := 0; i < 100; i++ {
		if _, err := s.Publish(fmt.Sprintf("event-%d", i)); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}

	readN := func(opts []honker.SubscribeOption, n int) []honker.Event {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		ch := s.Subscribe(ctx, opts...)
		var events []honker.Event
		for ev := range ch {
			events = append(events, ev)
			if len(events) >= n {
				cancel()
				for range ch {
				}
				return events
			}
		}
		return events
	}

	// Consumer A reads all 100 with auto-save
	eventsA := readN([]honker.SubscribeOption{honker.Consumer("consumer-a"), honker.SaveEveryN(1)}, 100)
	if len(eventsA) != 100 {
		t.Fatalf("consumer A got %d events, want 100", len(eventsA))
	}

	// Consumer B reads first 50 WITHOUT consumer (no auto-save)
	eventsB := readN(nil, 50)
	if len(eventsB) < 50 {
		t.Fatalf("consumer B got %d events, want 50", len(eventsB))
	}
	// Manually save B's offset at the 50th event
	if err := s.SaveOffset("consumer-b", eventsB[49].Offset); err != nil {
		t.Fatalf("SaveOffset B: %v", err)
	}

	// Publish 50 more
	for i := 100; i < 150; i++ {
		if _, err := s.Publish(fmt.Sprintf("event-%d", i)); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}

	// Consumer A resumes from saved offset — should get 50 new
	eventsA2 := readN([]honker.SubscribeOption{honker.Consumer("consumer-a"), honker.SaveEveryN(1)}, 50)
	if len(eventsA2) != 50 {
		t.Fatalf("consumer A resumed got %d events, want 50", len(eventsA2))
	}

	// Consumer B resumes from offset 50 — should get 50 remaining + 50 new = 100
	eventsB2 := readN([]honker.SubscribeOption{honker.Consumer("consumer-b"), honker.SaveEveryN(1)}, 100)
	if len(eventsB2) != 100 {
		t.Fatalf("consumer B resumed got %d events, want 100", len(eventsB2))
	}
}

func TestAcceptance_LockContention(t *testing.T) {
	db := openTestDB(t)

	var counter int64
	var mu sync.Mutex
	var wg sync.WaitGroup

	const n = 10
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			lock, err := db.Lock(ctx, "counter-lock", honker.WithTTL(5*time.Second))
			if err != nil {
				t.Errorf("Lock: %v", err)
				return
			}
			defer lock.Release()

			mu.Lock()
			counter++
			mu.Unlock()
		}()
	}

	wg.Wait()

	if counter != n {
		t.Fatalf("counter = %d, want %d", counter, n)
	}
}

func TestAcceptance_RateLimiterAccuracy(t *testing.T) {
	db := openTestDB(t)

	var allowed, denied int
	for i := 0; i < 20; i++ {
		ok, err := db.TryRateLimit("burst-test", 10, 2)
		if err != nil {
			t.Fatalf("TryRateLimit: %v", err)
		}
		if ok {
			allowed++
		} else {
			denied++
		}
	}

	if allowed != 10 {
		t.Fatalf("allowed = %d, want 10", allowed)
	}
	if denied != 10 {
		t.Fatalf("denied = %d, want 10", denied)
	}
}

func TestAcceptance_NotifyFanOut(t *testing.T) {
	db := openTestDB(t)

	const numListeners = 5
	listeners := make([]*honker.Listener, numListeners)
	for i := 0; i < numListeners; i++ {
		l, err := db.Listen("events", honker.FallbackPoll(50*time.Millisecond))
		if err != nil {
			t.Fatalf("Listen %d: %v", i, err)
		}
		defer l.Close()
		listeners[i] = l
	}

	err := db.WithTx(func(tx *honker.Tx) error {
		return tx.Notify("events", "broadcast")
	})
	if err != nil {
		t.Fatalf("Notify: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var received atomic.Int64
	var wg sync.WaitGroup
	for i, l := range listeners {
		wg.Add(1)
		go func(idx int, listener *honker.Listener) {
			defer wg.Done()
			n, err := listener.Next(ctx)
			if err != nil {
				t.Errorf("listener %d: %v", idx, err)
				return
			}
			_ = n
			received.Add(1)
		}(i, l)
	}

	wg.Wait()
	if received.Load() != numListeners {
		t.Fatalf("received = %d, want %d", received.Load(), numListeners)
	}

	// Send 10 rapid notifications
	for i := 0; i < 10; i++ {
		err := db.WithTx(func(tx *honker.Tx) error {
			return tx.Notify("events", fmt.Sprintf("msg-%d", i))
		})
		if err != nil {
			t.Fatalf("Notify %d: %v", i, err)
		}
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()

	for li, l := range listeners {
		for j := 0; j < 10; j++ {
			_, err := l.Next(ctx2)
			if err != nil {
				t.Fatalf("listener %d msg %d: %v", li, j, err)
			}
		}
	}
}

func TestAcceptance_MixedWorkload(t *testing.T) {
	db := openTestDB(t)
	q := db.Queue("mixed")
	s := db.Stream("mixed-events")

	var tasksExecuted atomic.Int64
	var eventsPublished atomic.Int64
	var notificationsReceived atomic.Int64

	q.PeriodicTask("work", honker.Every(1*time.Second), func(ctx context.Context, payload json.RawMessage) (any, error) {
		tasksExecuted.Add(1)
		s.Publish(map[string]string{"from": "worker"})
		eventsPublished.Add(1)
		return nil, nil
	})

	sched := db.Scheduler()

	listener, err := db.Listen("done", honker.FallbackPoll(50*time.Millisecond))
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer listener.Close()

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		sched.Run(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		db.RunWorkers(ctx, honker.WorkerOptions{Queue: "mixed", Concurrency: 2})
	}()

	// Notification listener
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			n, err := listener.Next(ctx)
			if err != nil {
				return
			}
			_ = n
			notificationsReceived.Add(1)
		}
	}()

	// Send some notifications
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			db.WithTx(func(tx *honker.Tx) error {
				return tx.Notify("done", fmt.Sprintf("n-%d", i))
			})
			time.Sleep(100 * time.Millisecond)
		}
	}()

	time.Sleep(5 * time.Second)
	cancel()
	wg.Wait()

	if tasksExecuted.Load() == 0 {
		t.Fatal("no tasks executed")
	}
	if eventsPublished.Load() == 0 {
		t.Fatal("no events published")
	}
	if notificationsReceived.Load() == 0 {
		t.Fatal("no notifications received")
	}
}

func TestAcceptance_GracefulShutdown(t *testing.T) {
	db := openTestDB(t)
	q := db.Queue("shutdown-q")
	s := db.Stream("shutdown-s")

	q.Task("slow", func(ctx context.Context, payload json.RawMessage) (any, error) {
		time.Sleep(100 * time.Millisecond)
		return nil, nil
	})

	// Enqueue some work
	for i := 0; i < 5; i++ {
		q.Enqueue(i)
	}

	sched := db.Scheduler()
	sched.Add("shutdown-tick", "shutdown-q", honker.Every(1*time.Second))

	listener, err := db.Listen("shutdown-ch", honker.FallbackPoll(50*time.Millisecond))
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		sched.Run(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		db.RunWorkers(ctx, honker.WorkerOptions{Queue: "shutdown-q", Concurrency: 4})
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		subCtx, subCancel := context.WithCancel(ctx)
		defer subCancel()
		ch := s.Subscribe(subCtx, honker.Consumer("shutdown-consumer"))
		for range ch {
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			_, err := listener.Next(ctx)
			if err != nil {
				return
			}
		}
	}()

	time.Sleep(500 * time.Millisecond)
	cancel()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All goroutines exited cleanly
	case <-time.After(5 * time.Second):
		t.Fatal("goroutines did not exit within 5s")
	}

	listener.Close()
}

func TestAcceptance_VisibilityTimeoutReClaim(t *testing.T) {
	db := openTestDB(t)
	q := db.Queue("vis-q", honker.WithVisibilityTimeout(1*time.Second))

	id, err := q.Enqueue(map[string]string{"task": "reclaim"})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Worker A claims but does NOT ack
	jobA, err := q.ClaimOne("worker-a")
	if err != nil {
		t.Fatalf("ClaimOne A: %v", err)
	}
	if jobA == nil || jobA.ID != id {
		t.Fatal("worker A should claim the job")
	}

	// Wait for visibility timeout to expire
	time.Sleep(1500 * time.Millisecond)

	// Worker B should be able to claim same job
	jobB, err := q.ClaimOne("worker-b")
	if err != nil {
		t.Fatalf("ClaimOne B: %v", err)
	}
	if jobB == nil {
		t.Fatal("worker B should claim expired job")
	}
	if jobB.ID != id {
		t.Fatalf("worker B got %s, want %s", jobB.ID, id)
	}

	if err := jobB.Ack(); err != nil {
		t.Fatalf("Ack B: %v", err)
	}

	got, err := q.GetJob(context.Background(), id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got != nil {
		t.Fatal("job should be gone after worker B acked")
	}
}
