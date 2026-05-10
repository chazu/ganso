package ganso_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chazu/ganso"
)

func TestTaskRegistryBasic(t *testing.T) {
	reg := ganso.NewRegistry()

	fn := func(_ context.Context, _ json.RawMessage) (any, error) { return nil, nil }
	err := reg.Register(ganso.TaskSpec{Name: "foo", Fn: fn, QueueName: "q"})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	spec, ok := reg.Get("foo")
	if !ok {
		t.Fatal("expected to find foo")
	}
	if spec.QueueName != "q" {
		t.Errorf("queue = %q, want q", spec.QueueName)
	}

	names := reg.Names()
	if len(names) != 1 || names[0] != "foo" {
		t.Errorf("Names = %v, want [foo]", names)
	}

	queues := reg.Queues()
	if len(queues) != 1 || queues[0] != "q" {
		t.Errorf("Queues = %v, want [q]", queues)
	}
}

func TestTaskCallAndWorker(t *testing.T) {
	db := openTestDB(t)
	q := db.Queue("work")
	reg := ganso.NewRegistry()

	var called atomic.Bool
	fn := func(_ context.Context, payload json.RawMessage) (any, error) {
		called.Store(true)
		var m map[string]string
		json.Unmarshal(payload, &m)
		return map[string]string{"echo": m["input"]}, nil
	}

	spec := ganso.TaskSpec{
		Name:        "echo-task",
		Fn:          fn,
		QueueName:   "work",
		Retries:     3,
		RetryDelay:  time.Second,
		Timeout:     5 * time.Second,
		StoreResult: true,
		ResultTTL:   60 * time.Second,
	}
	reg.Register(spec)

	handle := q.Task("echo-task", fn, ganso.WithStoreResult(true), ganso.WithResultTTL(60*time.Second))

	result, err := handle.Call(map[string]string{"input": "hello"})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Start worker.
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		db.RunWorkers(ctx, ganso.WorkerOptions{
			Queue:       "work",
			Concurrency: 1,
			Registry:    reg,
		})
	}()

	// Wait for result.
	getCtx, getCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer getCancel()

	raw, err := result.Get(getCtx)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if !called.Load() {
		t.Error("task function was not called")
	}

	var m map[string]string
	json.Unmarshal(raw, &m)
	if m["echo"] != "hello" {
		t.Errorf("result = %v, want echo=hello", m)
	}

	cancel()
	<-workerDone
}

func TestTaskRetryOnError(t *testing.T) {
	db := openTestDB(t)
	q := db.Queue("retry-q", ganso.WithMaxAttempts(3))
	reg := ganso.NewRegistry()

	var attempts atomic.Int32
	fn := func(_ context.Context, _ json.RawMessage) (any, error) {
		n := attempts.Add(1)
		if n < 3 {
			return nil, fmt.Errorf("fail attempt %d", n)
		}
		return "ok", nil
	}

	reg.Register(ganso.TaskSpec{
		Name:       "retry-me",
		Fn:         fn,
		QueueName:  "retry-q",
		Retries:    3,
		RetryDelay: 100 * time.Millisecond,
	})

	handle := q.Task("retry-me", fn, ganso.WithRetries(3), ganso.WithRetryDelay(100*time.Millisecond))
	handle.Call(nil)

	ctx, cancel := context.WithCancel(context.Background())

	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		db.RunWorkers(ctx, ganso.WorkerOptions{
			Queue:       "retry-q",
			Concurrency: 1,
			Registry:    reg,
		})
	}()

	// Wait for at least 3 attempts.
	deadline := time.After(10 * time.Second)
	for attempts.Load() < 3 {
		select {
		case <-deadline:
			t.Fatalf("timed out, only %d attempts", attempts.Load())
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}
	cancel()
	<-workerDone
}

func TestTaskCallLocal(t *testing.T) {
	db := openTestDB(t)
	q := db.Queue("local-q")

	fn := func(_ context.Context, payload json.RawMessage) (any, error) {
		return "direct", nil
	}

	handle := q.Task("local-task", fn)

	result, err := handle.CallLocal(context.Background(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("CallLocal: %v", err)
	}
	if result != "direct" {
		t.Errorf("result = %v, want direct", result)
	}
}
