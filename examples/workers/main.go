package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/chazu/ganso"
)

func main() {
	dir, _ := os.MkdirTemp("", "ganso-workers-example")
	defer os.RemoveAll(dir)

	db, err := ganso.Open(filepath.Join(dir, "example.db"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	q := db.Queue("tasks")

	// Register "add" task — returns sum of numbers.
	addHandle := q.Task("add", func(ctx context.Context, payload json.RawMessage) (any, error) {
		var nums []int
		json.Unmarshal(payload, &nums)
		sum := 0
		for _, n := range nums {
			sum += n
		}
		return sum, nil
	}, ganso.WithStoreResult(true), ganso.WithResultTTL(time.Minute))

	// Register "fail-once" task — fails first attempt, succeeds second.
	var failOnceAttempts atomic.Int64
	failOnceHandle := q.Task("fail-once", func(ctx context.Context, payload json.RawMessage) (any, error) {
		attempt := failOnceAttempts.Add(1)
		if attempt == 1 {
			return nil, fmt.Errorf("transient error")
		}
		return "recovered", nil
	}, ganso.WithStoreResult(true), ganso.WithResultTTL(time.Minute),
		ganso.WithRetries(3), ganso.WithRetryDelay(100*time.Millisecond))

	// Register "slow" task — takes 200ms.
	slowHandle := q.Task("slow", func(ctx context.Context, payload json.RawMessage) (any, error) {
		time.Sleep(200 * time.Millisecond)
		return "done", nil
	}, ganso.WithStoreResult(true), ganso.WithResultTTL(time.Minute),
		ganso.WithTimeout(5*time.Second))

	// Enqueue each task.
	addResult, err := addHandle.Call([]int{10, 20, 30})
	if err != nil {
		log.Fatal(err)
	}
	failResult, err := failOnceHandle.Call(nil)
	if err != nil {
		log.Fatal(err)
	}
	slowResult, err := slowHandle.Call(nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Enqueued 3 tasks: add, fail-once, slow")

	// Run workers.
	workerCtx, workerCancel := context.WithCancel(context.Background())
	workerDone := make(chan struct{})
	go func() {
		db.RunWorkers(workerCtx, ganso.WorkerOptions{Queue: "tasks", Concurrency: 2})
		close(workerDone)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Wait for "add" result.
	raw, err := addResult.Get(ctx)
	if err != nil {
		log.Fatalf("add Get: %v", err)
	}
	var sum int
	json.Unmarshal(raw, &sum)
	fmt.Printf("  add result: %d (expected 60)\n", sum)

	// Wait for "fail-once" result.
	raw, err = failResult.Get(ctx)
	if err != nil {
		log.Fatalf("fail-once Get: %v", err)
	}
	var recovered string
	json.Unmarshal(raw, &recovered)
	fmt.Printf("  fail-once result: %q (expected \"recovered\")\n", recovered)

	// Wait for "slow" result.
	raw, err = slowResult.Get(ctx)
	if err != nil {
		log.Fatalf("slow Get: %v", err)
	}
	var slowVal string
	json.Unmarshal(raw, &slowVal)
	fmt.Printf("  slow result: %q (expected \"done\")\n", slowVal)

	workerCancel()
	<-workerDone
	fmt.Println("All tasks completed successfully")
}
