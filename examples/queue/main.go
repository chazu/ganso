package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chazu/honker"
)

func main() {
	dir, _ := os.MkdirTemp("", "honker-queue-example")
	defer os.RemoveAll(dir)

	db, err := honker.Open(filepath.Join(dir, "example.db"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	q := db.Queue("jobs", honker.WithVisibilityTimeout(5*time.Second))

	// Enqueue 10 jobs with mixed priorities and delays.
	for i := 0; i < 10; i++ {
		priority := i % 3
		var opts []honker.EnqueueOption
		opts = append(opts, honker.WithPriority(priority))
		if i%2 == 0 {
			opts = append(opts, honker.Delay(time.Duration(i)*time.Millisecond))
		}
		_, err := q.Enqueue(map[string]any{"task_num": i, "priority": priority}, opts...)
		if err != nil {
			log.Fatalf("Enqueue %d: %v", i, err)
		}
	}
	fmt.Println("Enqueued 10 jobs")

	// Spawn 3 worker goroutines via ClaimIter.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var processed atomic.Int64
	var wg sync.WaitGroup

	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			wid := fmt.Sprintf("worker-%d", workerID)
			for job := range q.ClaimIter(ctx, wid) {
				var payload map[string]any
				json.Unmarshal(job.Payload, &payload)
				time.Sleep(10 * time.Millisecond)
				job.Ack()
				count := processed.Add(1)
				fmt.Printf("  Worker %d processed task %v (total: %d)\n", workerID, payload["task_num"], count)
				if count >= 10 {
					cancel()
				}
			}
		}(w)
	}

	wg.Wait()
	fmt.Printf("All done. Processed %d jobs.\n", processed.Load())

	// Verify no jobs left in _honker_live.
	rows, _ := db.Query(context.Background(),
		`SELECT COUNT(*) as cnt FROM _honker_live WHERE queue = :q`,
		map[string]any{":q": "jobs"},
	)
	fmt.Printf("Jobs remaining in live queue: %d\n", rows[0]["cnt"])
}
