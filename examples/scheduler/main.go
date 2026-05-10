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

	"github.com/chazu/ganso"
)

func main() {
	dir, _ := os.MkdirTemp("", "ganso-scheduler-example")
	defer os.RemoveAll(dir)

	db, err := ganso.Open(filepath.Join(dir, "example.db"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	q := db.Queue("scheduled")

	var everySecCount atomic.Int64
	var every2sCount atomic.Int64

	q.PeriodicTask("every-1s", ganso.Every(1*time.Second), func(ctx context.Context, payload json.RawMessage) (any, error) {
		everySecCount.Add(1)
		fmt.Printf("  every-1s fired (count: %d)\n", everySecCount.Load())
		return nil, nil
	})

	q.PeriodicTask("every-2s", ganso.Every(2*time.Second), func(ctx context.Context, payload json.RawMessage) (any, error) {
		every2sCount.Add(1)
		fmt.Printf("  every-2s fired (count: %d)\n", every2sCount.Load())
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
		db.RunWorkers(ctx, ganso.WorkerOptions{Queue: "scheduled", Concurrency: 2})
	}()

	// Run for 5 seconds.
	time.Sleep(5 * time.Second)
	fmt.Printf("After 5s: every-1s=%d every-2s=%d\n", everySecCount.Load(), every2sCount.Load())

	// Pause one task.
	paused, err := sched.Pause("every-1s")
	if err != nil {
		log.Fatalf("Pause: %v", err)
	}
	fmt.Printf("Paused every-1s: %v\n", paused)

	beforePause := everySecCount.Load()
	time.Sleep(2 * time.Second)
	afterPause := everySecCount.Load()
	fmt.Printf("During pause: every-1s count unchanged = %v\n", beforePause == afterPause)

	// Resume.
	resumed, err := sched.Resume("every-1s")
	if err != nil {
		log.Fatalf("Resume: %v", err)
	}
	fmt.Printf("Resumed every-1s: %v\n", resumed)

	time.Sleep(2 * time.Second)
	afterResume := everySecCount.Load()
	fmt.Printf("After resume: every-1s fired again = %v (count: %d)\n", afterResume > afterPause, afterResume)

	cancel()
	wg.Wait()
	fmt.Println("Clean shutdown complete")
}
