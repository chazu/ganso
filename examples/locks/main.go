package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/chazu/ganso"
)

func main() {
	dir, _ := os.MkdirTemp("", "ganso-locks-example")
	defer os.RemoveAll(dir)

	db, err := ganso.Open(filepath.Join(dir, "example.db"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Acquire lock.
	lock, err := db.TryLock("migration", ganso.WithTTL(10*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Acquired lock 'migration'")

	// Another goroutine tries TryLock — should get ErrLockHeld.
	done1 := make(chan struct{})
	go func() {
		defer close(done1)
		_, err := db.TryLock("migration")
		if err == ganso.ErrLockHeld {
			fmt.Println("  TryLock: correctly got ErrLockHeld")
		} else {
			fmt.Printf("  TryLock: unexpected result: %v\n", err)
		}
	}()
	<-done1

	// Another goroutine uses blocking Lock — should acquire after release.
	done2 := make(chan struct{})
	go func() {
		defer close(done2)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		l, err := db.Lock(ctx, "migration", ganso.WithTTL(5*time.Second))
		if err != nil {
			fmt.Printf("  Lock (blocking): error: %v\n", err)
			return
		}
		fmt.Println("  Lock (blocking): acquired after release")
		l.Release()
	}()

	time.Sleep(500 * time.Millisecond)
	lock.Release()
	fmt.Println("Released lock 'migration'")
	<-done2

	// Rate limiting.
	fmt.Println("\nRate limiting: 5 requests per 10s window")
	var allowed, denied int
	for i := 0; i < 7; i++ {
		ok, err := db.TryRateLimit("api", 5, 10)
		if err != nil {
			log.Fatal(err)
		}
		if ok {
			allowed++
			fmt.Printf("  Request %d: ALLOWED\n", i+1)
		} else {
			denied++
			fmt.Printf("  Request %d: DENIED\n", i+1)
		}
	}
	fmt.Printf("Allowed: %d, Denied: %d\n", allowed, denied)

	// Sweep rate limits.
	swept, err := db.SweepRateLimits(0)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Swept %d rate limit records\n", swept)
}
