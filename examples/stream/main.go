package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/chazu/honker"
)

func main() {
	dir, _ := os.MkdirTemp("", "honker-stream-example")
	defer os.RemoveAll(dir)

	db, err := honker.Open(filepath.Join(dir, "example.db"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	s := db.Stream("orders")

	// Publish 20 events with keys.
	for i := 0; i < 20; i++ {
		_, err := s.Publish(
			map[string]any{"order_id": i, "status": "created"},
			honker.WithKey(fmt.Sprintf("order-%d", i)),
		)
		if err != nil {
			log.Fatalf("Publish %d: %v", i, err)
		}
	}
	fmt.Println("Published 20 events")

	// Subscribe with durable consumer, read all events.
	ctx1, cancel1 := context.WithTimeout(context.Background(), 3*time.Second)
	ch1 := s.Subscribe(ctx1, honker.Consumer("reader"), honker.SaveEveryN(1))
	count := 0
	for ev := range ch1 {
		count++
		if count <= 3 {
			var p map[string]any
			json.Unmarshal(ev.Payload, &p)
			fmt.Printf("  Event offset=%d order_id=%v\n", ev.Offset, p["order_id"])
		}
		if count >= 20 {
			cancel1()
			for range ch1 {
			}
			break
		}
	}
	fmt.Printf("Read %d events in first subscription\n", count)

	// Publish 5 more events.
	for i := 20; i < 25; i++ {
		s.Publish(map[string]any{"order_id": i, "status": "created"})
	}
	fmt.Println("Published 5 more events")

	// Re-subscribe with same consumer — should resume from saved offset.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	ch2 := s.Subscribe(ctx2, honker.Consumer("reader"), honker.SaveEveryN(1))
	count2 := 0
	for ev := range ch2 {
		count2++
		var p map[string]any
		json.Unmarshal(ev.Payload, &p)
		fmt.Printf("  Resumed: offset=%d order_id=%v\n", ev.Offset, p["order_id"])
		if count2 >= 5 {
			cancel2()
			for range ch2 {
			}
			break
		}
	}
	fmt.Printf("Consumer resumed and got %d new events (expected 5)\n", count2)
}
