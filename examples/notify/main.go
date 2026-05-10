package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/chazu/honker"
)

func main() {
	dir, _ := os.MkdirTemp("", "honker-notify-example")
	defer os.RemoveAll(dir)

	db, err := honker.Open(filepath.Join(dir, "example.db"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	listener, err := db.Listen("alerts", honker.FallbackPoll(50*time.Millisecond))
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	// Send 5 notifications from a goroutine.
	go func() {
		for i := 0; i < 5; i++ {
			time.Sleep(50 * time.Millisecond)
			db.WithTx(func(tx *honker.Tx) error {
				return tx.Notify("alerts", fmt.Sprintf("alert-%d", i))
			})
		}
	}()

	// Receive all 5.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for i := 0; i < 5; i++ {
		n, err := listener.Next(ctx)
		if err != nil {
			log.Fatalf("Next %d: %v", i, err)
		}
		fmt.Printf("  Received: channel=%s payload=%s\n", n.Channel, n.Payload)
	}
	fmt.Println("All 5 notifications received")

	// Prune notifications (keep at most 1, older than 0 = all eligible).
	err = db.PruneNotifications(honker.OlderThan(0), honker.MaxKeep(1))
	if err != nil {
		log.Fatalf("Prune: %v", err)
	}

	rows, _ := db.Query(context.Background(),
		`SELECT COUNT(*) as cnt FROM _honker_notifications`,
		nil,
	)
	fmt.Printf("Notifications after prune: %d\n", rows[0]["cnt"])
}
