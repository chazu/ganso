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
	dir, _ := os.MkdirTemp("", "ganso-notify-example")
	defer os.RemoveAll(dir)

	db, err := ganso.Open(filepath.Join(dir, "example.db"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	listener, err := db.Listen("alerts", ganso.FallbackPoll(50*time.Millisecond))
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	// Send 5 notifications from a goroutine.
	go func() {
		for i := 0; i < 5; i++ {
			time.Sleep(50 * time.Millisecond)
			db.WithTx(func(tx *ganso.Tx) error {
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
	err = db.PruneNotifications(ganso.OlderThan(0), ganso.MaxKeep(1))
	if err != nil {
		log.Fatalf("Prune: %v", err)
	}

	rows, _ := db.Query(context.Background(),
		`SELECT COUNT(*) as cnt FROM _ganso_notifications`,
		nil,
	)
	fmt.Printf("Notifications after prune: %d\n", rows[0]["cnt"])
}
