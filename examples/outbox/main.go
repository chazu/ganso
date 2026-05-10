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
	dir, _ := os.MkdirTemp("", "honker-outbox-example")
	defer os.RemoveAll(dir)

	db, err := honker.Open(filepath.Join(dir, "example.db"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create a "users" table.
	err = db.WithTx(func(tx *honker.Tx) error {
		return tx.Execute(`CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`, nil)
	})
	if err != nil {
		log.Fatal(err)
	}

	ob := db.Outbox("emails")
	var deliveries []string

	// Commit path: insert user + send welcome email in one transaction.
	err = db.WithTx(func(tx *honker.Tx) error {
		if err := tx.Execute(`INSERT INTO users (name, email) VALUES (:name, :email)`,
			map[string]any{":name": "Alice", ":email": "alice@example.com"}); err != nil {
			return err
		}
		_, err := ob.Send(tx, map[string]string{"type": "welcome", "to": "alice@example.com"})
		return err
	})
	if err != nil {
		log.Fatalf("commit path: %v", err)
	}
	fmt.Println("Committed: user insert + outbox send")

	// Rollback path: both should be discarded.
	err = db.WithTx(func(tx *honker.Tx) error {
		tx.Execute(`INSERT INTO users (name, email) VALUES (:name, :email)`,
			map[string]any{":name": "Bob", ":email": "bob@example.com"})
		ob.Send(tx, map[string]string{"type": "welcome", "to": "bob@example.com"})
		return fmt.Errorf("simulated failure")
	})
	fmt.Printf("Rollback: err=%v (expected)\n", err)

	// Run outbox worker.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		ob.Run(ctx, func(_ context.Context, payload json.RawMessage) error {
			var msg map[string]string
			json.Unmarshal(payload, &msg)
			deliveries = append(deliveries, msg["to"])
			fmt.Printf("  Delivered email to: %s\n", msg["to"])
			cancel()
			return nil
		})
		close(done)
	}()

	<-done

	fmt.Printf("Total deliveries: %d (should be 1, only Alice)\n", len(deliveries))

	// Verify Bob's user row doesn't exist.
	rows, _ := db.Query(context.Background(),
		`SELECT COUNT(*) as cnt FROM users WHERE name = :name`,
		map[string]any{":name": "Bob"},
	)
	fmt.Printf("Bob in users table: %v (should be 0)\n", rows[0]["cnt"])
}
