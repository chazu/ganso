package ganso_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chazu/ganso"
)

func TestOutboxSendInTx(t *testing.T) {
	db := openTestDB(t)
	ob := db.Outbox("emails", ganso.WithOutboxMaxAttempts(3))

	var delivered sync.Map

	err := db.WithTx(func(tx *ganso.Tx) error {
		_, err := ob.Send(tx, map[string]string{"to": "alice@example.com"})
		return err
	})
	if err != nil {
		t.Fatalf("WithTx+Send: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		ob.Run(ctx, func(_ context.Context, payload json.RawMessage) error {
			var m map[string]string
			json.Unmarshal(payload, &m)
			delivered.Store(m["to"], true)
			return nil
		})
	}()

	deadline := time.After(5 * time.Second)
	for {
		if _, ok := delivered.Load("alice@example.com"); ok {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for delivery")
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}
	cancel()
	<-runDone
}

func TestOutboxTxRollbackNoDelivery(t *testing.T) {
	db := openTestDB(t)
	ob := db.Outbox("rollback-test")

	_ = db.WithTx(func(tx *ganso.Tx) error {
		ob.Send(tx, map[string]string{"should": "vanish"})
		return fmt.Errorf("rollback")
	})

	var deliverCount atomic.Int32

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		ob.Run(ctx, func(_ context.Context, _ json.RawMessage) error {
			deliverCount.Add(1)
			return nil
		})
	}()

	<-ctx.Done()
	cancel()
	<-runDone

	if deliverCount.Load() != 0 {
		t.Errorf("expected 0 deliveries after rollback, got %d", deliverCount.Load())
	}
}

func TestOutboxRetryOnFailure(t *testing.T) {
	db := openTestDB(t)
	ob := db.Outbox("retry-ob",
		ganso.WithOutboxMaxAttempts(5),
		ganso.WithBaseBackoff(100*time.Millisecond),
	)

	ob.Enqueue(map[string]string{"msg": "retry-me"})

	var attempts atomic.Int32

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		ob.Run(ctx, func(_ context.Context, _ json.RawMessage) error {
			n := attempts.Add(1)
			if n < 3 {
				return fmt.Errorf("fail %d", n)
			}
			return nil
		})
	}()

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
	<-runDone
}
