package ganso_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/chazu/ganso"
)

func drainCh(cancel context.CancelFunc, ch <-chan ganso.Event) {
	cancel()
	for range ch {
	}
	// Allow subscribe goroutine to fully exit after closing channel.
	time.Sleep(20 * time.Millisecond)
}

func TestStreamPublishAndRead(t *testing.T) {
	db := openTestDB(t)
	s := db.Stream("events")

	off1, err := s.Publish(map[string]string{"type": "created"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	off2, err := s.Publish(map[string]string{"type": "updated"}, ganso.WithKey("order-1"))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if off2 <= off1 {
		t.Errorf("offsets not monotonic: %d <= %d", off2, off1)
	}

	rows, err := db.Query(context.Background(),
		`SELECT "offset", topic, key, payload FROM _ganso_stream WHERE topic = :t ORDER BY "offset"`,
		map[string]any{":t": "events"},
	)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[1]["key"] != "order-1" {
		t.Errorf("key = %v, want order-1", rows[1]["key"])
	}
}

func TestStreamPublishTx(t *testing.T) {
	db := openTestDB(t)
	s := db.Stream("events")

	var offset int64
	err := db.WithTx(func(tx *ganso.Tx) error {
		var txErr error
		offset, txErr = s.PublishTx(tx, map[string]string{"tx": "yes"})
		return txErr
	})
	if err != nil {
		t.Fatalf("WithTx: %v", err)
	}
	if offset <= 0 {
		t.Errorf("expected positive offset, got %d", offset)
	}
}

func TestStreamPublishTxRollback(t *testing.T) {
	db := openTestDB(t)
	s := db.Stream("events")

	_ = db.WithTx(func(tx *ganso.Tx) error {
		_, _ = s.PublishTx(tx, map[string]string{"should": "vanish"})
		return context.Canceled
	})

	rows, err := db.Query(context.Background(),
		`SELECT COUNT(*) as cnt FROM _ganso_stream WHERE topic = :t`,
		map[string]any{":t": "events"},
	)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if rows[0]["cnt"].(int64) != 0 {
		t.Errorf("expected 0 events after rollback, got %v", rows[0]["cnt"])
	}
}

func TestStreamSaveGetOffset(t *testing.T) {
	db := openTestDB(t)
	s := db.Stream("events")

	ctx := context.Background()

	off, err := s.GetOffset(ctx, "consumer-1")
	if err != nil {
		t.Fatalf("GetOffset: %v", err)
	}
	if off != 0 {
		t.Errorf("initial offset = %d, want 0", off)
	}

	if err := s.SaveOffset("consumer-1", 42); err != nil {
		t.Fatalf("SaveOffset: %v", err)
	}

	off, err = s.GetOffset(ctx, "consumer-1")
	if err != nil {
		t.Fatalf("GetOffset: %v", err)
	}
	if off != 42 {
		t.Errorf("offset = %d, want 42", off)
	}

	if err := s.SaveOffset("consumer-1", 100); err != nil {
		t.Fatalf("SaveOffset: %v", err)
	}
	off, err = s.GetOffset(ctx, "consumer-1")
	if err != nil {
		t.Fatalf("GetOffset: %v", err)
	}
	if off != 100 {
		t.Errorf("offset = %d, want 100", off)
	}
}

func TestStreamSubscribeExisting(t *testing.T) {
	db := openTestDB(t)
	s := db.Stream("events")

	for i := 0; i < 5; i++ {
		if _, err := s.Publish(map[string]int{"i": i}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	ch := s.Subscribe(ctx)

	var received []ganso.Event
	for i := 0; i < 5; i++ {
		ev := <-ch
		received = append(received, ev)
	}

	drainCh(cancel, ch)

	if len(received) != 5 {
		t.Fatalf("expected 5 events, got %d", len(received))
	}
	if received[0].Offset >= received[4].Offset {
		t.Error("offsets not ascending")
	}
}

func TestStreamSubscribeNewEvents(t *testing.T) {
	db := openTestDB(t)
	s := db.Stream("events")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

	ch := s.Subscribe(ctx)

	publishDone := make(chan struct{})
	go func() {
		defer close(publishDone)
		time.Sleep(50 * time.Millisecond)
		s.Publish(map[string]string{"after": "subscribe"})
	}()

	select {
	case ev := <-ch:
		var m map[string]string
		if err := json.Unmarshal(ev.Payload, &m); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if m["after"] != "subscribe" {
			t.Errorf("payload = %v, want after=subscribe", m)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for event")
	}

	<-publishDone
	drainCh(cancel, ch)
}

func TestStreamSubscribeWithConsumer(t *testing.T) {
	db := openTestDB(t)
	s := db.Stream("events")

	for i := 0; i < 3; i++ {
		if _, err := s.Publish(map[string]int{"i": i}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	ch1 := s.Subscribe(ctx1, ganso.Consumer("my-consumer"), ganso.SaveEveryN(1))
	var lastOffset int64
	for i := 0; i < 3; i++ {
		ev := <-ch1
		lastOffset = ev.Offset
	}
	drainCh(cancel1, ch1)

	saved, err := s.GetOffset(context.Background(), "my-consumer")
	if err != nil {
		t.Fatalf("GetOffset: %v", err)
	}
	if saved != lastOffset {
		t.Errorf("saved offset = %d, want %d", saved, lastOffset)
	}

	for i := 3; i < 5; i++ {
		if _, err := s.Publish(map[string]int{"i": i}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	ch2 := s.Subscribe(ctx2, ganso.Consumer("my-consumer"))
	var got []int64
	for i := 0; i < 2; i++ {
		ev := <-ch2
		got = append(got, ev.Offset)
	}
	drainCh(cancel2, ch2)

	if len(got) != 2 {
		t.Fatalf("expected 2 new events, got %d", len(got))
	}
	if got[0] <= lastOffset {
		t.Errorf("first new event offset %d should be > saved offset %d", got[0], lastOffset)
	}
}

func TestStreamSubscribeFromOffset(t *testing.T) {
	db := openTestDB(t)
	s := db.Stream("events")

	var offsets []int64
	for i := 0; i < 5; i++ {
		off, err := s.Publish(map[string]int{"i": i})
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}
		offsets = append(offsets, off)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ch := s.Subscribe(ctx, ganso.FromOffset(offsets[2]))
	var got []int64
	for i := 0; i < 2; i++ {
		ev := <-ch
		got = append(got, ev.Offset)
	}
	drainCh(cancel, ch)

	if len(got) != 2 {
		t.Fatalf("expected 2 events, got %d", len(got))
	}
	if got[0] != offsets[3] {
		t.Errorf("first event offset = %d, want %d", got[0], offsets[3])
	}
}

func TestStreamIsolation(t *testing.T) {
	db := openTestDB(t)
	s1 := db.Stream("topic-a")
	s2 := db.Stream("topic-b")

	s1.Publish(map[string]string{"from": "a"})
	s2.Publish(map[string]string{"from": "b"})

	ctx, cancel := context.WithCancel(context.Background())
	ch := s1.Subscribe(ctx)

	ev := <-ch
	var m map[string]string
	json.Unmarshal(ev.Payload, &m)
	if m["from"] != "a" {
		t.Errorf("stream isolation broken: got from=%s, want a", m["from"])
	}

	drainCh(cancel, ch)
}
