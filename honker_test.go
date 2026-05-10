package honker_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/chazu/honker"
)

func TestOpenClose(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := honker.Open(dbPath, honker.WithMaxReaders(2))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Verify file exists
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("db file not created: %v", err)
	}
}

func TestWithTxAndQuery(t *testing.T) {
	dir := t.TempDir()
	db, err := honker.Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Write via WithTx
	err = db.WithTx(func(tx *honker.Tx) error {
		return tx.Notify("test-channel", "hello")
	})
	if err != nil {
		t.Fatalf("WithTx: %v", err)
	}

	// Read via Query
	rows, err := db.Query(context.Background(),
		`SELECT channel, payload FROM _honker_notifications WHERE channel = :ch`,
		map[string]any{":ch": "test-channel"},
	)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0]["channel"] != "test-channel" {
		t.Errorf("channel = %v, want test-channel", rows[0]["channel"])
	}
	if rows[0]["payload"] != "hello" {
		t.Errorf("payload = %v, want hello", rows[0]["payload"])
	}
}

func TestQueueMemoization(t *testing.T) {
	dir := t.TempDir()
	db, err := honker.Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	q1 := db.Queue("tasks")
	q2 := db.Queue("tasks")
	if q1 != q2 {
		t.Error("Queue should return the same instance for the same name")
	}
}

func TestStreamMemoization(t *testing.T) {
	dir := t.TempDir()
	db, err := honker.Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	s1 := db.Stream("events")
	s2 := db.Stream("events")
	if s1 != s2 {
		t.Error("Stream should return the same instance for the same name")
	}
}

func TestDoubleClose(t *testing.T) {
	dir := t.TempDir()
	db, err := honker.Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := db.Close(); err != honker.ErrClosed {
		t.Errorf("second Close = %v, want ErrClosed", err)
	}
}

func TestWithTxAfterClose(t *testing.T) {
	dir := t.TempDir()
	db, err := honker.Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	db.Close()

	err = db.WithTx(func(tx *honker.Tx) error { return nil })
	if err != honker.ErrClosed {
		t.Errorf("WithTx after close = %v, want ErrClosed", err)
	}
}

func TestSchemaTablesExist(t *testing.T) {
	dir := t.TempDir()
	db, err := honker.Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	tables := []string{
		"_honker_notifications",
		"_honker_live",
		"_honker_dead",
		"_honker_locks",
		"_honker_rate_limits",
		"_honker_scheduler_tasks",
		"_honker_results",
		"_honker_stream",
		"_honker_stream_consumers",
	}

	for _, table := range tables {
		rows, err := db.Query(context.Background(),
			`SELECT name FROM sqlite_master WHERE type='table' AND name=:name`,
			map[string]any{":name": table},
		)
		if err != nil {
			t.Errorf("query for table %s: %v", table, err)
			continue
		}
		if len(rows) == 0 {
			t.Errorf("table %s not found in schema", table)
		}
	}

	// Also check the view
	rows, err := db.Query(context.Background(),
		`SELECT name FROM sqlite_master WHERE type='view' AND name='_honker_jobs'`,
		nil,
	)
	if err != nil {
		t.Fatalf("query for view: %v", err)
	}
	if len(rows) == 0 {
		t.Error("view _honker_jobs not found in schema")
	}
}
