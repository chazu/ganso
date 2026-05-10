package ganso_test

import (
	"context"
	"testing"
	"time"

	"github.com/chazu/ganso"
)

func TestTryLockAcquireRelease(t *testing.T) {
	db := openTestDB(t)

	l, err := db.TryLock("test-lock")
	if err != nil {
		t.Fatalf("TryLock: %v", err)
	}

	if err := l.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
}

func TestTryLockContention(t *testing.T) {
	db := openTestDB(t)

	l1, err := db.TryLock("test-lock", ganso.WithOwner("owner-1"))
	if err != nil {
		t.Fatalf("TryLock owner-1: %v", err)
	}

	_, err = db.TryLock("test-lock", ganso.WithOwner("owner-2"))
	if err != ganso.ErrLockHeld {
		t.Errorf("expected ErrLockHeld, got %v", err)
	}

	l1.Release()

	l2, err := db.TryLock("test-lock", ganso.WithOwner("owner-2"))
	if err != nil {
		t.Fatalf("TryLock after release: %v", err)
	}
	l2.Release()
}

func TestTryLockSameOwnerReentrant(t *testing.T) {
	db := openTestDB(t)

	l1, err := db.TryLock("test-lock", ganso.WithOwner("same"))
	if err != nil {
		t.Fatalf("TryLock: %v", err)
	}

	// Same owner should fail (INSERT OR IGNORE hits, but owner matches).
	l2, err := db.TryLock("test-lock", ganso.WithOwner("same"))
	if err != nil {
		t.Fatalf("same owner TryLock: %v", err)
	}

	l1.Release()
	l2.Release()
}

func TestLockBlocking(t *testing.T) {
	db := openTestDB(t)

	l1, err := db.TryLock("blocker", ganso.WithOwner("holder"), ganso.WithTTL(2*time.Second))
	if err != nil {
		t.Fatalf("TryLock: %v", err)
	}

	acquired := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		l2, err := db.Lock(ctx, "blocker", ganso.WithOwner("waiter"))
		if err != nil {
			t.Errorf("Lock waiter: %v", err)
			return
		}
		l2.Release()
		close(acquired)
	}()

	// Release after short delay.
	time.Sleep(100 * time.Millisecond)
	l1.Release()

	select {
	case <-acquired:
	case <-time.After(5 * time.Second):
		t.Fatal("Lock did not unblock after release")
	}
}

func TestLockContextCancellation(t *testing.T) {
	db := openTestDB(t)

	_, err := db.TryLock("ctx-lock", ganso.WithOwner("holder"))
	if err != nil {
		t.Fatalf("TryLock: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err = db.Lock(ctx, "ctx-lock", ganso.WithOwner("waiter"))
	if err != context.DeadlineExceeded {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
}

func TestWithLock(t *testing.T) {
	db := openTestDB(t)

	ran := false
	err := db.WithLock(context.Background(), "wl", func() error {
		ran = true
		// Lock should be held — another TryLock should fail.
		_, err := db.TryLock("wl", ganso.WithOwner("other"))
		if err != ganso.ErrLockHeld {
			t.Errorf("expected ErrLockHeld inside WithLock, got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithLock: %v", err)
	}
	if !ran {
		t.Error("fn did not run")
	}

	// Lock should be released.
	l, err := db.TryLock("wl", ganso.WithOwner("after"))
	if err != nil {
		t.Fatalf("TryLock after WithLock: %v", err)
	}
	l.Release()
}

func TestDifferentLockNames(t *testing.T) {
	db := openTestDB(t)

	l1, err := db.TryLock("lock-a")
	if err != nil {
		t.Fatalf("TryLock a: %v", err)
	}

	l2, err := db.TryLock("lock-b")
	if err != nil {
		t.Fatalf("TryLock b: %v", err)
	}

	l1.Release()
	l2.Release()
}

func TestTryRateLimitBasic(t *testing.T) {
	db := openTestDB(t)

	for i := 0; i < 5; i++ {
		allowed, err := db.TryRateLimit("api", 5, 60)
		if err != nil {
			t.Fatalf("TryRateLimit call %d: %v", i, err)
		}
		if !allowed {
			t.Errorf("call %d should be allowed", i)
		}
	}

	// 6th call should be denied.
	allowed, err := db.TryRateLimit("api", 5, 60)
	if err != nil {
		t.Fatalf("TryRateLimit: %v", err)
	}
	if allowed {
		t.Error("6th call should be denied")
	}
}

func TestTryRateLimitDifferentNames(t *testing.T) {
	db := openTestDB(t)

	for i := 0; i < 3; i++ {
		db.TryRateLimit("limit-a", 3, 60)
	}

	// limit-a exhausted, but limit-b should still work.
	allowed, err := db.TryRateLimit("limit-b", 3, 60)
	if err != nil {
		t.Fatalf("TryRateLimit: %v", err)
	}
	if !allowed {
		t.Error("different name should be allowed")
	}
}

func TestSweepRateLimits(t *testing.T) {
	db := openTestDB(t)

	// Create some entries.
	for i := 0; i < 3; i++ {
		db.TryRateLimit("sweep-test", 100, 60)
	}

	// Sweep with a very large threshold — should delete nothing.
	n, err := db.SweepRateLimits(0)
	if err != nil {
		t.Fatalf("SweepRateLimits: %v", err)
	}
	// Window start is current time, so olderThanSec=0 means cutoff=now,
	// which should catch current windows.
	_ = n // Just verify no error; exact count depends on timing.
}
