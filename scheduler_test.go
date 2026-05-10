package honker_test

import (
	"context"
	"testing"
	"time"

	"github.com/chazu/honker"
)

func TestSchedulerAddAndList(t *testing.T) {
	db := openTestDB(t)
	sched := db.Scheduler()

	s, err := honker.ParseSchedule("*/5 * * * *")
	if err != nil {
		t.Fatal(err)
	}

	err = sched.Add("backup", "jobs", s, honker.WithSchedulePayload(map[string]string{"type": "backup"}))
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	infos, err := sched.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(infos) != 1 {
		t.Fatalf("expected 1 task, got %d", len(infos))
	}
	if infos[0].Name != "backup" {
		t.Errorf("name = %q, want backup", infos[0].Name)
	}
	if infos[0].Queue != "jobs" {
		t.Errorf("queue = %q, want jobs", infos[0].Queue)
	}
	if !infos[0].Enabled {
		t.Error("expected enabled")
	}
}

func TestSchedulerRemove(t *testing.T) {
	db := openTestDB(t)
	sched := db.Scheduler()

	s, _ := honker.ParseSchedule("0 * * * *")
	sched.Add("temp", "q", s)

	removed, err := sched.Remove("temp")
	if err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if !removed {
		t.Error("expected removed=true")
	}

	removed, err = sched.Remove("nonexistent")
	if err != nil {
		t.Fatalf("Remove nonexistent: %v", err)
	}
	if removed {
		t.Error("expected removed=false for nonexistent")
	}
}

func TestSchedulerPauseResume(t *testing.T) {
	db := openTestDB(t)
	sched := db.Scheduler()

	s, _ := honker.ParseSchedule("*/10 * * * *")
	sched.Add("periodic", "q", s)

	paused, err := sched.Pause("periodic")
	if err != nil {
		t.Fatalf("Pause: %v", err)
	}
	if !paused {
		t.Error("expected paused=true")
	}

	infos, _ := sched.List()
	if infos[0].Enabled {
		t.Error("expected disabled after pause")
	}

	// Pause again should be no-op.
	paused, _ = sched.Pause("periodic")
	if paused {
		t.Error("double pause should return false")
	}

	resumed, err := sched.Resume("periodic")
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if !resumed {
		t.Error("expected resumed=true")
	}

	infos, _ = sched.List()
	if !infos[0].Enabled {
		t.Error("expected enabled after resume")
	}
}

func TestSchedulerRunTickFires(t *testing.T) {
	db := openTestDB(t)
	sched := db.Scheduler()

	// Schedule that fires every second.
	s := honker.Every(1 * time.Second)
	err := sched.Add("fast", "tick-queue", s,
		honker.WithSchedulePayload(map[string]string{"tick": "yes"}),
	)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Run scheduler in background.
	errCh := make(chan error, 1)
	go func() {
		errCh <- sched.Run(ctx)
	}()

	// Wait for a job to appear in the queue.
	q := db.Queue("tick-queue")
	var job *honker.Job
	deadline := time.After(4 * time.Second)
	for job == nil {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for scheduled job")
		default:
			job, _ = q.ClaimOne("test-worker")
			if job == nil {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}

	if job == nil {
		t.Fatal("no job claimed")
	}
	job.Ack()
	cancel()

	if err := <-errCh; err != nil {
		t.Errorf("Run error: %v", err)
	}
}

func TestSchedulerAddReplace(t *testing.T) {
	db := openTestDB(t)
	sched := db.Scheduler()

	s1, _ := honker.ParseSchedule("0 * * * *")
	s2, _ := honker.ParseSchedule("*/15 * * * *")

	sched.Add("task", "q1", s1)
	sched.Add("task", "q2", s2)

	infos, _ := sched.List()
	if len(infos) != 1 {
		t.Fatalf("expected 1 task after replace, got %d", len(infos))
	}
	if infos[0].Queue != "q2" {
		t.Errorf("queue = %q, want q2", infos[0].Queue)
	}
	if infos[0].CronExpr != "*/15 * * * *" {
		t.Errorf("cron = %q, want */15 * * * *", infos[0].CronExpr)
	}
}
