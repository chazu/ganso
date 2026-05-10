package ganso_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/chazu/ganso"
)

func BenchmarkEnqueue(b *testing.B) {
	db := benchDB(b)
	q := db.Queue("bench")

	payload := map[string]string{"key": "value"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := q.Enqueue(payload)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEnqueueParallel(b *testing.B) {
	db := benchDB(b)
	q := db.Queue("bench-par")

	payload := map[string]string{"key": "value"}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := q.Enqueue(payload)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkClaimAck(b *testing.B) {
	db := benchDB(b)
	q := db.Queue("bench-claim")

	payload := map[string]string{"key": "value"}
	for i := 0; i < b.N; i++ {
		q.Enqueue(payload)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		job, err := q.ClaimOne("bench-worker")
		if err != nil {
			b.Fatal(err)
		}
		if job == nil {
			b.Fatal("nil job")
		}
		job.Ack()
	}
}

func BenchmarkStreamPublish(b *testing.B) {
	db := benchDB(b)
	s := db.Stream("bench-stream")

	payload := map[string]string{"event": "test"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := s.Publish(payload)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStreamSubscribeRead(b *testing.B) {
	db := benchDB(b)
	s := db.Stream("bench-pubsub")

	payload := map[string]string{"event": "test"}
	for i := 0; i < b.N; i++ {
		s.Publish(payload)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ch := s.Subscribe(ctx)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			b.Fatal("subscribe timeout")
		}
	}
	b.StopTimer()
	cancel()
	for range ch {
	}
}

func BenchmarkNotifyListen(b *testing.B) {
	db := benchDB(b)

	listener, err := db.Listen("bench-chan")
	if err != nil {
		b.Fatal(err)
	}
	defer listener.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := db.WithTx(func(tx *ganso.Tx) error {
			return tx.Notify("bench-chan", "ping")
		})
		if err != nil {
			b.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = listener.Next(ctx)
		cancel()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTryLockRelease(b *testing.B) {
	db := benchDB(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l, err := db.TryLock("bench-lock")
		if err != nil {
			b.Fatal(err)
		}
		l.Release()
	}
}

func BenchmarkTryRateLimit(b *testing.B) {
	db := benchDB(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.TryRateLimit("bench-rl", 1000000, 3600)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCronNextAfter(b *testing.B) {
	s, _ := ganso.ParseSchedule("*/5 * * * *")
	from := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.NextAfter(from)
	}
}

func BenchmarkQueryReader(b *testing.B) {
	db := benchDB(b)

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Query(ctx, "SELECT 1", nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSaveGetOffset(b *testing.B) {
	db := benchDB(b)
	s := db.Stream("bench-offset")

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.SaveOffset("consumer", int64(i))
		s.GetOffset(ctx, "consumer")
	}
}

func BenchmarkEnqueueClaimBatch(b *testing.B) {
	db := benchDB(b)
	q := db.Queue("bench-batch")

	payload := map[string]string{"key": "value"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 10; j++ {
			q.Enqueue(payload)
		}
		jobs, err := q.ClaimBatch("worker", 10)
		if err != nil {
			b.Fatal(err)
		}
		for _, j := range jobs {
			j.Ack()
		}
	}
}

func BenchmarkTaskDispatch(b *testing.B) {
	db := benchDB(b)
	q := db.Queue("bench-task")
	reg := ganso.NewRegistry()

	fn := func(_ context.Context, payload json.RawMessage) (any, error) {
		return nil, nil
	}
	reg.Register(ganso.TaskSpec{
		Name:      "noop",
		Fn:        fn,
		QueueName: "bench-task",
	})
	q.Task("noop", fn)

	ctx, cancel := context.WithCancel(context.Background())
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		db.RunWorkers(ctx, ganso.WorkerOptions{
			Queue:       "bench-task",
			Concurrency: 1,
			Registry:    reg,
		})
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Enqueue(map[string]any{
			"__ganso_task__": map[string]any{
				"task": "noop",
				"args": []any{nil},
			},
		})
	}
	// Wait for all jobs to be processed.
	time.Sleep(500 * time.Millisecond)
	b.StopTimer()

	cancel()
	<-workerDone
}

func benchDB(b *testing.B) *ganso.Database {
	b.Helper()
	db, err := ganso.Open(b.TempDir()+"/bench.db", ganso.WithMaxReaders(4))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	return db
}
