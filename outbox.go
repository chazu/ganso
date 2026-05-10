package ganso

import (
	"context"
	"encoding/json"
	"math"
	"time"
)

// DeliveryFunc is called by the Outbox worker to deliver each message.
type DeliveryFunc func(ctx context.Context, payload json.RawMessage) error

// Outbox implements the transactional outbox pattern backed by SQLite.
// Enqueue side effects in the same transaction as business writes; a
// background worker drives delivery.
type Outbox struct {
	db                *Database
	Name              string
	maxAttempts       int
	baseBackoff       time.Duration
	visibilityTimeout time.Duration
	queue             *Queue // lazily initialized backing queue
}

// initQueue lazily creates the backing queue named "_outbox:<name>".
func (o *Outbox) initQueue() *Queue {
	if o.queue == nil {
		o.queue = o.db.Queue("_outbox:"+o.Name,
			WithVisibilityTimeout(o.visibilityTimeout),
			WithMaxAttempts(o.maxAttempts),
		)
	}
	return o.queue
}

// Send enqueues a payload inside the caller's transaction. The payload is
// committed atomically with whatever else the transaction contains. This is
// the core outbox pattern: business write + side-effect enqueue in one tx.
func (o *Outbox) Send(tx *Tx, payload any, opts ...EnqueueOption) (string, error) {
	return o.initQueue().EnqueueTx(tx, payload, opts...)
}

// Enqueue enqueues a payload outside of a user transaction (convenience).
func (o *Outbox) Enqueue(payload any, opts ...EnqueueOption) (string, error) {
	return o.initQueue().Enqueue(payload, opts...)
}

// Run starts the outbox delivery worker. It blocks until ctx is cancelled.
// For each claimed job, deliveryFn is called. On success the job is ack'd;
// on failure it is retried with exponential backoff up to maxAttempts, after
// which it moves to the dead-letter table.
func (o *Outbox) Run(ctx context.Context, deliveryFn DeliveryFunc) error {
	q := o.initQueue()
	workerID := "outbox-" + o.Name + "-worker"
	ch := q.Claims(ctx, workerID)

	for job := range ch {
		err := deliveryFn(ctx, job.Payload)
		if err != nil {
			// Exponential backoff: baseBackoff * 2^(attempts-1)
			attempts := job.Attempts
			if attempts < 1 {
				attempts = 1
			}
			exp := math.Pow(2, float64(attempts-1))
			delaySec := int(o.baseBackoff.Seconds() * exp)
			if delaySec < 1 {
				delaySec = 1
			}
			_ = job.Retry(delaySec, err.Error())
			continue
		}
		_ = job.Ack()
	}
	return nil
}
