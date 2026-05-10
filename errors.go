package ganso

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrLockHeld    = errors.New("ganso: lock is already held")
	ErrClosed      = errors.New("ganso: database is closed")
	ErrUnknownTask = errors.New("ganso: unknown task")
)

// Retryable wraps an error to request retry with a specific delay.
// Task handlers return this to tell the worker to retry with a specific delay
// instead of using the default retry delay.
type Retryable struct {
	Err   error
	Delay time.Duration
}

func (r *Retryable) Error() string {
	return fmt.Sprintf("retryable (delay %s): %v", r.Delay, r.Err)
}

func (r *Retryable) Unwrap() error {
	return r.Err
}
