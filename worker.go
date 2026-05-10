package ganso

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"
)

// TaskFunc is the signature for task handler functions.
type TaskFunc func(ctx context.Context, payload json.RawMessage) (any, error)

// TaskSpec holds metadata for a registered task.
type TaskSpec struct {
	Name        string
	Fn          TaskFunc
	QueueName   string
	Retries     int
	RetryDelay  time.Duration
	Timeout     time.Duration
	Priority    int
	StoreResult bool
	ResultTTL   time.Duration
}

// TaskRegistry is a map of task name to TaskSpec.
type TaskRegistry struct {
	mu    sync.RWMutex
	tasks map[string]*TaskSpec
}

// defaultRegistry is the package-level task registry.
var defaultRegistry = NewRegistry()

// NewRegistry creates a new empty TaskRegistry.
func NewRegistry() *TaskRegistry {
	return &TaskRegistry{tasks: make(map[string]*TaskSpec)}
}

// Register adds a task spec to the registry. Returns an error if the same name
// is already registered with a different function.
func (r *TaskRegistry) Register(spec TaskSpec) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.tasks[spec.Name]; ok {
		// Allow re-registration of the same function (idempotent).
		if fmt.Sprintf("%p", existing.Fn) != fmt.Sprintf("%p", spec.Fn) {
			return fmt.Errorf("ganso: duplicate task name %q", spec.Name)
		}
	}
	r.tasks[spec.Name] = &spec
	return nil
}

// Get returns the TaskSpec for the given name, if registered.
func (r *TaskRegistry) Get(name string) (*TaskSpec, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.tasks[name]
	return s, ok
}

// Names returns all registered task names, sorted.
func (r *TaskRegistry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.tasks))
	for n := range r.tasks {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// Queues returns unique queue names across all registered tasks, sorted.
func (r *TaskRegistry) Queues() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	set := make(map[string]struct{})
	for _, s := range r.tasks {
		set[s.QueueName] = struct{}{}
	}
	queues := make([]string, 0, len(set))
	for q := range set {
		queues = append(queues, q)
	}
	sort.Strings(queues)
	return queues
}

// TaskHandle is returned by Queue.Task. Call it to enqueue the task.
type TaskHandle struct {
	spec  *TaskSpec
	queue *Queue
}

// Call enqueues the task with the given payload and returns a TaskResult that
// can be used to wait for the result.
func (h *TaskHandle) Call(payload any) (*TaskResult, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("ganso: marshal task payload: %w", err)
	}

	envelope := taskEnvelope{
		GansoTask: &taskInner{
			Task: h.spec.Name,
			Args: []json.RawMessage{payloadBytes},
		},
	}

	jobID, err := h.queue.Enqueue(envelope, WithPriority(h.spec.Priority))
	if err != nil {
		return nil, err
	}
	return &TaskResult{jobID: jobID, queue: h.queue}, nil
}

// CallLocal invokes the task function directly in the caller's process,
// bypassing the queue. Useful for testing.
func (h *TaskHandle) CallLocal(ctx context.Context, payload json.RawMessage) (any, error) {
	return h.spec.Fn(ctx, payload)
}

// TaskResult wraps a job ID for retrieving the result of a task execution.
type TaskResult struct {
	jobID string
	queue *Queue
}

// ID returns the underlying job ID.
func (r *TaskResult) ID() string { return r.jobID }

// Get blocks until the worker saves a result for this job, then returns it.
func (r *TaskResult) Get(ctx context.Context) (json.RawMessage, error) {
	return r.queue.WaitResult(ctx, r.jobID)
}

// Task registers a function as a task on this queue and returns a TaskHandle.
func (q *Queue) Task(name string, fn TaskFunc, opts ...TaskOption) *TaskHandle {
	cfg := defaultTaskConfig()
	for _, o := range opts {
		o(&cfg)
	}

	spec := TaskSpec{
		Name:        name,
		Fn:          fn,
		QueueName:   q.Name,
		Retries:     cfg.retries,
		RetryDelay:  cfg.retryDelay,
		Timeout:     cfg.timeout,
		Priority:    cfg.priority,
		StoreResult: cfg.storeResult,
		ResultTTL:   cfg.resultTTL,
	}
	if spec.Retries <= 0 {
		spec.Retries = q.maxAttempts
	}

	// Register in default registry; ignore duplicate errors for idempotency.
	_ = defaultRegistry.Register(spec)

	return &TaskHandle{spec: &spec, queue: q}
}

// PeriodicTask registers a task that also gets a scheduler entry.
func (q *Queue) PeriodicTask(name string, schedule Schedule, fn TaskFunc, opts ...TaskOption) *TaskHandle {
	handle := q.Task(name, fn, opts...)

	// Also register with the scheduler.
	sched := q.db.Scheduler()
	// Build the task envelope as the scheduler payload.
	envelope := taskEnvelope{
		GansoTask: &taskInner{
			Task: name,
			Args: []json.RawMessage{json.RawMessage("null")},
		},
	}
	schedOpts := []ScheduleTaskOption{
		WithSchedulePayload(envelope),
		WithSchedulePriority(handle.spec.Priority),
	}
	_ = sched.Add(name, q.Name, schedule, schedOpts...)

	return handle
}

// Task is a convenience shortcut on Database: db.Task(queue, name, fn).
func (db *Database) Task(queueName, name string, fn TaskFunc, opts ...TaskOption) *TaskHandle {
	return db.Queue(queueName).Task(name, fn, opts...)
}

// PeriodicTask is a convenience shortcut on Database.
func (db *Database) PeriodicTask(queueName, name string, schedule Schedule, fn TaskFunc, opts ...TaskOption) *TaskHandle {
	return db.Queue(queueName).PeriodicTask(name, schedule, fn, opts...)
}

// WorkerOptions configures RunWorkers.
type WorkerOptions struct {
	// Queue limits workers to a single queue. If empty, workers run on all
	// queues found in the registry.
	Queue string
	// Concurrency is the number of worker goroutines per queue. Defaults to
	// GOMAXPROCS.
	Concurrency int
	// Registry to dispatch tasks from. Defaults to the package-level registry.
	Registry *TaskRegistry
}

// RunWorkers runs worker goroutines that claim and execute tasks. It blocks
// until ctx is cancelled.
func (db *Database) RunWorkers(ctx context.Context, opts WorkerOptions) error {
	if opts.Concurrency <= 0 {
		opts.Concurrency = runtime.GOMAXPROCS(0)
	}
	reg := opts.Registry
	if reg == nil {
		reg = defaultRegistry
	}

	var queues []string
	if opts.Queue != "" {
		queues = []string{opts.Queue}
	} else {
		queues = reg.Queues()
	}
	if len(queues) == 0 {
		return fmt.Errorf("ganso: no queues to run workers on")
	}

	var wg sync.WaitGroup
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	pid := os.Getpid()

	for _, qName := range queues {
		q := db.Queue(qName)
		for i := 0; i < opts.Concurrency; i++ {
			workerID := fmt.Sprintf("ganso-worker-%d-%s-%d", pid, qName, i)
			wg.Add(1)
			go func(q *Queue, wid string) {
				defer wg.Done()
				runWorkerLoop(workerCtx, q, wid, reg)
			}(q, workerID)
		}
	}

	// Block until ctx is cancelled.
	<-ctx.Done()
	cancel() // signal workers to stop
	wg.Wait()
	return nil
}

// taskEnvelope is the JSON structure wrapping task invocations.
type taskEnvelope struct {
	GansoTask *taskInner `json:"__ganso_task__,omitempty"`
}

type taskInner struct {
	Task string            `json:"task"`
	Args []json.RawMessage `json:"args"`
}

// runWorkerLoop claims and dispatches tasks for a single worker goroutine.
func runWorkerLoop(ctx context.Context, q *Queue, workerID string, reg *TaskRegistry) {
	ch := q.Claims(ctx, workerID)
	for job := range ch {
		dispatchJob(ctx, job, reg)
	}
}

// dispatchJob parses the envelope, looks up the task, runs it, and handles
// success/failure.
func dispatchJob(ctx context.Context, job *Job, reg *TaskRegistry) {
	// Parse envelope.
	var env taskEnvelope
	if err := json.Unmarshal(job.Payload, &env); err != nil || env.GansoTask == nil {
		// Not a task envelope; dead-letter it.
		_ = job.Fail("non-task payload: cannot parse envelope")
		return
	}

	taskName := env.GansoTask.Task
	spec, ok := reg.Get(taskName)
	if !ok {
		_ = job.Fail(fmt.Sprintf("unknown task: %q (registered: %v)", taskName, reg.Names()))
		return
	}

	// Extract payload from args (first element, or null).
	var payload json.RawMessage
	if len(env.GansoTask.Args) > 0 {
		payload = env.GansoTask.Args[0]
	} else {
		payload = json.RawMessage("null")
	}

	// Run with optional timeout.
	var (
		result any
		runErr error
	)
	runCtx := ctx
	var runCancel context.CancelFunc
	if spec.Timeout > 0 {
		runCtx, runCancel = context.WithTimeout(ctx, spec.Timeout)
	} else {
		runCtx, runCancel = context.WithCancel(ctx)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		result, runErr = spec.Fn(runCtx, payload)
	}()

	select {
	case <-done:
		runCancel()
	case <-runCtx.Done():
		runCancel()
		// Wait briefly for the goroutine to finish.
		select {
		case <-done:
		case <-time.After(time.Second):
		}
		if runErr == nil {
			runErr = fmt.Errorf("timeout after %s", spec.Timeout)
		}
	}

	if runErr != nil {
		// Check for Retryable error to extract custom delay.
		var retryable *Retryable
		delaySec := int(spec.RetryDelay.Seconds())
		if errors.As(runErr, &retryable) {
			delaySec = int(retryable.Delay.Seconds())
		}
		_ = job.Retry(delaySec, runErr.Error())
		return
	}

	// Success: save result if configured, then ack.
	if spec.StoreResult && job.queue != nil {
		resultBytes, err := json.Marshal(result)
		if err == nil {
			_ = job.queue.SaveResult(job.ID, json.RawMessage(resultBytes), int(spec.ResultTTL.Seconds()))
		}
	}
	_ = job.Ack()
}
