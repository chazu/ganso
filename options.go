package ganso

import "time"

// ---------------------------------------------------------------------------
// OpenOption
// ---------------------------------------------------------------------------

type openConfig struct {
	maxReaders   int
	pollInterval time.Duration
}

func defaultOpenConfig() openConfig {
	return openConfig{
		// A single-writer coordination store (e.g. one dispatcher goroutine +
		// a few dashboard readers) does not need 10 reader connections, each of
		// which carries its own page cache. 3 keeps the page-cache ceiling and
		// open-handle count small; callers who need more pass WithMaxReaders.
		maxReaders: 3,
		// 1ms was a busy-poll: a watcher with no consumers issued ~1000
		// PRAGMA data_version reads/sec. 250ms is ample for change notification
		// and near-eliminates the idle wakeup/CPU cost.
		pollInterval: 250 * time.Millisecond,
	}
}

// OpenOption configures how a Database is opened.
type OpenOption func(*openConfig)

// WithMaxReaders sets the number of reader connections in the pool.
func WithMaxReaders(n int) OpenOption {
	return func(c *openConfig) { c.maxReaders = n }
}

// WithPollInterval sets the default polling interval used by listeners.
func WithPollInterval(d time.Duration) OpenOption {
	return func(c *openConfig) { c.pollInterval = d }
}

// ---------------------------------------------------------------------------
// QueueOption
// ---------------------------------------------------------------------------

type queueConfig struct {
	visibilityTimeout time.Duration
	maxAttempts       int
}

func defaultQueueConfig() queueConfig {
	return queueConfig{
		visibilityTimeout: 30 * time.Second,
		maxAttempts:       3,
	}
}

// QueueOption configures a Queue.
type QueueOption func(*queueConfig)

// WithVisibilityTimeout sets how long a claimed message stays invisible.
func WithVisibilityTimeout(d time.Duration) QueueOption {
	return func(c *queueConfig) { c.visibilityTimeout = d }
}

// WithMaxAttempts sets the maximum delivery attempts before dead-lettering.
func WithMaxAttempts(n int) QueueOption {
	return func(c *queueConfig) { c.maxAttempts = n }
}

// ---------------------------------------------------------------------------
// EnqueueOption
// ---------------------------------------------------------------------------

type enqueueConfig struct {
	runAt    *time.Time
	delay    time.Duration
	priority int
	expires  time.Duration
}

// EnqueueOption configures an individual enqueue call.
type EnqueueOption func(*enqueueConfig)

// RunAt schedules the message to become visible at a specific time.
func RunAt(t time.Time) EnqueueOption {
	return func(c *enqueueConfig) { c.runAt = &t }
}

// Delay schedules the message to become visible after a duration from now.
func Delay(d time.Duration) EnqueueOption {
	return func(c *enqueueConfig) { c.delay = d }
}

// WithPriority sets the message priority (higher = dequeued first).
func WithPriority(p int) EnqueueOption {
	return func(c *enqueueConfig) { c.priority = p }
}

// Expires sets a TTL after which the message is discarded.
func Expires(d time.Duration) EnqueueOption {
	return func(c *enqueueConfig) { c.expires = d }
}

// ---------------------------------------------------------------------------
// PublishOption
// ---------------------------------------------------------------------------

type publishConfig struct {
	key string
}

// PublishOption configures a stream publish call.
type PublishOption func(*publishConfig)

// WithKey sets a partitioning key on the event.
func WithKey(key string) PublishOption {
	return func(c *publishConfig) { c.key = key }
}

// ---------------------------------------------------------------------------
// SubscribeOption
// ---------------------------------------------------------------------------

type subscribeConfig struct {
	consumer         string
	fromOffset       int64
	saveEveryN       int
	saveEveryDur     time.Duration
}

func defaultSubscribeConfig() subscribeConfig {
	return subscribeConfig{
		saveEveryN:   100,
		saveEveryDur: 5 * time.Second,
	}
}

// SubscribeOption configures a stream subscription.
type SubscribeOption func(*subscribeConfig)

// Consumer sets a durable consumer name for offset tracking.
func Consumer(name string) SubscribeOption {
	return func(c *subscribeConfig) { c.consumer = name }
}

// FromOffset sets the starting offset for a new subscription.
func FromOffset(offset int64) SubscribeOption {
	return func(c *subscribeConfig) { c.fromOffset = offset }
}

// SaveEveryN commits the consumer offset every n messages.
func SaveEveryN(n int) SubscribeOption {
	return func(c *subscribeConfig) { c.saveEveryN = n }
}

// SaveEveryDuration commits the consumer offset on a time interval.
func SaveEveryDuration(d time.Duration) SubscribeOption {
	return func(c *subscribeConfig) { c.saveEveryDur = d }
}

// ---------------------------------------------------------------------------
// ListenOption
// ---------------------------------------------------------------------------

type listenConfig struct {
	fallbackPoll time.Duration
}

func defaultListenConfig() listenConfig {
	return listenConfig{
		fallbackPoll: time.Second,
	}
}

// ListenOption configures notification listening.
type ListenOption func(*listenConfig)

// FallbackPoll sets the polling interval when update hooks are unavailable.
func FallbackPoll(d time.Duration) ListenOption {
	return func(c *listenConfig) { c.fallbackPoll = d }
}

// ---------------------------------------------------------------------------
// LockOption
// ---------------------------------------------------------------------------

type lockConfig struct {
	ttl   time.Duration
	owner string
}

func defaultLockConfig() lockConfig {
	return lockConfig{
		ttl: 30 * time.Second,
	}
}

// LockOption configures a distributed lock.
type LockOption func(*lockConfig)

// WithTTL sets the lock's time-to-live.
func WithTTL(d time.Duration) LockOption {
	return func(c *lockConfig) { c.ttl = d }
}

// WithOwner sets the lock owner identifier.
func WithOwner(owner string) LockOption {
	return func(c *lockConfig) { c.owner = owner }
}

// ---------------------------------------------------------------------------
// TaskOption
// ---------------------------------------------------------------------------

type taskConfig struct {
	retries      int
	retryDelay   time.Duration
	timeout      time.Duration
	priority     int
	storeResult  bool
	resultTTL    time.Duration
}

func defaultTaskConfig() taskConfig {
	return taskConfig{
		retries:    3,
		retryDelay: 5 * time.Second,
		timeout:    30 * time.Second,
		resultTTL:  24 * time.Hour,
	}
}

// TaskOption configures a task registration.
type TaskOption func(*taskConfig)

// WithRetries sets the maximum number of retry attempts for a task.
func WithRetries(n int) TaskOption {
	return func(c *taskConfig) { c.retries = n }
}

// WithRetryDelay sets the delay between retry attempts.
func WithRetryDelay(d time.Duration) TaskOption {
	return func(c *taskConfig) { c.retryDelay = d }
}

// WithTimeout sets the execution timeout per attempt.
func WithTimeout(d time.Duration) TaskOption {
	return func(c *taskConfig) { c.timeout = d }
}

// WithTaskPriority sets the default priority for submitted tasks.
func WithTaskPriority(p int) TaskOption {
	return func(c *taskConfig) { c.priority = p }
}

// WithStoreResult enables or disables result storage for completed tasks.
func WithStoreResult(b bool) TaskOption {
	return func(c *taskConfig) { c.storeResult = b }
}

// WithResultTTL sets how long results are kept before expiry.
func WithResultTTL(d time.Duration) TaskOption {
	return func(c *taskConfig) { c.resultTTL = d }
}

// ---------------------------------------------------------------------------
// SchedulerOption
// ---------------------------------------------------------------------------

type schedulerConfig struct {
	lockName string
}

func defaultSchedulerConfig() schedulerConfig {
	return schedulerConfig{
		lockName: "_ganso_scheduler_lock",
	}
}

// SchedulerOption configures a cron scheduler.
type SchedulerOption func(*schedulerConfig)

// WithLockName sets the distributed lock name used by the scheduler.
func WithLockName(name string) SchedulerOption {
	return func(c *schedulerConfig) { c.lockName = name }
}

// ---------------------------------------------------------------------------
// ScheduleTaskOption
// ---------------------------------------------------------------------------

type scheduleTaskConfig struct {
	priority int
	expires  time.Duration
	payload  any
}

func defaultScheduleTaskConfig() scheduleTaskConfig {
	return scheduleTaskConfig{}
}

// ScheduleTaskOption configures an individual scheduled task registration.
type ScheduleTaskOption func(*scheduleTaskConfig)

// WithSchedulePriority sets the priority for a scheduled task's enqueued jobs.
func WithSchedulePriority(p int) ScheduleTaskOption {
	return func(c *scheduleTaskConfig) { c.priority = p }
}

// WithScheduleExpires sets the TTL for jobs created by a scheduled task.
func WithScheduleExpires(d time.Duration) ScheduleTaskOption {
	return func(c *scheduleTaskConfig) { c.expires = d }
}

// WithSchedulePayload sets the payload for a scheduled task's enqueued jobs.
func WithSchedulePayload(v any) ScheduleTaskOption {
	return func(c *scheduleTaskConfig) { c.payload = v }
}

// ---------------------------------------------------------------------------
// OutboxOption
// ---------------------------------------------------------------------------

type outboxConfig struct {
	maxAttempts       int
	baseBackoff       time.Duration
	visibilityTimeout time.Duration
}

func defaultOutboxConfig() outboxConfig {
	return outboxConfig{
		maxAttempts:       5,
		baseBackoff:       time.Second,
		visibilityTimeout: 30 * time.Second,
	}
}

// OutboxOption configures an Outbox.
type OutboxOption func(*outboxConfig)

// WithOutboxMaxAttempts sets the maximum delivery attempts for outbox messages.
func WithOutboxMaxAttempts(n int) OutboxOption {
	return func(c *outboxConfig) { c.maxAttempts = n }
}

// WithBaseBackoff sets the base delay for exponential backoff on retries.
func WithBaseBackoff(d time.Duration) OutboxOption {
	return func(c *outboxConfig) { c.baseBackoff = d }
}

// WithOutboxVisibilityTimeout sets how long a claimed outbox message stays invisible.
func WithOutboxVisibilityTimeout(d time.Duration) OutboxOption {
	return func(c *outboxConfig) { c.visibilityTimeout = d }
}

// ---------------------------------------------------------------------------
// ClaimOption
// ---------------------------------------------------------------------------

type claimConfig struct {
	idlePoll time.Duration
}

func defaultClaimConfig() claimConfig {
	return claimConfig{
		idlePoll: time.Second,
	}
}

// ClaimOption configures a claim/dequeue operation.
type ClaimOption func(*claimConfig)

// WithIdlePoll sets the polling interval when no messages are available.
func WithIdlePoll(d time.Duration) ClaimOption {
	return func(c *claimConfig) { c.idlePoll = d }
}

// ---------------------------------------------------------------------------
// PruneOption
// ---------------------------------------------------------------------------

type pruneConfig struct {
	olderThan time.Duration
	maxKeep   int
}

func defaultPruneConfig() pruneConfig {
	return pruneConfig{
		olderThan: 7 * 24 * time.Hour,
		maxKeep:   1000,
	}
}

// PruneOption configures a prune operation.
type PruneOption func(*pruneConfig)

// OlderThan prunes records older than the given duration.
func OlderThan(d time.Duration) PruneOption {
	return func(c *pruneConfig) { c.olderThan = d }
}

// MaxKeep limits how many records to retain after pruning.
func MaxKeep(n int) PruneOption {
	return func(c *pruneConfig) { c.maxKeep = n }
}
