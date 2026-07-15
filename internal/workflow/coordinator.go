package workflow

import (
	"context"
	"errors"
	"time"

	"github.com/khangpt2k6/AgentBus/internal/agentstream"
)

// Defaults for a freshly constructed coordinator.
const (
	DefaultMaxAttempts = 3
	DefaultLeaseTTL    = 30 * time.Second
	defaultSweepEvery  = 250 * time.Millisecond
	maxLeaseWait       = 30 * time.Second
)

// Publisher appends an envelope durably to the given topic: main WAL in
// single-node mode, shard WAL + quorum in cluster mode. grpcapi.Server
// satisfies this.
type Publisher interface {
	PublishEnvelope(ctx context.Context, topic string, env agentstream.Event) (offset int64, partition int32, err error)
}

// Metrics is the optional sink the coordinator publishes to. A nil sink is
// fine; methods are simply not called.
type Metrics interface {
	IncWorkflowEvent(evType string)
	SetWorkflowExecutions(status string, n int)
	IncWorkflowLeaseGranted()
	IncWorkflowLeaseExpired()
	IncWorkflowRetry()
	IncWorkflowCompletionRejected()
}

// ErrNotFound is returned for operations on unknown executions.
var ErrNotFound = errors.New("workflow execution not found")

// Coordinator owns the scheduling side of the runtime: it appends state
// transitions to the log (through Publisher), folds them into the Store,
// grants leases, and sweeps expired ones. Reads that arrive via the Poller
// fold into the same store; because the fold is idempotent, seeing our own
// events again is harmless.
type Coordinator struct {
	store   *Store
	pub     Publisher
	metrics Metrics

	maxAttempts int
	leaseTTL    time.Duration
	sweepEvery  time.Duration
	now         func() time.Time

	stop chan struct{}
	done chan struct{}
}

// Option configures a Coordinator.
type Option func(*Coordinator)

// WithMetrics attaches a metrics sink.
func WithMetrics(m Metrics) Option { return func(c *Coordinator) { c.metrics = m } }

// WithDefaults overrides the per-execution defaults applied when a submit
// omits max_attempts or lease_ttl.
func WithDefaults(maxAttempts int, leaseTTL time.Duration) Option {
	return func(c *Coordinator) {
		if maxAttempts > 0 {
			c.maxAttempts = maxAttempts
		}
		if leaseTTL > 0 {
			c.leaseTTL = leaseTTL
		}
	}
}

// WithSweepInterval overrides how often expired leases are re-enqueued.
func WithSweepInterval(d time.Duration) Option {
	return func(c *Coordinator) {
		if d > 0 {
			c.sweepEvery = d
		}
	}
}

// WithTerminalRetention bounds how many completed/failed executions stay
// in the in-memory index; the oldest are retired first. Their events
// remain on the log, so history is never lost - this only caps memory
// under sustained high completion rates. 0 (the default) keeps all.
func WithTerminalRetention(n int) Option {
	return func(c *Coordinator) {
		if n > 0 {
			c.store.retainTerminal = n
		}
	}
}

// withClock overrides the wall clock, for tests.
func withClock(now func() time.Time) Option {
	return func(c *Coordinator) { c.now = now }
}

// NewCoordinator builds a coordinator over its own empty store.
func NewCoordinator(pub Publisher, opts ...Option) *Coordinator {
	c := &Coordinator{
		store:       NewStore(),
		pub:         pub,
		maxAttempts: DefaultMaxAttempts,
		leaseTTL:    DefaultLeaseTTL,
		sweepEvery:  defaultSweepEvery,
		now:         time.Now,
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Store exposes the underlying store for read paths (service, tests).
func (c *Coordinator) Store() *Store { return c.store }

// Ingest folds one envelope from the log into the store. Called by the
// Poller for every workflow event, including ones this coordinator
// appended itself.
func (c *Coordinator) Ingest(e agentstream.Event) { c.store.Apply(e) }

// append publishes the envelope durably, then folds it locally so the next
// scheduling decision sees it immediately instead of waiting a poll tick.
func (c *Coordinator) append(ctx context.Context, env agentstream.Event) error {
	if _, _, err := c.pub.PublishEnvelope(ctx, Topic, env); err != nil {
		return err
	}
	c.store.Apply(env)
	if c.metrics != nil {
		c.metrics.IncWorkflowEvent(env.Type)
	}
	return nil
}

// SubmitSpec describes a new workflow execution.
type SubmitSpec struct {
	Tenant      string
	Project     string
	WorkflowID  string
	TaskType    string
	Input       []byte
	MaxAttempts int
	LeaseTTL    time.Duration
}

// Submit durably enqueues a new execution. Submitting an existing id is an
// idempotent no-op and reports alreadyExists.
func (c *Coordinator) Submit(ctx context.Context, spec SubmitSpec) (alreadyExists bool, err error) {
	if _, ok := c.store.Get(spec.Tenant, spec.Project, spec.WorkflowID); ok {
		return true, nil
	}
	maxAttempts := spec.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = c.maxAttempts
	}
	ttl := spec.LeaseTTL
	if ttl <= 0 {
		ttl = c.leaseTTL
	}
	env, err := newEnvelope(EventSubmitted, spec.Tenant, spec.Project, spec.WorkflowID,
		CoordinatorAgentID, 0, SubmitPayload{
			TaskType:    spec.TaskType,
			Input:       spec.Input,
			MaxAttempts: maxAttempts,
			LeaseTTLMS:  ttl.Milliseconds(),
		}, c.now())
	if err != nil {
		return false, err
	}
	return false, c.append(ctx, env)
}

// Lease is what a worker receives for one granted attempt.
type Lease struct {
	Tenant        string
	Project       string
	WorkflowID    string
	TaskType      string
	Input         []byte
	Attempt       int
	LeaseDeadline time.Time
}

// Lease grants the oldest pending execution of taskType to workerID,
// recording the lease on the log before the worker sees it. When nothing
// is pending it blocks up to wait (capped at 30s), then returns (nil, nil).
func (c *Coordinator) Lease(ctx context.Context, taskType, workerID string, wait time.Duration) (*Lease, error) {
	leases, err := c.LeaseBatch(ctx, taskType, workerID, 1, wait)
	if err != nil || len(leases) == 0 {
		return nil, err
	}
	return leases[0], nil
}

// LeaseBatch grants up to max pending executions of taskType to workerID
// in one call, amortizing per-call overhead for high-throughput workers.
// Each grant is still an individual durable log event. Blocks up to wait
// only while it has granted nothing; once at least one lease is held it
// returns immediately with what is available.
func (c *Coordinator) LeaseBatch(ctx context.Context, taskType, workerID string, max int, wait time.Duration) ([]*Lease, error) {
	if max <= 0 {
		max = 1
	}
	if wait > maxLeaseWait {
		wait = maxLeaseWait
	}
	deadline := c.now().Add(wait)
	var out []*Lease
	for len(out) < max {
		x := c.store.ReserveNext(taskType)
		if x == nil {
			if len(out) > 0 {
				return out, nil
			}
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return nil, nil
			}
			wakeCh := c.store.WaitChan(taskType)
			timer := time.NewTimer(remaining)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
				return nil, nil
			case <-wakeCh:
				timer.Stop()
			}
			continue
		}
		lease, err := c.grant(ctx, x, workerID)
		if err != nil {
			c.store.Unreserve(x)
			if len(out) > 0 {
				return out, nil // hand back what was already durably granted
			}
			return nil, err
		}
		out = append(out, lease)
	}
	return out, nil
}

// grant appends the wf.leased event for a reserved execution and builds
// the worker-facing lease.
func (c *Coordinator) grant(ctx context.Context, x *Execution, workerID string) (*Lease, error) {
	attempt := x.Attempt + 1
	env, err := newEnvelope(EventLeased, x.Tenant, x.Project, x.WorkflowID,
		workerID, attempt, LeasePayload{WorkerID: workerID}, c.now())
	if err == nil {
		err = c.append(ctx, env)
	}
	if err != nil {
		return nil, err
	}
	if c.metrics != nil {
		c.metrics.IncWorkflowLeaseGranted()
	}
	granted, _ := c.store.Get(x.Tenant, x.Project, x.WorkflowID)
	return &Lease{
		Tenant:        x.Tenant,
		Project:       x.Project,
		WorkflowID:    x.WorkflowID,
		TaskType:      x.TaskType,
		Input:         x.Input,
		Attempt:       attempt,
		LeaseDeadline: granted.LeaseDeadline,
	}, nil
}

// Heartbeat extends a running lease. It returns the new deadline, or
// valid = false when the lease is gone (expired, superseded, or terminal).
func (c *Coordinator) Heartbeat(ctx context.Context, tenant, project, workflowID, workerID string, attempt int) (valid bool, deadline time.Time, err error) {
	x, ok := c.store.Get(tenant, project, workflowID)
	if !ok {
		return false, time.Time{}, ErrNotFound
	}
	if x.Status != StatusRunning || x.Attempt != attempt || x.WorkerID != workerID {
		return false, time.Time{}, nil
	}
	env, err := newEnvelope(EventHeartbeat, tenant, project, workflowID,
		workerID, attempt, LeasePayload{WorkerID: workerID}, c.now())
	if err != nil {
		return false, time.Time{}, err
	}
	if err := c.append(ctx, env); err != nil {
		return false, time.Time{}, err
	}
	cur, _ := c.store.Get(tenant, project, workflowID)
	return true, cur.LeaseDeadline, nil
}

// Complete records a successful result exactly once. accepted = false with
// duplicate = true means the execution had already completed; accepted =
// false alone means the claim was stale (wrong attempt, terminal failure,
// or a concurrent completion in flight).
func (c *Coordinator) Complete(ctx context.Context, tenant, project, workflowID, workerID string, attempt int, result []byte) (accepted, duplicate bool, err error) {
	ok, dup := c.store.BeginComplete(tenant, project, workflowID, attempt)
	if !ok {
		if c.metrics != nil {
			c.metrics.IncWorkflowCompletionRejected()
		}
		return false, dup, nil
	}
	env, err := newEnvelope(EventCompleted, tenant, project, workflowID,
		workerID, attempt, CompletePayload{WorkerID: workerID, Result: result}, c.now())
	if err == nil {
		err = c.append(ctx, env)
	}
	if err != nil {
		c.store.AbortComplete(tenant, project, workflowID)
		return false, false, err
	}
	return true, false, nil
}

// Fail records a failed attempt, then either schedules a retry or declares
// the execution dead depending on retryable and remaining attempts.
func (c *Coordinator) Fail(ctx context.Context, tenant, project, workflowID, workerID string, attempt int, errMsg string, retryable bool) (accepted, willRetry bool, err error) {
	x, ok := c.store.Get(tenant, project, workflowID)
	if !ok {
		return false, false, ErrNotFound
	}
	if x.Terminal() || x.Status != StatusRunning || x.Attempt != attempt || x.WorkerID != workerID {
		return false, false, nil
	}
	env, err := newEnvelope(EventFailed, tenant, project, workflowID,
		workerID, attempt, FailPayload{WorkerID: workerID, Error: errMsg, Retryable: retryable}, c.now())
	if err != nil {
		return false, false, err
	}
	if err := c.append(ctx, env); err != nil {
		return false, false, err
	}
	if retryable && attempt < x.MaxAttempts {
		if err := c.appendReason(ctx, x, EventRetry, ReasonAttemptFailed); err != nil {
			return true, false, err
		}
		if c.metrics != nil {
			c.metrics.IncWorkflowRetry()
		}
		return true, true, nil
	}
	reason := ReasonAttemptsExhausted
	if !retryable {
		reason = ReasonNotRetryable
	}
	return true, false, c.appendReason(ctx, x, EventDead, reason)
}

func (c *Coordinator) appendReason(ctx context.Context, x Execution, evType, reason string) error {
	env, err := newEnvelope(evType, x.Tenant, x.Project, x.WorkflowID,
		CoordinatorAgentID, x.Attempt, ReasonPayload{Reason: reason}, c.now())
	if err != nil {
		return err
	}
	return c.append(ctx, env)
}

// SweepExpired re-enqueues (or kills) every running execution whose lease
// deadline passed. Returns how many leases were reclaimed.
func (c *Coordinator) SweepExpired(ctx context.Context) int {
	expired := c.store.ExpiredLeases(c.now())
	for i := range expired {
		x := expired[i]
		if c.metrics != nil {
			c.metrics.IncWorkflowLeaseExpired()
		}
		if x.Attempt < x.MaxAttempts {
			if err := c.appendReason(ctx, x, EventRetry, ReasonLeaseExpired); err == nil && c.metrics != nil {
				c.metrics.IncWorkflowRetry()
			}
		} else {
			_ = c.appendReason(ctx, x, EventDead, ReasonAttemptsExhausted)
		}
	}
	return len(expired)
}

// Start runs the expiry sweep loop until Stop is called.
func (c *Coordinator) Start() {
	go func() {
		defer close(c.done)
		t := time.NewTicker(c.sweepEvery)
		defer t.Stop()
		for {
			select {
			case <-c.stop:
				return
			case <-t.C:
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				c.SweepExpired(ctx)
				cancel()
			}
		}
	}()
}

// Stop halts the sweep loop and waits for it to exit.
func (c *Coordinator) Stop() {
	close(c.stop)
	<-c.done
}

// PublishGauges pushes execution counts by status to the metrics sink.
// Uses the incremental counters, so it is O(1) regardless of store size.
func (c *Coordinator) PublishGauges() {
	if c.metrics == nil {
		return
	}
	counts := c.store.CountByStatus()
	for _, st := range []Status{StatusPending, StatusRunning, StatusRetrying, StatusCompleted, StatusFailed} {
		c.metrics.SetWorkflowExecutions(string(st), counts[st])
	}
}
