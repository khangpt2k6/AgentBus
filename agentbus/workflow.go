package agentbus

import (
	"context"
	"errors"
	"fmt"
	"time"

	pb "github.com/khangpt2k6/AgentBus/proto"
)

// WorkflowSpec describes a durable workflow execution to submit.
type WorkflowSpec struct {
	Tenant     string
	Project    string
	// WorkflowID uniquely names this execution within (Tenant, Project).
	// Resubmitting the same id is an idempotent no-op.
	WorkflowID string
	// TaskType names the worker queue that will execute this workflow.
	TaskType string
	Input    []byte
	// MaxAttempts caps lease attempts before the execution is declared
	// failed. 0 uses the broker default (3).
	MaxAttempts int
	// LeaseTTL is how long a worker may hold a lease without completing,
	// failing, or heartbeating. 0 uses the broker default (30s).
	LeaseTTL time.Duration
}

// SubmitWorkflow durably enqueues a workflow execution. In cluster mode it
// transparently redirects to the shard leader, mirroring PublishAgent.
// Returns alreadyExists = true when the id was submitted before.
func (c *Client) SubmitWorkflow(ctx context.Context, spec WorkflowSpec) (alreadyExists bool, err error) {
	req := &pb.SubmitWorkflowRequest{
		Tenant:      spec.Tenant,
		Project:     spec.Project,
		WorkflowId:  spec.WorkflowID,
		TaskType:    spec.TaskType,
		Input:       spec.Input,
		MaxAttempts: int32(spec.MaxAttempts),
		LeaseTtlMs:  spec.LeaseTTL.Milliseconds(),
	}
	resp, err := c.wf.SubmitWorkflow(ctx, req)
	if hint, redirect := notLeaderHint(err); redirect {
		conn, dialErr := dialLeader(ctx, hint)
		if dialErr != nil {
			return false, dialErr
		}
		defer conn.Close()
		resp, err = pb.NewWorkflowServiceClient(conn).SubmitWorkflow(ctx, req)
	}
	if err != nil {
		return false, fmt.Errorf("agentbus: submit workflow: %w", err)
	}
	return resp.AlreadyExists, nil
}

// ExecutionState is a client-side view of one workflow execution.
type ExecutionState struct {
	Tenant        string
	Project       string
	WorkflowID    string
	TaskType      string
	Status        string // pending | running | retrying | completed | failed
	Attempt       int
	MaxAttempts   int
	WorkerID      string
	Result        []byte
	Error         string
	SubmittedAt   time.Time
	UpdatedAt     time.Time
	LeaseDeadline time.Time
}

// GetExecution fetches the current state of one execution. found = false
// means the broker does not know the id.
func (c *Client) GetExecution(ctx context.Context, tenant, project, workflowID string) (ExecutionState, bool, error) {
	resp, err := c.wf.GetExecution(ctx, &pb.GetExecutionRequest{
		Tenant: tenant, Project: project, WorkflowId: workflowID,
	})
	if err != nil {
		return ExecutionState{}, false, fmt.Errorf("agentbus: get execution: %w", err)
	}
	if !resp.Found {
		return ExecutionState{}, false, nil
	}
	out := ExecutionState{
		Tenant:      resp.Tenant,
		Project:     resp.Project,
		WorkflowID:  resp.WorkflowId,
		TaskType:    resp.TaskType,
		Status:      resp.Status,
		Attempt:     int(resp.Attempt),
		MaxAttempts: int(resp.MaxAttempts),
		WorkerID:    resp.WorkerId,
		Result:      resp.Result,
		Error:       resp.Error,
		SubmittedAt: time.Unix(0, resp.SubmittedUnixNano),
		UpdatedAt:   time.Unix(0, resp.UpdatedUnixNano),
	}
	if resp.LeaseDeadlineUnixNano != 0 {
		out.LeaseDeadline = time.Unix(0, resp.LeaseDeadlineUnixNano)
	}
	return out, true, nil
}

// ExecutionTransition is one step of an execution's history as rebuilt
// deterministically from the event log.
type ExecutionTransition struct {
	EventType string
	Status    string
	Attempt   int
	WorkerID  string
	Detail    string
	At        time.Time
}

// ExecutionHistory returns the ordered state transitions of one execution.
func (c *Client) ExecutionHistory(ctx context.Context, tenant, project, workflowID string) ([]ExecutionTransition, bool, error) {
	resp, err := c.wf.GetExecutionHistory(ctx, &pb.GetExecutionRequest{
		Tenant: tenant, Project: project, WorkflowId: workflowID,
	})
	if err != nil {
		return nil, false, fmt.Errorf("agentbus: execution history: %w", err)
	}
	if !resp.Found {
		return nil, false, nil
	}
	out := make([]ExecutionTransition, 0, len(resp.Transitions))
	for _, tr := range resp.Transitions {
		out = append(out, ExecutionTransition{
			EventType: tr.EventType,
			Status:    tr.Status,
			Attempt:   int(tr.Attempt),
			WorkerID:  tr.WorkerId,
			Detail:    tr.Detail,
			At:        time.Unix(0, tr.AtUnixNano),
		})
	}
	return out, true, nil
}

// ExecutionSummary is a compact listing row.
type ExecutionSummary struct {
	Tenant     string
	Project    string
	WorkflowID string
	TaskType   string
	Status     string
	Attempt    int
	UpdatedAt  time.Time
}

// ListExecutions returns execution summaries (optionally filtered by
// status) plus counts by status over every tracked execution.
func (c *Client) ListExecutions(ctx context.Context, status string, limit int) ([]ExecutionSummary, map[string]int64, error) {
	resp, err := c.wf.ListExecutions(ctx, &pb.ListExecutionsRequest{Status: status, Limit: int32(limit)})
	if err != nil {
		return nil, nil, fmt.Errorf("agentbus: list executions: %w", err)
	}
	out := make([]ExecutionSummary, 0, len(resp.Executions))
	for _, x := range resp.Executions {
		out = append(out, ExecutionSummary{
			Tenant:     x.Tenant,
			Project:    x.Project,
			WorkflowID: x.WorkflowId,
			TaskType:   x.TaskType,
			Status:     x.Status,
			Attempt:    int(x.Attempt),
			UpdatedAt:  time.Unix(0, x.UpdatedUnixNano),
		})
	}
	return out, resp.Counts, nil
}

// Task is one leased workflow attempt handed to a worker handler.
type Task struct {
	Tenant        string
	Project       string
	WorkflowID    string
	TaskType      string
	Input         []byte
	Attempt       int
	LeaseDeadline time.Time
}

// ErrNonRetryable wraps handler errors that must not be retried; the
// execution is declared failed immediately regardless of remaining
// attempts. Use with fmt.Errorf("...: %w", agentbus.ErrNonRetryable) or
// errors.Join.
var ErrNonRetryable = errors.New("agentbus: non-retryable task failure")

// TaskHandler executes one leased attempt. Returning nil error completes
// the execution with the returned result bytes; returning an error fails
// the attempt (retryable unless the error wraps ErrNonRetryable). The
// context is canceled when the worker shuts down.
type TaskHandler func(ctx context.Context, task Task) ([]byte, error)

// Worker leases tasks of one task type and runs a handler over them, with
// automatic heartbeating for long-running handlers, until its context is
// canceled.
type Worker struct {
	client      *Client
	taskType    string
	handler     TaskHandler
	workerID    string
	concurrency int
	leaseWait   time.Duration
}

// WorkerOption configures a Worker.
type WorkerOption func(*Worker)

// WithWorkerID sets a stable worker identity (default: generated).
func WithWorkerID(id string) WorkerOption { return func(w *Worker) { w.workerID = id } }

// WithConcurrency sets how many tasks the worker runs at once (default 1).
func WithConcurrency(n int) WorkerOption {
	return func(w *Worker) {
		if n > 0 {
			w.concurrency = n
		}
	}
}

// WithLeaseWait sets the long-poll duration of each lease request
// (default 5s).
func WithLeaseWait(d time.Duration) WorkerOption {
	return func(w *Worker) {
		if d > 0 {
			w.leaseWait = d
		}
	}
}

// NewWorker builds a worker for one task type. Call Run to start it.
func NewWorker(c *Client, taskType string, handler TaskHandler, opts ...WorkerOption) *Worker {
	w := &Worker{
		client:      c,
		taskType:    taskType,
		handler:     handler,
		workerID:    fmt.Sprintf("worker-%d", time.Now().UnixNano()),
		concurrency: 1,
		leaseWait:   5 * time.Second,
	}
	for _, o := range opts {
		o(w)
	}
	return w
}

// Run leases and executes tasks until ctx is canceled. It returns ctx's
// error after all in-flight handlers finish.
func (w *Worker) Run(ctx context.Context) error {
	done := make(chan struct{})
	for i := 0; i < w.concurrency; i++ {
		slot := i
		go func() {
			defer func() { done <- struct{}{} }()
			w.loop(ctx, fmt.Sprintf("%s-%d", w.workerID, slot))
		}()
	}
	for i := 0; i < w.concurrency; i++ {
		<-done
	}
	return ctx.Err()
}

func (w *Worker) loop(ctx context.Context, workerID string) {
	for ctx.Err() == nil {
		resp, err := w.client.wf.LeaseTask(ctx, &pb.LeaseTaskRequest{
			TaskType: w.taskType,
			WorkerId: workerID,
			WaitMs:   w.leaseWait.Milliseconds(),
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			time.Sleep(500 * time.Millisecond) // transient broker error; back off
			continue
		}
		if !resp.Found {
			continue
		}
		w.execute(ctx, workerID, resp)
	}
}

func (w *Worker) execute(ctx context.Context, workerID string, lease *pb.LeaseTaskResponse) {
	task := Task{
		Tenant:        lease.Tenant,
		Project:       lease.Project,
		WorkflowID:    lease.WorkflowId,
		TaskType:      lease.TaskType,
		Input:         lease.Input,
		Attempt:       int(lease.Attempt),
		LeaseDeadline: time.Unix(0, lease.LeaseDeadlineUnixNano),
	}

	// Heartbeat at a third of the remaining lease so a slow handler keeps
	// its lease alive. Stops when the handler returns or the lease is
	// reported gone.
	hbCtx, stopHB := context.WithCancel(ctx)
	defer stopHB()
	go func() {
		deadline := task.LeaseDeadline
		for {
			interval := time.Until(deadline) / 3
			if interval < time.Second {
				interval = time.Second
			}
			select {
			case <-hbCtx.Done():
				return
			case <-time.After(interval):
			}
			resp, err := w.client.wf.HeartbeatTask(hbCtx, &pb.HeartbeatTaskRequest{
				Tenant:     task.Tenant,
				Project:    task.Project,
				WorkflowId: task.WorkflowID,
				WorkerId:   workerID,
				Attempt:    int32(task.Attempt),
			})
			if err != nil || !resp.Valid {
				return
			}
			deadline = time.Unix(0, resp.LeaseDeadlineUnixNano)
		}
	}()

	result, err := w.handler(ctx, task)
	stopHB()
	if err != nil {
		_, _ = w.client.wf.FailTask(ctx, &pb.FailTaskRequest{
			Tenant:     task.Tenant,
			Project:    task.Project,
			WorkflowId: task.WorkflowID,
			WorkerId:   workerID,
			Attempt:    int32(task.Attempt),
			Error:      err.Error(),
			Retryable:  !errors.Is(err, ErrNonRetryable),
		})
		return
	}
	_, _ = w.client.wf.CompleteTask(ctx, &pb.CompleteTaskRequest{
		Tenant:     task.Tenant,
		Project:    task.Project,
		WorkflowId: task.WorkflowID,
		WorkerId:   workerID,
		Attempt:    int32(task.Attempt),
		Result:     result,
	})
}
