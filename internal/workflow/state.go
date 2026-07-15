package workflow

import (
	"encoding/json"
	"time"

	"github.com/khangpt2k6/AgentBus/internal/agentstream"
)

// Status is an execution's lifecycle state.
type Status string

const (
	StatusPending   Status = "pending"   // waiting for a worker lease
	StatusRunning   Status = "running"   // leased to a worker
	StatusRetrying  Status = "retrying"  // attempt failed, coordinator deciding
	StatusCompleted Status = "completed" // terminal success
	StatusFailed    Status = "failed"    // terminal failure
)

// Transition is one recorded step in an execution's history. Heartbeats
// update the lease deadline but are not recorded, so history stays bounded
// by attempts rather than by task duration.
type Transition struct {
	EventType string
	Status    Status
	Attempt   int
	WorkerID  string
	Detail    string
	At        time.Time
}

// Execution is the folded state of one workflow execution. All fields are
// derived exclusively from events, which is what makes rebuilds from the
// log deterministic.
type Execution struct {
	Tenant     string
	Project    string
	WorkflowID string

	TaskType    string
	Status      Status
	Attempt     int // granted leases so far, 1-based
	MaxAttempts int
	LeaseTTL    time.Duration
	WorkerID    string
	Input       []byte
	Result      []byte
	Error       string

	SubmittedAt   time.Time
	UpdatedAt     time.Time
	LeaseDeadline time.Time

	Transitions []Transition

	// Runtime-only coordination flags; not part of folded state and reset
	// to false on rebuild. reserved parks an execution while its lease
	// event is being appended; completing fences concurrent completion
	// calls for the same attempt.
	reserved   bool
	completing bool
}

// Key returns the log key of this execution (same shape as a session key,
// so executions shard exactly like sessions).
func (x *Execution) Key() string {
	return agentstream.SessionKey(x.Tenant, x.Project, x.WorkflowID)
}

// Terminal reports whether the execution reached a final state.
func (x *Execution) Terminal() bool {
	return x.Status == StatusCompleted || x.Status == StatusFailed
}

func (x *Execution) record(evType string, attempt int, worker, detail string, at time.Time) {
	x.UpdatedAt = at
	x.Transitions = append(x.Transitions, Transition{
		EventType: evType,
		Status:    x.Status,
		Attempt:   attempt,
		WorkerID:  worker,
		Detail:    detail,
		At:        at,
	})
}

// apply folds one event into the execution and reports whether state
// changed. It is a pure function of (state, event): decisions depend only
// on folded fields and the event's own timestamp, and re-applying a
// delivered event is a no-op, so the local synchronous apply and the
// poller's later re-delivery converge on identical state.
func (x *Execution) apply(e agentstream.Event) bool {
	at := eventTime(e)
	switch e.Type {
	case EventLeased:
		var p LeasePayload
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			return false
		}
		if x.Terminal() || x.Status != StatusPending || e.Attempt != x.Attempt+1 {
			return false
		}
		x.Status = StatusRunning
		x.Attempt = e.Attempt
		x.WorkerID = p.WorkerID
		x.LeaseDeadline = at.Add(x.LeaseTTL)
		x.reserved = false
		x.record(EventLeased, e.Attempt, p.WorkerID, "", at)
		return true

	case EventHeartbeat:
		var p LeasePayload
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			return false
		}
		if x.Status != StatusRunning || e.Attempt != x.Attempt || p.WorkerID != x.WorkerID {
			return false
		}
		x.LeaseDeadline = at.Add(x.LeaseTTL)
		x.UpdatedAt = at
		return true

	case EventCompleted:
		var p CompletePayload
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			return false
		}
		// Accept only the current attempt on a live execution. A zombie
		// worker whose lease was re-granted carries a stale attempt and is
		// fenced out here - this is the exactly-once completion gate.
		if x.Terminal() || e.Attempt != x.Attempt || e.Attempt == 0 {
			return false
		}
		x.Status = StatusCompleted
		x.Result = p.Result
		x.WorkerID = p.WorkerID
		x.completing = false
		x.record(EventCompleted, e.Attempt, p.WorkerID, "", at)
		return true

	case EventFailed:
		var p FailPayload
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			return false
		}
		if x.Terminal() || x.Status != StatusRunning || e.Attempt != x.Attempt {
			return false
		}
		x.Status = StatusRetrying
		x.Error = p.Error
		x.record(EventFailed, e.Attempt, p.WorkerID, p.Error, at)
		return true

	case EventRetry:
		var p ReasonPayload
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			return false
		}
		if x.Terminal() || x.Status == StatusPending {
			return false
		}
		x.Status = StatusPending
		x.WorkerID = ""
		x.LeaseDeadline = time.Time{}
		x.record(EventRetry, x.Attempt, "", p.Reason, at)
		return true

	case EventDead:
		var p ReasonPayload
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			return false
		}
		if x.Terminal() {
			return false
		}
		x.Status = StatusFailed
		if x.Error == "" {
			x.Error = p.Reason
		}
		x.record(EventDead, x.Attempt, "", p.Reason, at)
		return true
	}
	return false
}

// newExecution folds a wf.submitted event into a fresh execution.
func newExecution(e agentstream.Event) (*Execution, bool) {
	var p SubmitPayload
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		return nil, false
	}
	if p.TaskType == "" {
		return nil, false
	}
	at := eventTime(e)
	x := &Execution{
		Tenant:      e.Tenant,
		Project:     e.Project,
		WorkflowID:  e.SessionID,
		TaskType:    p.TaskType,
		Status:      StatusPending,
		MaxAttempts: p.MaxAttempts,
		LeaseTTL:    time.Duration(p.LeaseTTLMS) * time.Millisecond,
		Input:       p.Input,
		SubmittedAt: at,
	}
	x.record(EventSubmitted, 0, e.AgentID, p.TaskType, at)
	return x, true
}
