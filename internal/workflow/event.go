// Package workflow is a durable task-execution layer on top of the event
// log. Every state transition of an execution is an event appended through
// the same durable publish path as agent events (main WAL single-node,
// shard WAL + quorum replication in cluster mode). Execution state is a
// pure fold over those events, so it rebuilds deterministically from the
// log after a crash, restart, or leader failover.
package workflow

import (
	"encoding/json"
	"time"

	"github.com/khangpt2k6/EventBus/internal/agentstream"
)

// Topic is the log topic workflow events are appended to. Kept separate
// from agent-events so the control plane and the execution runtime tail
// independent streams.
const Topic = "workflow-events"

// CoordinatorAgentID marks events synthesized by the coordinator itself
// (retries, deaths) rather than by a worker.
const CoordinatorAgentID = "wf-coordinator"

// Event types. The envelope's Attempt field carries the lease attempt the
// event belongs to; attempt numbers are 1-based and count granted leases.
const (
	EventSubmitted = "wf.submitted"
	EventLeased    = "wf.leased"
	EventHeartbeat = "wf.heartbeat"
	EventCompleted = "wf.completed"
	EventFailed    = "wf.failed"
	EventRetry     = "wf.retry"
	EventDead      = "wf.dead"
)

// SubmitPayload is the payload of a wf.submitted event.
type SubmitPayload struct {
	TaskType    string `json:"task_type"`
	Input       []byte `json:"input,omitempty"`
	MaxAttempts int    `json:"max_attempts"`
	LeaseTTLMS  int64  `json:"lease_ttl_ms"`
}

// LeasePayload is the payload of wf.leased and wf.heartbeat events; the
// attempt lives in the envelope.
type LeasePayload struct {
	WorkerID string `json:"worker_id"`
}

// CompletePayload is the payload of a wf.completed event.
type CompletePayload struct {
	WorkerID string `json:"worker_id"`
	Result   []byte `json:"result,omitempty"`
}

// FailPayload is the payload of a wf.failed event.
type FailPayload struct {
	WorkerID  string `json:"worker_id"`
	Error     string `json:"error"`
	Retryable bool   `json:"retryable"`
}

// ReasonPayload is the payload of wf.retry and wf.dead events.
type ReasonPayload struct {
	Reason string `json:"reason"`
}

// Retry / death reasons.
const (
	ReasonLeaseExpired      = "lease_expired"
	ReasonAttemptFailed     = "attempt_failed"
	ReasonNotRetryable      = "not_retryable"
	ReasonAttemptsExhausted = "attempts_exhausted"
)

// newEnvelope builds a workflow event envelope with an explicit CreatedAt
// so the local synchronous apply and the later poller re-delivery fold the
// exact same bytes.
func newEnvelope(evType, tenant, project, workflowID, agentID string, attempt int, payload any, at time.Time) (agentstream.Event, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return agentstream.Event{}, err
	}
	return agentstream.Event{
		Version:   "v1",
		Type:      evType,
		Tenant:    tenant,
		Project:   project,
		SessionID: workflowID,
		AgentID:   agentID,
		Attempt:   attempt,
		CreatedAt: at.UTC().Format(time.RFC3339Nano),
		Payload:   raw,
	}, nil
}

// IsWorkflowEvent reports whether the envelope type belongs to this layer.
func IsWorkflowEvent(evType string) bool {
	switch evType {
	case EventSubmitted, EventLeased, EventHeartbeat, EventCompleted,
		EventFailed, EventRetry, EventDead:
		return true
	}
	return false
}

// eventTime parses the envelope timestamp. Folds must use this - never the
// wall clock - so replaying the log reproduces identical state.
func eventTime(e agentstream.Event) time.Time {
	t, err := time.Parse(time.RFC3339Nano, e.CreatedAt)
	if err != nil {
		return time.Time{}
	}
	return t
}
