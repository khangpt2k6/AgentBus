package controlplane

import (
	"sort"
	"sync"
	"time"

	"github.com/khangpt2k6/EventBus/internal/agentstream"
)

// RunStatus is the workflow state of one agent session.
type RunStatus string

const (
	StatusRunning   RunStatus = "running"   // an agent is actively producing events
	StatusWaiting   RunStatus = "waiting"   // handed off, awaiting the target agent's first event
	StatusFailed    RunStatus = "failed"    // an error event was observed
	StatusCompleted RunStatus = "completed" // a complete event was observed
)

// RunState is the control plane's view of one session's progress.
type RunState struct {
	SessionKey   string
	ActiveAgent  string
	PendingAgent string // set after a handoff, until the target agent picks up
	StepCount    int
	Status       RunStatus
	LastEventAt  time.Time
}

// SessionStore is a concurrency-safe map of session run-state, advanced by the
// agent-event stream. It is the "where is each run" half of the control plane.
type SessionStore struct {
	mu       sync.RWMutex
	sessions map[string]*RunState
	now      func() time.Time
}

// NewSessionStore returns an empty store.
func NewSessionStore() *SessionStore {
	return &SessionStore{
		sessions: make(map[string]*RunState),
		now:      time.Now,
	}
}

// Apply advances the run-state machine for one event. toAgent is the handoff
// target and is only consulted for handoff events.
func (s *SessionStore) Apply(e agentstream.Event, toAgent string) {
	k := agentstream.SessionKey(e.Tenant, e.Project, e.SessionID)
	if k == "//" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	rs := s.sessions[k]
	if rs == nil {
		rs = &RunState{SessionKey: k, ActiveAgent: e.AgentID, Status: StatusRunning}
		s.sessions[k] = rs
	}
	rs.LastEventAt = s.now()

	// Handoff pickup: any event from the pending agent claims the active role.
	if rs.PendingAgent != "" && e.AgentID == rs.PendingAgent {
		rs.ActiveAgent = e.AgentID
		rs.PendingAgent = ""
		rs.Status = StatusRunning
	}

	switch e.Type {
	case eventHandoff:
		if toAgent != "" {
			rs.PendingAgent = toAgent
			rs.Status = StatusWaiting
		}
		rs.StepCount++
	case eventError:
		rs.Status = StatusFailed
		rs.StepCount++
	case eventComplete:
		rs.Status = StatusCompleted
		rs.StepCount++
	default:
		if rs.Status != StatusFailed && rs.Status != StatusCompleted {
			rs.Status = StatusRunning
		}
		if rs.ActiveAgent == "" {
			rs.ActiveAgent = e.AgentID
		}
		rs.StepCount++
	}
}

// Get returns a snapshot of one session's run-state.
func (s *SessionStore) Get(sessionKey string) (RunState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rs := s.sessions[sessionKey]
	if rs == nil {
		return RunState{}, false
	}
	return *rs, true
}

// Escalate moves a session to waiting on the given agent. The control plane
// calls this when it initiates an automatic escalation (e.g. after a terminal
// error), so the run waits for the escalation agent to pick it up.
func (s *SessionStore) Escalate(sessionKey, toAgent string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rs := s.sessions[sessionKey]
	if rs == nil {
		return
	}
	rs.PendingAgent = toAgent
	rs.Status = StatusWaiting
	rs.LastEventAt = s.now()
}

// List returns a snapshot of all session run-states, sorted by session key.
func (s *SessionStore) List() []RunState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]RunState, 0, len(s.sessions))
	for _, rs := range s.sessions {
		out = append(out, *rs)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SessionKey < out[j].SessionKey })
	return out
}

// CountByStatus returns how many sessions are in each status.
func (s *SessionStore) CountByStatus() map[RunStatus]int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[RunStatus]int, 4)
	for _, rs := range s.sessions {
		out[rs.Status]++
	}
	return out
}
