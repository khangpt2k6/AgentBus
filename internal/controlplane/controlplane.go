package controlplane

import (
	"encoding/json"
	"time"

	"github.com/khangpt2k6/AgentBus/internal/agentstream"
)

// Default tuning for a freshly constructed control plane.
const (
	defaultAgentTTL  = 30 * time.Second
	defaultInboxSize = 256
)

// Event types the control plane interprets. Every other type is treated as
// generic activity (advances the run to running, bumps the step count). These
// mirror the SDK conventions in agentbus/events.go but are kept local so the
// internal control plane does not depend on the public SDK package.
const (
	eventHandoff  = "handoff"
	eventError    = "error"
	eventComplete = "complete"

	// eventEscalation is synthesized by the control plane (not a producer) when
	// it auto-escalates a failed run to the configured escalation agent.
	eventEscalation = "escalation"
)

// CPMetrics is the optional metrics sink the control plane publishes to. A nil
// sink is fine; methods are simply not called.
type CPMetrics interface {
	SetActiveAgents(n int)
	SetSessions(status string, n int)
	IncHandoffRouted()
	IncHandoffUnrouted()
	IncEscalation()
}

// ControlPlane ties the registry, session store, and inboxes together. It
// ingests agent events (read-side) and exposes a facade the gRPC service calls.
type ControlPlane struct {
	reg             *Registry
	sessions        *SessionStore
	inboxes         *Inboxes
	metrics         CPMetrics
	escalationAgent string // when set, terminal errors auto-escalate to this agent
}

// Option configures a ControlPlane.
type Option func(*ControlPlane)

// WithMetrics attaches a metrics sink.
func WithMetrics(m CPMetrics) Option { return func(cp *ControlPlane) { cp.metrics = m } }

// WithAgentTTL overrides the registry staleness TTL.
func WithAgentTTL(ttl time.Duration) Option {
	return func(cp *ControlPlane) { cp.reg = NewRegistry(ttl) }
}

// WithInboxSize overrides the per-agent inbox buffer size.
func WithInboxSize(size int) Option {
	return func(cp *ControlPlane) { cp.inboxes = NewInboxes(size) }
}

// WithEscalationAgent enables active escalation: when a session emits a terminal
// error, the control plane routes an escalation event to this agent and marks
// the run waiting on it. Empty (the default) disables escalation.
func WithEscalationAgent(id string) Option {
	return func(cp *ControlPlane) { cp.escalationAgent = id }
}

// New builds a control plane with sensible defaults.
func New(opts ...Option) *ControlPlane {
	cp := &ControlPlane{
		reg:      NewRegistry(defaultAgentTTL),
		sessions: NewSessionStore(),
		inboxes:  NewInboxes(defaultInboxSize),
	}
	for _, o := range opts {
		o(cp)
	}
	return cp
}

// handoffPayload is the minimal shape we read from a handoff event's payload.
type handoffPayload struct {
	ToAgent string `json:"to_agent"`
}

func parseToAgent(payload []byte) string {
	var h handoffPayload
	if err := json.Unmarshal(payload, &h); err != nil {
		return ""
	}
	return h.ToAgent
}

// Ingest advances control-plane state for one agent event. It marks the
// producing agent busy, applies the session state machine, and routes handoffs
// to the target agent's inbox. It never blocks.
func (cp *ControlPlane) Ingest(e agentstream.Event) {
	cp.reg.Touch(e.AgentID, agentstream.SessionKey(e.Tenant, e.Project, e.SessionID))

	toAgent := ""
	if e.Type == eventHandoff {
		toAgent = parseToAgent(e.Payload)
	}
	cp.sessions.Apply(e, toAgent)

	if e.Type == eventHandoff && toAgent != "" {
		re := RoutedEvent{
			SessionKey: agentstream.SessionKey(e.Tenant, e.Project, e.SessionID),
			Type:       e.Type,
			Payload:    e.Payload,
		}
		if cp.inboxes.Deliver(toAgent, re) {
			cp.incRouted()
		} else {
			cp.incUnrouted()
		}
	}

	// Active orchestration: on a terminal error, auto-escalate to the configured
	// agent (unless it is the one that errored). The bus initiates this step.
	if e.Type == eventError && cp.escalationAgent != "" && cp.escalationAgent != e.AgentID {
		cp.escalate(e)
	}
}

// escalate routes an escalation event to the configured escalation agent and
// moves the session to waiting on it.
func (cp *ControlPlane) escalate(e agentstream.Event) {
	key := agentstream.SessionKey(e.Tenant, e.Project, e.SessionID)
	re := RoutedEvent{
		SessionKey: key,
		Type:       eventEscalation,
		Payload:    []byte(`{"reason":"terminal error","from_agent":"` + e.AgentID + `"}`),
	}
	cp.sessions.Escalate(key, cp.escalationAgent)
	cp.inboxes.Deliver(cp.escalationAgent, re) // best-effort; session is marked waiting regardless
	cp.incEscalation()
}

// --- facade for the gRPC service ---

func (cp *ControlPlane) Register(id string, caps []string) { cp.reg.Register(id, caps) }
func (cp *ControlPlane) Heartbeat(id string) bool          { return cp.reg.Heartbeat(id) }
func (cp *ControlPlane) Deregister(id string)              { cp.reg.Deregister(id) }
func (cp *ControlPlane) OpenInbox(id string) <-chan RoutedEvent {
	cp.reg.Register(id, nil) // opening an inbox implies the agent is present
	return cp.inboxes.Open(id)
}
func (cp *ControlPlane) ListAgents() []AgentInfo { return cp.reg.List() }

// ListSessions returns a snapshot of all tracked session run-states.
func (cp *ControlPlane) ListSessions() []RunState { return cp.sessions.List() }

// GetSession returns the run-state for a session triple.
func (cp *ControlPlane) GetSession(tenant, project, session string) (RunState, bool) {
	return cp.sessions.Get(agentstream.SessionKey(tenant, project, session))
}

// Sweep marks stale agents offline and republishes gauges to the metrics sink.
func (cp *ControlPlane) Sweep() {
	cp.reg.SweepStale()
	if cp.metrics == nil {
		return
	}
	cp.metrics.SetActiveAgents(cp.reg.ActiveCount())
	for status, n := range cp.sessions.CountByStatus() {
		cp.metrics.SetSessions(string(status), n)
	}
}

func (cp *ControlPlane) incRouted() {
	if cp.metrics != nil {
		cp.metrics.IncHandoffRouted()
	}
}

func (cp *ControlPlane) incUnrouted() {
	if cp.metrics != nil {
		cp.metrics.IncHandoffUnrouted()
	}
}

func (cp *ControlPlane) incEscalation() {
	if cp.metrics != nil {
		cp.metrics.IncEscalation()
	}
}
