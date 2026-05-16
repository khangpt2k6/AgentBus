package agentstream

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// OTEL semantic conventions used by AgentBus. Adopted in spans on the broker
// (and recommended for SDK callers' own spans) so a single trace search like
// `agent.session.id = sess-42` returns every event in a session.
const (
	AttrAgentTenant  = "agent.session.tenant"
	AttrAgentProject = "agent.session.project"
	AttrAgentSession = "agent.session.id"
	AttrAgentID      = "agent.id"
	AttrAgentType    = "agent.event.type"
	AttrAgentStep    = "agent.event.step"
	AttrAgentAttempt = "agent.event.attempt"
)

// PeekEnvelope quickly extracts the agent envelope fields used for tracing
// from a raw payload. Returns (zero, false) if the bytes are not a
// recognizable envelope. Cheaper than full decode — only inspects the
// fields we tag spans with.
func PeekEnvelope(payload []byte) (Event, bool) {
	if len(payload) == 0 || payload[0] != '{' {
		return Event{}, false
	}
	var ev Event
	if err := json.Unmarshal(payload, &ev); err != nil {
		return Event{}, false
	}
	if ev.Tenant == "" || ev.SessionID == "" || ev.Type == "" {
		return Event{}, false
	}
	return ev, true
}

// AttributesFor produces the standard set of agent attributes for an event.
// Use to tag any span where the event flows through.
func AttributesFor(ev Event) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String(AttrAgentTenant, ev.Tenant),
		attribute.String(AttrAgentProject, ev.Project),
		attribute.String(AttrAgentSession, ev.SessionID),
		attribute.String(AttrAgentType, ev.Type),
	}
	if ev.AgentID != "" {
		attrs = append(attrs, attribute.String(AttrAgentID, ev.AgentID))
	}
	if ev.Step != "" {
		attrs = append(attrs, attribute.String(AttrAgentStep, ev.Step))
	}
	if ev.Attempt > 0 {
		attrs = append(attrs, attribute.Int(AttrAgentAttempt, ev.Attempt))
	}
	return attrs
}

// SessionTraceID derives a deterministic OTEL TraceID from a session key.
// All events for the same session map to the same TraceID — backends like
// Tempo can then group them into a single trace UI even when the producers
// did not propagate trace context.
//
// Returns an invalid TraceID (zero) when session is empty. Callers should
// check with TraceID.IsValid().
func SessionTraceID(tenant, project, session string) trace.TraceID {
	key := SessionKey(tenant, project, session)
	if key == "//" {
		return trace.TraceID{}
	}
	sum := sha256.Sum256([]byte("agentbus/session/" + key))
	var id trace.TraceID
	// TraceID is 16 bytes. OTEL rejects all-zero IDs; SHA256 of a non-empty
	// input is effectively never zero, but be defensive.
	copy(id[:], sum[:16])
	if binary.BigEndian.Uint64(id[:8])|binary.BigEndian.Uint64(id[8:]) == 0 {
		id[15] = 1
	}
	return id
}
