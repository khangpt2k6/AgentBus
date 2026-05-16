package agentstream

import (
	"testing"
)

func TestSessionTraceIDDeterministic(t *testing.T) {
	a := SessionTraceID("acme", "bot", "sess-42")
	b := SessionTraceID("acme", "bot", "sess-42")
	if a != b {
		t.Errorf("not deterministic: %s vs %s", a, b)
	}
	if !a.IsValid() {
		t.Errorf("invalid trace id: %s", a)
	}
}

func TestSessionTraceIDDistinctForDistinctSessions(t *testing.T) {
	a := SessionTraceID("acme", "bot", "sess-42")
	b := SessionTraceID("acme", "bot", "sess-43")
	c := SessionTraceID("acme", "other", "sess-42")
	if a == b || a == c || b == c {
		t.Errorf("collision: %s %s %s", a, b, c)
	}
}

func TestSessionTraceIDEmptyReturnsInvalid(t *testing.T) {
	id := SessionTraceID("", "", "")
	if id.IsValid() {
		t.Errorf("empty key should yield invalid trace id, got %s", id)
	}
}

func TestPeekEnvelopeAcceptsValidJSON(t *testing.T) {
	payload := []byte(`{"version":"v1","type":"tool.call","tenant":"acme","project":"bot","session_id":"s1","agent_id":"a","created_at":"2026-01-01T00:00:00Z","payload":{}}`)
	ev, ok := PeekEnvelope(payload)
	if !ok {
		t.Fatal("should peek")
	}
	if ev.Tenant != "acme" || ev.SessionID != "s1" || ev.Type != "tool.call" {
		t.Errorf("wrong fields: %+v", ev)
	}
}

func TestPeekEnvelopeRejectsGarbage(t *testing.T) {
	for _, p := range [][]byte{
		nil, []byte("not json"), []byte("[1,2,3]"), []byte(`{}`),
		[]byte(`{"hello":"world"}`),
	} {
		if _, ok := PeekEnvelope(p); ok {
			t.Errorf("should reject %q", p)
		}
	}
}

func TestAttributesForIncludesOptionalOnlyWhenSet(t *testing.T) {
	full := Event{
		Tenant: "a", Project: "b", SessionID: "c", AgentID: "d",
		Type: "tool.call", Step: "step1", Attempt: 2,
	}
	min := Event{Tenant: "a", Project: "b", SessionID: "c", Type: "tool.call"}
	if len(AttributesFor(full)) <= len(AttributesFor(min)) {
		t.Error("full event should have more attributes than minimal")
	}
}
