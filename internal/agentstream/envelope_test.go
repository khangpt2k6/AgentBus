package agentstream

import (
	"encoding/json"
	"testing"
)

func TestSessionKey(t *testing.T) {
	got := SessionKey("acme", "support-bot", "sess-42")
	want := "acme/support-bot/sess-42"
	if got != want {
		t.Fatalf("SessionKey()=%q want %q", got, want)
	}
}

func TestMarshalSetsDefaults(t *testing.T) {
	e := Event{
		Type:      "token.chunk",
		Tenant:    "acme",
		Project:   "support-bot",
		SessionID: "sess-42",
		AgentID:   "agent-a",
	}
	raw, err := e.Marshal()
	if err != nil {
		t.Fatalf("Marshal() err=%v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("json.Unmarshal err=%v", err)
	}
	if got["version"] != "v1" {
		t.Fatalf("version=%v want v1", got["version"])
	}
	if got["created_at"] == "" {
		t.Fatalf("created_at should be set")
	}
}

func TestMarshalValidation(t *testing.T) {
	_, err := Event{
		Type:      "token.chunk",
		Tenant:    "acme",
		Project:   "support-bot",
		SessionID: "sess-42",
	}.Marshal()
	if err == nil {
		t.Fatal("expected validation error for missing agent_id")
	}
}

func TestParseEvent(t *testing.T) {
	raw := []byte(`{"version":"v1","type":"tool.call","tenant":"acme","project":"support-bot","session_id":"sess-42","agent_id":"planner","attempt":2,"created_at":"2026-04-03T10:00:00Z","payload":{"tool":"search"}}`)
	ev, ok := ParseEvent(raw)
	if !ok {
		t.Fatal("expected ParseEvent success")
	}
	if ev.Type != "tool.call" || ev.Attempt != 2 {
		t.Fatalf("unexpected parsed event: %+v", ev)
	}
}

func TestParseEventRejectsInvalidPayload(t *testing.T) {
	if _, ok := ParseEvent([]byte(`{"type":"tool.call"}`)); ok {
		t.Fatal("expected invalid envelope to be rejected")
	}
}
