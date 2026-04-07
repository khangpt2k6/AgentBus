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
