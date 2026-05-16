package agentbus

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestEncodeAgentEventFillsDefaults(t *testing.T) {
	encoded, err := encodeAgentEvent(AgentEvent{
		Tenant:    "acme",
		Project:   "support-bot",
		SessionID: "sess-42",
		AgentID:   "planner",
		Type:      "tool.call",
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var env envelope
	if err := json.Unmarshal(encoded, &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if env.Version != "v1" {
		t.Errorf("version: got %q, want v1", env.Version)
	}
	if env.CreatedAt == "" {
		t.Error("created_at should be filled in")
	}
	if _, err := time.Parse(time.RFC3339Nano, env.CreatedAt); err != nil {
		t.Errorf("created_at not RFC3339Nano: %v", err)
	}
	if string(env.Payload) != "{}" {
		t.Errorf("default payload: got %s, want {}", env.Payload)
	}
}

func TestEncodeAgentEventRejectsInvalidJSONPayload(t *testing.T) {
	_, err := encodeAgentEvent(AgentEvent{
		Tenant:    "acme",
		Project:   "x",
		SessionID: "s",
		AgentID:   "a",
		Type:      "t",
		Payload:   []byte(`not json {{`),
	})
	if err == nil {
		t.Fatal("expected error for invalid JSON payload")
	}
}

func TestValidateAgentEventRequiresFields(t *testing.T) {
	cases := []struct {
		name string
		ev   AgentEvent
	}{
		{"missing type", AgentEvent{Tenant: "a", Project: "b", SessionID: "c", AgentID: "d"}},
		{"missing tenant", AgentEvent{Type: "t", Project: "b", SessionID: "c", AgentID: "d"}},
		{"missing project", AgentEvent{Type: "t", Tenant: "a", SessionID: "c", AgentID: "d"}},
		{"missing session", AgentEvent{Type: "t", Tenant: "a", Project: "b", AgentID: "d"}},
		{"missing agent", AgentEvent{Type: "t", Tenant: "a", Project: "b", SessionID: "c"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateAgentEvent(tc.ev); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestSessionKeyIsStable(t *testing.T) {
	a := sessionKey("acme", "bot", "sess-1")
	b := sessionKey(" acme ", "bot", "sess-1")
	if a != b {
		t.Errorf("whitespace should be trimmed: %q vs %q", a, b)
	}
	if !strings.Contains(a, "sess-1") {
		t.Errorf("key should contain session id: %q", a)
	}
}
