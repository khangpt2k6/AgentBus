package agentbus

import (
	"encoding/json"
	"testing"
)

func TestDecodeEventRoundTrip(t *testing.T) {
	encoded, err := encodeAgentEvent(AgentEvent{
		Tenant:    "acme",
		Project:   "bot",
		SessionID: "sess-1",
		AgentID:   "planner",
		Type:      EventTypeToolCall,
		Step:      "search",
		Attempt:   2,
		Payload:   json.RawMessage(`{"tool":"web"}`),
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	ev, ok := DecodeEvent(encoded)
	if !ok {
		t.Fatal("decode failed")
	}
	if ev.Type != EventTypeToolCall {
		t.Errorf("type: %q", ev.Type)
	}
	if ev.Tenant != "acme" || ev.SessionID != "sess-1" || ev.AgentID != "planner" {
		t.Errorf("identity fields wrong: %+v", ev)
	}
	if ev.Step != "search" || ev.Attempt != 2 {
		t.Errorf("step/attempt: %s / %d", ev.Step, ev.Attempt)
	}
	if string(ev.Payload) != `{"tool":"web"}` {
		t.Errorf("payload: %s", ev.Payload)
	}
}

func TestDecodeEventRejectsNonEnvelope(t *testing.T) {
	cases := [][]byte{
		[]byte(`{}`),
		[]byte(`{"hello":"world"}`),
		[]byte(`not json`),
		[]byte(``),
	}
	for _, c := range cases {
		if _, ok := DecodeEvent(c); ok {
			t.Errorf("should reject %q", c)
		}
	}
}

func TestSessionMatchesUsesTriple(t *testing.T) {
	ev := DecodedEvent{Tenant: "acme", Project: "bot", SessionID: "s1"}
	if !sessionMatches(ev, SessionRef{Tenant: "acme", Project: "bot", SessionID: "s1"}) {
		t.Error("should match identical triple")
	}
	if sessionMatches(ev, SessionRef{Tenant: "acme", Project: "bot", SessionID: "s2"}) {
		t.Error("should NOT match different session")
	}
	if sessionMatches(ev, SessionRef{Tenant: "other", Project: "bot", SessionID: "s1"}) {
		t.Error("should NOT match different tenant")
	}
}

func TestResolveSessionRoutingDeterministic(t *testing.T) {
	c := &Client{}
	sess := SessionRef{Tenant: "acme", Project: "bot", SessionID: "sess-42"}
	t1, p1, err := c.resolveSessionRouting(sess, "", 0)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	t2, p2, _ := c.resolveSessionRouting(sess, "", 0)
	if t1 != t2 || p1 != p2 {
		t.Errorf("non-deterministic: (%s,%d) vs (%s,%d)", t1, p1, t2, p2)
	}
	if t1 != DefaultAgentTopic {
		t.Errorf("default topic: %s", t1)
	}
	if p1 < 0 || p1 >= 3 {
		t.Errorf("partition out of [0,3): %d", p1)
	}
}

func TestResolveSessionRoutingRequiresTriple(t *testing.T) {
	c := &Client{}
	cases := []SessionRef{
		{Project: "bot", SessionID: "s"},
		{Tenant: "acme", SessionID: "s"},
		{Tenant: "acme", Project: "bot"},
	}
	for _, s := range cases {
		if _, _, err := c.resolveSessionRouting(s, "", 0); err == nil {
			t.Errorf("should error for %+v", s)
		}
	}
}
