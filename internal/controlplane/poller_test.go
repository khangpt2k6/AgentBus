package controlplane

import (
	"testing"
	"time"

	"github.com/khangpt2k6/EventBus/internal/agentstream"
	"github.com/khangpt2k6/EventBus/internal/broker"
)

func publishAgentEvent(t *testing.T, b *broker.Broker, e agentstream.Event) {
	t.Helper()
	enc, err := e.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	k := agentstream.SessionKey(e.Tenant, e.Project, e.SessionID)
	if _, _, err := b.PublishWithKey("agent-events", k, enc); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

func TestPollerDrainsAgentEvents(t *testing.T) {
	b := broker.New()
	cp := New()
	p := NewPoller(b, cp, "agent-events")

	publishAgentEvent(t, b, ev("tool.call", "planner", "s1"))
	publishAgentEvent(t, b, ev("tool.result", "planner", "s1"))
	publishAgentEvent(t, b, ev("complete", "planner", "s1"))

	p.drainOnce()

	rs, ok := cp.GetSession("acme", "bot", "s1")
	if !ok {
		t.Fatal("session s1 should exist after draining")
	}
	if rs.Status != StatusCompleted {
		t.Errorf("status = %q, want completed", rs.Status)
	}
	if _, ok := cp.reg.Get("planner"); !ok {
		t.Error("planner should be registered via ingestion")
	}
}

func TestPollerIdempotent(t *testing.T) {
	b := broker.New()
	cp := New()
	p := NewPoller(b, cp, "agent-events")

	publishAgentEvent(t, b, ev("tool.call", "planner", "s1"))
	n1 := p.drainOnce()
	n2 := p.drainOnce()
	if n1 != 1 {
		t.Errorf("first drain ingested %d, want 1", n1)
	}
	if n2 != 0 {
		t.Errorf("second drain ingested %d, want 0 (cursor advanced)", n2)
	}
}

func TestPollerSkipsNonEnvelope(t *testing.T) {
	b := broker.New()
	cp := New()
	p := NewPoller(b, cp, "agent-events")

	if _, _, err := b.PublishWithKey("agent-events", "k", []byte("not an envelope")); err != nil {
		t.Fatal(err)
	}
	if n := p.drainOnce(); n != 0 {
		t.Errorf("non-envelope payloads should be skipped, ingested %d", n)
	}
}

func TestPollerStartStop(t *testing.T) {
	b := broker.New()
	cp := New()
	p := NewPoller(b, cp, "agent-events")
	p.Start()

	publishAgentEvent(t, b, ev("tool.call", "planner", "s1"))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := cp.GetSession("acme", "bot", "s1"); ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	p.Stop()

	if _, ok := cp.GetSession("acme", "bot", "s1"); !ok {
		t.Error("background poller should have ingested the published event")
	}
}
