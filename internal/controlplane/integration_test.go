package controlplane

import (
	"testing"
	"time"

	"github.com/khangpt2k6/EventBus/internal/broker"
)

// TestEndToEndMultiAgentRun drives a realistic multi-agent run through a real
// in-process broker and the control-plane poller, then asserts the control
// plane tracked the handoff, switched the active agent, and reached completion.
func TestEndToEndMultiAgentRun(t *testing.T) {
	b := broker.New()
	cp := New()
	poller := NewPoller(b, cp, "agent-events")

	// An agent connects and opens its inbox before any traffic.
	cp.Register("escalator", []string{"escalate"})
	inbox := cp.OpenInbox("escalator")

	// A planner works, hands off to the escalator, which finishes the run.
	publishAgentEvent(t, b, ev("tool.call", "planner", "s1"))
	publishAgentEvent(t, b, handoffEv("planner", "escalator", "s1"))
	publishAgentEvent(t, b, ev("tool.call", "escalator", "s1"))
	publishAgentEvent(t, b, ev("complete", "escalator", "s1"))

	poller.drainOnce()

	// Session run-state: escalator took over and the run completed.
	rs, ok := cp.GetSession("acme", "bot", "s1")
	if !ok {
		t.Fatal("session s1 should be tracked")
	}
	if rs.ActiveAgent != "escalator" {
		t.Errorf("active agent = %q, want escalator (handoff pickup)", rs.ActiveAgent)
	}
	if rs.Status != StatusCompleted {
		t.Errorf("status = %q, want completed", rs.Status)
	}
	if rs.StepCount != 4 {
		t.Errorf("step count = %d, want 4", rs.StepCount)
	}

	// The handoff was actually routed to the escalator's inbox.
	select {
	case re := <-inbox:
		if re.Type != "handoff" {
			t.Errorf("inbox event = %+v, want handoff", re)
		}
	case <-time.After(time.Second):
		t.Fatal("escalator inbox should have received the routed handoff")
	}

	// Both agents are known to the registry (escalator registered, planner via
	// ingestion).
	agents := cp.ListAgents()
	if len(agents) != 2 {
		t.Fatalf("ListAgents = %d agents, want 2 (%+v)", len(agents), agents)
	}
	seen := map[string]bool{}
	for _, a := range agents {
		seen[a.ID] = true
	}
	if !seen["planner"] || !seen["escalator"] {
		t.Errorf("expected planner and escalator, got %+v", agents)
	}
}
