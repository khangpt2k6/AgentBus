package controlplane

import (
	"testing"
	"time"
)

func TestEscalationOnTerminalErrorRoutes(t *testing.T) {
	m := &fakeMetrics{}
	cp := New(WithMetrics(m), WithEscalationAgent("oncall"))
	cp.Register("oncall", nil)
	inbox := cp.OpenInbox("oncall")

	cp.Ingest(ev("tool.call", "planner", "s1"))
	cp.Ingest(ev("error", "planner", "s1"))

	select {
	case re := <-inbox:
		if re.Type != "escalation" {
			t.Errorf("inbox event = %+v, want escalation", re)
		}
	case <-time.After(time.Second):
		t.Fatal("escalation should be delivered to the oncall inbox")
	}
	if m.escalations != 1 {
		t.Errorf("IncEscalation = %d, want 1", m.escalations)
	}

	rs, _ := cp.GetSession("acme", "bot", "s1")
	if rs.Status != StatusWaiting || rs.PendingAgent != "oncall" {
		t.Errorf("session = %+v, want waiting on oncall", rs)
	}
}

func TestEscalationPickupByOncall(t *testing.T) {
	cp := New(WithEscalationAgent("oncall"))
	cp.Register("oncall", nil)
	cp.OpenInbox("oncall")

	cp.Ingest(ev("tool.call", "planner", "s1"))
	cp.Ingest(ev("error", "planner", "s1"))
	cp.Ingest(ev("tool.call", "oncall", "s1")) // oncall takes over

	rs, _ := cp.GetSession("acme", "bot", "s1")
	if rs.ActiveAgent != "oncall" {
		t.Errorf("active = %q, want oncall after pickup", rs.ActiveAgent)
	}
	if rs.Status != StatusRunning {
		t.Errorf("status = %q, want running", rs.Status)
	}
}

func TestNoEscalationWhenDisabled(t *testing.T) {
	m := &fakeMetrics{}
	cp := New(WithMetrics(m)) // no escalation agent configured
	cp.Ingest(ev("tool.call", "planner", "s1"))
	cp.Ingest(ev("error", "planner", "s1"))

	if m.escalations != 0 {
		t.Errorf("escalations = %d, want 0 when disabled", m.escalations)
	}
	rs, _ := cp.GetSession("acme", "bot", "s1")
	if rs.Status != StatusFailed {
		t.Errorf("status = %q, want failed (no escalation)", rs.Status)
	}
}

func TestNoSelfEscalation(t *testing.T) {
	m := &fakeMetrics{}
	cp := New(WithMetrics(m), WithEscalationAgent("oncall"))
	// the oncall agent itself errors; do not escalate to itself
	cp.Ingest(ev("error", "oncall", "s1"))
	if m.escalations != 0 {
		t.Errorf("escalations = %d, want 0 (no self-escalation)", m.escalations)
	}
}
