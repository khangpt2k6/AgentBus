package controlplane

import (
	"sync"
	"testing"

	"github.com/khangpt2k6/EventBus/internal/agentstream"
)

func ev(typ, agent, session string) agentstream.Event {
	return agentstream.Event{
		Type:      typ,
		Tenant:    "acme",
		Project:   "bot",
		SessionID: session,
		AgentID:   agent,
	}
}

func key(session string) string { return agentstream.SessionKey("acme", "bot", session) }

func TestSessionFirstEventCreatesRunning(t *testing.T) {
	s := NewSessionStore()
	s.Apply(ev("tool.call", "planner", "s1"), "")

	rs, ok := s.Get(key("s1"))
	if !ok {
		t.Fatal("expected session s1 to exist")
	}
	if rs.Status != StatusRunning {
		t.Errorf("status = %q, want running", rs.Status)
	}
	if rs.ActiveAgent != "planner" {
		t.Errorf("active = %q, want planner", rs.ActiveAgent)
	}
	if rs.StepCount != 1 {
		t.Errorf("steps = %d, want 1", rs.StepCount)
	}
}

func TestSessionHandoffSetsWaiting(t *testing.T) {
	s := NewSessionStore()
	s.Apply(ev("tool.call", "planner", "s1"), "")
	s.Apply(ev("handoff", "planner", "s1"), "escalator")

	rs, _ := s.Get(key("s1"))
	if rs.Status != StatusWaiting {
		t.Errorf("status = %q, want waiting", rs.Status)
	}
	if rs.PendingAgent != "escalator" {
		t.Errorf("pending = %q, want escalator", rs.PendingAgent)
	}
	if rs.ActiveAgent != "planner" {
		t.Errorf("active should still be planner until pickup, got %q", rs.ActiveAgent)
	}
}

func TestSessionPickupAfterHandoff(t *testing.T) {
	s := NewSessionStore()
	s.Apply(ev("tool.call", "planner", "s1"), "")
	s.Apply(ev("handoff", "planner", "s1"), "escalator")
	s.Apply(ev("tool.call", "escalator", "s1"), "")

	rs, _ := s.Get(key("s1"))
	if rs.ActiveAgent != "escalator" {
		t.Errorf("active = %q, want escalator after pickup", rs.ActiveAgent)
	}
	if rs.PendingAgent != "" {
		t.Errorf("pending should be cleared, got %q", rs.PendingAgent)
	}
	if rs.Status != StatusRunning {
		t.Errorf("status = %q, want running", rs.Status)
	}
}

func TestSessionErrorFails(t *testing.T) {
	s := NewSessionStore()
	s.Apply(ev("tool.call", "planner", "s1"), "")
	s.Apply(ev("error", "planner", "s1"), "")
	rs, _ := s.Get(key("s1"))
	if rs.Status != StatusFailed {
		t.Errorf("status = %q, want failed", rs.Status)
	}
}

func TestSessionCompleteCompletes(t *testing.T) {
	s := NewSessionStore()
	s.Apply(ev("tool.call", "planner", "s1"), "")
	s.Apply(ev("complete", "planner", "s1"), "")
	rs, _ := s.Get(key("s1"))
	if rs.Status != StatusCompleted {
		t.Errorf("status = %q, want completed", rs.Status)
	}
}

func TestSessionFullSequence(t *testing.T) {
	s := NewSessionStore()
	s.Apply(ev("tool.call", "planner", "s1"), "")
	s.Apply(ev("tool.result", "planner", "s1"), "")
	s.Apply(ev("handoff", "planner", "s1"), "escalator")
	s.Apply(ev("tool.call", "escalator", "s1"), "")
	s.Apply(ev("complete", "escalator", "s1"), "")

	rs, _ := s.Get(key("s1"))
	if rs.ActiveAgent != "escalator" {
		t.Errorf("active = %q, want escalator", rs.ActiveAgent)
	}
	if rs.Status != StatusCompleted {
		t.Errorf("status = %q, want completed", rs.Status)
	}
	if rs.StepCount != 5 {
		t.Errorf("steps = %d, want 5", rs.StepCount)
	}
}

func TestSessionCountByStatus(t *testing.T) {
	s := NewSessionStore()
	s.Apply(ev("tool.call", "a", "s1"), "")
	s.Apply(ev("complete", "a", "s2"), "")
	s.Apply(ev("error", "a", "s3"), "")

	counts := s.CountByStatus()
	if counts[StatusRunning] != 1 || counts[StatusCompleted] != 1 || counts[StatusFailed] != 1 {
		t.Errorf("counts = %v", counts)
	}
}

func TestSessionConcurrent(t *testing.T) {
	s := NewSessionStore()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			sess := "s" + string(rune('0'+n%5))
			s.Apply(ev("tool.call", "planner", sess), "")
			_, _ = s.Get(key(sess))
			_ = s.CountByStatus()
		}(i)
	}
	wg.Wait()
}
