package controlplane

import (
	"testing"
	"time"

	"github.com/khangpt2k6/EventBus/internal/agentstream"
)

type fakeMetrics struct {
	routed, unrouted, active, escalations int
	sessions                             map[string]int
}

func (f *fakeMetrics) SetActiveAgents(n int) { f.active = n }
func (f *fakeMetrics) SetSessions(status string, n int) {
	if f.sessions == nil {
		f.sessions = map[string]int{}
	}
	f.sessions[status] = n
}
func (f *fakeMetrics) IncHandoffRouted()   { f.routed++ }
func (f *fakeMetrics) IncHandoffUnrouted() { f.unrouted++ }
func (f *fakeMetrics) IncEscalation()      { f.escalations++ }

func handoffEv(from, to, session string) agentstream.Event {
	e := ev("handoff", from, session)
	e.Payload = []byte(`{"from_agent":"` + from + `","to_agent":"` + to + `"}`)
	return e
}

func TestIngestUpdatesRegistryAndSession(t *testing.T) {
	cp := New()
	cp.Ingest(ev("tool.call", "planner", "s1"))

	a, ok := cp.reg.Get("planner")
	if !ok || a.Status != EventBusy {
		t.Errorf("planner not marked busy after ingest: %+v ok=%v", a, ok)
	}
	rs, ok := cp.GetSession("acme", "bot", "s1")
	if !ok || rs.Status != StatusRunning || rs.ActiveAgent != "planner" {
		t.Errorf("session not running under planner: %+v ok=%v", rs, ok)
	}
}

func TestIngestHandoffRoutedToOpenInbox(t *testing.T) {
	m := &fakeMetrics{}
	cp := New(WithMetrics(m))
	cp.Register("escalator", nil)
	ch := cp.OpenInbox("escalator")

	cp.Ingest(ev("tool.call", "planner", "s1"))
	cp.Ingest(handoffEv("planner", "escalator", "s1"))

	select {
	case re := <-ch:
		if re.Type != "handoff" {
			t.Errorf("inbox got %+v", re)
		}
	case <-time.After(time.Second):
		t.Fatal("expected handoff delivered to escalator inbox")
	}
	if m.routed != 1 {
		t.Errorf("IncHandoffRouted = %d, want 1", m.routed)
	}
	rs, _ := cp.GetSession("acme", "bot", "s1")
	if rs.Status != StatusWaiting || rs.PendingAgent != "escalator" {
		t.Errorf("session = %+v, want waiting/escalator", rs)
	}
}

func TestIngestHandoffUnroutedUnknownAgent(t *testing.T) {
	m := &fakeMetrics{}
	cp := New(WithMetrics(m))
	cp.Ingest(ev("tool.call", "planner", "s1"))
	cp.Ingest(handoffEv("planner", "ghost", "s1")) // ghost has no open inbox

	if m.unrouted != 1 {
		t.Errorf("IncHandoffUnrouted = %d, want 1", m.unrouted)
	}
	if m.routed != 0 {
		t.Errorf("IncHandoffRouted = %d, want 0", m.routed)
	}
	rs, _ := cp.GetSession("acme", "bot", "s1")
	if rs.Status != StatusWaiting || rs.PendingAgent != "ghost" {
		t.Errorf("session should still record the handoff intent: %+v", rs)
	}
}

func TestFacadeRegisterListAndHeartbeat(t *testing.T) {
	cp := New()
	cp.Register("planner", []string{"plan"})
	if !cp.Heartbeat("planner") {
		t.Error("heartbeat on registered agent should be true")
	}
	agents := cp.ListAgents()
	if len(agents) != 1 || agents[0].ID != "planner" {
		t.Errorf("ListAgents = %+v", agents)
	}
	cp.Deregister("planner")
	if len(cp.ListAgents()) != 0 {
		t.Error("agent should be gone after deregister")
	}
}

func TestSweepRefreshesMetrics(t *testing.T) {
	m := &fakeMetrics{}
	cp := New(WithMetrics(m))
	cp.Register("planner", nil)
	cp.Ingest(ev("tool.call", "planner", "s1"))
	cp.Sweep()
	if m.active == 0 {
		t.Error("Sweep should publish active-agent count to metrics")
	}
	if m.sessions[string(StatusRunning)] != 1 {
		t.Errorf("Sweep should publish session status counts, got %+v", m.sessions)
	}
}
