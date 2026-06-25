package controlplane

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestRegistryRegisterAndGet(t *testing.T) {
	r := NewRegistry(time.Minute)
	r.Register("planner", []string{"search", "plan"})

	got, ok := r.Get("planner")
	if !ok {
		t.Fatal("expected planner to be registered")
	}
	if got.Status != AgentIdle {
		t.Errorf("new agent status = %q, want %q", got.Status, AgentIdle)
	}
	if len(got.Capabilities) != 2 || got.Capabilities[0] != "search" {
		t.Errorf("capabilities = %v, want [search plan]", got.Capabilities)
	}
}

func TestRegistryHeartbeat(t *testing.T) {
	r := NewRegistry(time.Minute)
	if r.Heartbeat("ghost") {
		t.Error("Heartbeat on unknown agent should return false")
	}

	r.Register("planner", nil)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	r.now = func() time.Time { return base }
	r.Register("planner", nil) // re-register stamps LastSeen at base

	later := base.Add(10 * time.Second)
	r.now = func() time.Time { return later }
	if !r.Heartbeat("planner") {
		t.Fatal("Heartbeat on known agent should return true")
	}
	got, _ := r.Get("planner")
	if !got.LastSeen.Equal(later) {
		t.Errorf("LastSeen = %v, want %v", got.LastSeen, later)
	}
}

func TestRegistryTouch(t *testing.T) {
	r := NewRegistry(time.Minute)
	r.Register("planner", nil)
	r.Touch("planner", "acme/bot/sess-1")

	got, _ := r.Get("planner")
	if got.Status != AgentBusy {
		t.Errorf("status after Touch = %q, want %q", got.Status, AgentBusy)
	}
	if got.CurrentSession != "acme/bot/sess-1" {
		t.Errorf("current session = %q", got.CurrentSession)
	}
}

func TestRegistryTouchAutoRegisters(t *testing.T) {
	r := NewRegistry(time.Minute)
	r.Touch("late-agent", "acme/bot/s")
	if _, ok := r.Get("late-agent"); !ok {
		t.Error("Touch on unknown agent should auto-register it")
	}
}

func TestRegistrySweepStale(t *testing.T) {
	r := NewRegistry(30 * time.Second)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	r.now = func() time.Time { return base }
	r.Register("planner", nil)

	r.now = func() time.Time { return base.Add(45 * time.Second) }
	r.SweepStale()

	got, _ := r.Get("planner")
	if got.Status != AgentOffline {
		t.Errorf("status after stale sweep = %q, want %q", got.Status, AgentOffline)
	}
}

func TestRegistrySweepKeepsFresh(t *testing.T) {
	r := NewRegistry(30 * time.Second)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	r.now = func() time.Time { return base }
	r.Register("planner", nil)

	r.now = func() time.Time { return base.Add(10 * time.Second) }
	r.SweepStale()
	got, _ := r.Get("planner")
	if got.Status == AgentOffline {
		t.Error("fresh agent should not be swept offline")
	}
}

func TestRegistryDeregister(t *testing.T) {
	r := NewRegistry(time.Minute)
	r.Register("planner", nil)
	r.Deregister("planner")
	if _, ok := r.Get("planner"); ok {
		t.Error("agent should be gone after Deregister")
	}
}

func TestRegistryConcurrent(t *testing.T) {
	r := NewRegistry(time.Minute)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			id := fmt.Sprintf("agent-%d", n%8)
			r.Register(id, nil)
			r.Heartbeat(id)
			r.Touch(id, "s")
			r.SweepStale()
			_ = r.List()
		}(i)
	}
	wg.Wait()
	if len(r.List()) == 0 {
		t.Error("expected some agents registered")
	}
}
