// Package controlplane adds an in-memory agent control-plane layer on top of the
// event bus: an agent registry, a per-session run-state machine, and handoff
// routing. It is a read-side consumer of the agent-events stream and never sits
// on the publish hot path.
package controlplane

import (
	"sort"
	"sync"
	"time"
)

// AgentStatus is the liveness/activity state the control plane tracks per agent.
type AgentStatus string

const (
	AgentIdle    AgentStatus = "idle"    // registered, no active session
	EventBusy    AgentStatus = "busy"    // currently producing events
	AgentOffline AgentStatus = "offline" // no heartbeat/activity within the TTL
)

// AgentInfo is a snapshot of one registered agent.
type AgentInfo struct {
	ID             string
	Status         AgentStatus
	Capabilities   []string
	CurrentSession string
	LastSeen       time.Time
}

// Registry is a concurrency-safe in-memory map of known agents. It is the
// "who is out there" half of the control plane.
type Registry struct {
	mu     sync.RWMutex
	agents map[string]*AgentInfo
	ttl    time.Duration
	now    func() time.Time
}

// NewRegistry returns a registry that marks agents offline once they have not
// been seen for ttl.
func NewRegistry(ttl time.Duration) *Registry {
	return &Registry{
		agents: make(map[string]*AgentInfo),
		ttl:    ttl,
		now:    time.Now,
	}
}

// Register adds or refreshes an agent, stamping it idle and seen-now.
func (r *Registry) Register(id string, caps []string) {
	if id == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	a := r.agents[id]
	if a == nil {
		a = &AgentInfo{ID: id, Status: AgentIdle}
		r.agents[id] = a
	}
	if caps != nil {
		a.Capabilities = caps
	}
	if a.Status == AgentOffline {
		a.Status = AgentIdle
	}
	a.LastSeen = r.now()
}

// Heartbeat refreshes LastSeen for a known agent. Returns false if the agent is
// unknown.
func (r *Registry) Heartbeat(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	a := r.agents[id]
	if a == nil {
		return false
	}
	a.LastSeen = r.now()
	if a.Status == AgentOffline {
		a.Status = AgentIdle
	}
	return true
}

// Touch is called by ingestion when an agent produces an event: it marks the
// agent busy on the given session and seen-now, auto-registering if needed.
func (r *Registry) Touch(id, session string) {
	if id == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	a := r.agents[id]
	if a == nil {
		a = &AgentInfo{ID: id}
		r.agents[id] = a
	}
	a.Status = EventBusy
	a.CurrentSession = session
	a.LastSeen = r.now()
}

// Deregister removes an agent.
func (r *Registry) Deregister(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.agents, id)
}

// Get returns a snapshot of one agent.
func (r *Registry) Get(id string) (AgentInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	a := r.agents[id]
	if a == nil {
		return AgentInfo{}, false
	}
	return *a, true
}

// List returns a snapshot of all agents, sorted by ID for stable output.
func (r *Registry) List() []AgentInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]AgentInfo, 0, len(r.agents))
	for _, a := range r.agents {
		out = append(out, *a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// SweepStale marks any agent offline whose LastSeen is older than the TTL.
func (r *Registry) SweepStale() {
	r.mu.Lock()
	defer r.mu.Unlock()
	cutoff := r.now().Add(-r.ttl)
	for _, a := range r.agents {
		if a.Status != AgentOffline && a.LastSeen.Before(cutoff) {
			a.Status = AgentOffline
		}
	}
}

// ActiveCount returns the number of agents not marked offline.
func (r *Registry) ActiveCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n := 0
	for _, a := range r.agents {
		if a.Status != AgentOffline {
			n++
		}
	}
	return n
}
