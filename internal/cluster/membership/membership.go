// Package membership wraps hashicorp/memberlist so the rest of the
// cluster code talks to a small AgentBus-specific surface instead of
// directly to memberlist's broader API.
package membership

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"

	"github.com/hashicorp/memberlist"
)

// Config is the minimum the membership subsystem needs at startup.
type Config struct {
	NodeID     string
	GossipBind string
	JoinAddrs  []string // empty = bootstrap; non-empty = join existing
}

// EventType discriminates between node lifecycle signals.
type EventType int

const (
	EventJoin EventType = iota
	EventLeave
)

// Event is delivered on Events() when membership changes.
type Event struct {
	Type   EventType
	NodeID string
	Addr   string
}

// Membership is the live handle to a running gossip cluster member.
type Membership struct {
	ml     *memberlist.Memberlist
	events chan Event

	mu   sync.RWMutex
	dead bool
}

// Start creates the local member and joins the cluster if JoinAddrs is set.
func Start(cfg Config) (*Membership, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("NodeID is required")
	}
	host, portStr, err := net.SplitHostPort(cfg.GossipBind)
	if err != nil {
		return nil, fmt.Errorf("GossipBind %q invalid: %w", cfg.GossipBind, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("GossipBind %q port not numeric: %w", cfg.GossipBind, err)
	}

	mc := memberlist.DefaultLocalConfig()
	mc.Name = cfg.NodeID
	mc.BindAddr = host
	mc.BindPort = port
	mc.AdvertiseAddr = host
	mc.AdvertisePort = port
	mc.LogOutput = io.Discard // suppress library chatter; callers use Events() instead

	m := &Membership{
		events: make(chan Event, 128),
	}
	mc.Events = &delegate{out: m.events}

	ml, err := memberlist.Create(mc)
	if err != nil {
		return nil, fmt.Errorf("memberlist create: %w", err)
	}
	m.ml = ml

	if len(cfg.JoinAddrs) > 0 {
		// ml.Join returns the number of nodes successfully contacted. If
		// zero peers are reachable yet (e.g. this is the first node up),
		// we proceed anyway — other nodes will push their state to us when
		// they join, making gossip eventually-consistent regardless of
		// start order.
		if _, err := ml.Join(cfg.JoinAddrs); err != nil {
			// Non-fatal: log and continue. The cluster will converge once
			// other nodes are up and performing their own joins.
			_ = err // caller can observe convergence via Alive()
		}
	}
	return m, nil
}

// Alive returns the NodeIDs of all currently-alive cluster members,
// including this node itself.
func (m *Membership) Alive() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.dead || m.ml == nil {
		return nil
	}
	members := m.ml.Members()
	out := make([]string, 0, len(members))
	for _, n := range members {
		out = append(out, n.Name)
	}
	return out
}

// Events returns the channel of lifecycle events. Buffered; callers must
// drain it or events are dropped.
func (m *Membership) Events() <-chan Event { return m.events }

// Shutdown leaves the cluster gracefully and stops the local listener.
func (m *Membership) Shutdown() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.dead {
		return nil
	}
	m.dead = true
	if m.ml == nil {
		return nil
	}
	// Best-effort graceful leave; ignore timeout error.
	_ = m.ml.Leave(0)
	return m.ml.Shutdown()
}

type delegate struct {
	out chan<- Event
}

func (d *delegate) NotifyJoin(n *memberlist.Node) {
	d.send(Event{Type: EventJoin, NodeID: n.Name, Addr: n.Address()})
}
func (d *delegate) NotifyLeave(n *memberlist.Node) {
	d.send(Event{Type: EventLeave, NodeID: n.Name, Addr: n.Address()})
}
func (d *delegate) NotifyUpdate(n *memberlist.Node) {} // not used in v1

func (d *delegate) send(ev Event) {
	select {
	case d.out <- ev:
	default:
		// Channel full; drop. Production code may want a counter here.
	}
}
