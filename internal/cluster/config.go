// Package cluster contains the distributed-mode subsystems for AgentBus.
// It is opt-in: a broker that does not pass --cluster does not import or
// initialize any code here.
package cluster

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// Peer identifies one node in the cluster by its stable NodeID and the
// TCP address its Raft transport listens on. GossipAddr is the UDP/TCP
// address memberlist should dial to join; if empty, RaftAddr is used
// (valid only when raft and gossip share the same host:port, which is
// not the case in multi-port test setups).
type Peer struct {
	NodeID     string
	RaftAddr   string
	GossipAddr string // optional; falls back to RaftAddr when empty
}

// Config bundles every knob the cluster subsystems need. Populated from
// CLI flags in cmd/broker/main.go when --cluster is set.
type Config struct {
	NodeID     string
	RaftBind   string
	GossipBind string
	RaftDir    string
	ClientAddr string // gRPC address clients should dial; used in redirect hints
	Peers      []Peer
}

// ParsePeers reads the --peers flag value, comma-separated "id@host:port".
// Empty string is valid and returns an empty slice (single-node bootstrap).
func ParsePeers(raw string) ([]Peer, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	out := make([]Peer, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		at := strings.Index(p, "@")
		if at <= 0 || at == len(p)-1 {
			return nil, fmt.Errorf("peer %q must be of the form id@host:port", p)
		}
		id := strings.TrimSpace(p[:at])
		addr := strings.TrimSpace(p[at+1:])
		if id == "" {
			return nil, fmt.Errorf("peer %q has empty node id", p)
		}
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, fmt.Errorf("peer %q has invalid address: %w", p, err)
		}
		if _, err := strconv.Atoi(port); err != nil {
			return nil, fmt.Errorf("peer %q has non-numeric port: %w", p, err)
		}
		if host == "" {
			return nil, fmt.Errorf("peer %q has empty host", p)
		}
		out = append(out, Peer{NodeID: id, RaftAddr: addr})
	}
	return out, nil
}

// Validate checks the Config has the minimum required fields populated.
func (c Config) Validate() error {
	if strings.TrimSpace(c.NodeID) == "" {
		return fmt.Errorf("NodeID is required")
	}
	if strings.TrimSpace(c.RaftBind) == "" {
		return fmt.Errorf("RaftBind is required")
	}
	if strings.TrimSpace(c.GossipBind) == "" {
		return fmt.Errorf("GossipBind is required")
	}
	if strings.TrimSpace(c.RaftDir) == "" {
		return fmt.Errorf("RaftDir is required")
	}
	return nil
}
