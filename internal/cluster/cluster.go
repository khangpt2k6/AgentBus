package cluster

import (
	"fmt"
	"io"
	"os"

	"github.com/khangpt2k6/AgentBus/internal/cluster/membership"
	"github.com/khangpt2k6/AgentBus/internal/cluster/metadata"
)

// Cluster bundles the membership and metadata subsystems behind a single
// lifecycle. Broker main wires one of these up when --cluster is set.
type Cluster struct {
	cfg  Config
	mem  *membership.Membership
	meta *metadata.Metadata
}

// Status is a snapshot of cluster state for /readyz or `cluster status` CLI.
type Status struct {
	NodeID         string
	AliveMembers   []string
	MetadataLeader string
	IsLeader       bool
	FSMembers      map[string]string
}

// Start brings up both subsystems. On any error, partial state is cleaned up.
func Start(cfg Config, logOut io.Writer) (*Cluster, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if logOut == nil {
		logOut = os.Stderr
	}

	// Membership: build join list from cfg.Peers minus self.
	join := make([]string, 0, len(cfg.Peers))
	for _, p := range cfg.Peers {
		if p.NodeID == cfg.NodeID {
			continue
		}
		// Translate raft address host to use the gossip port. In v1 we
		// assume the same host serves both transports; production may
		// add an explicit GossipAddr per peer.
		join = append(join, p.RaftAddr)
	}
	mem, err := membership.Start(membership.Config{
		NodeID:     cfg.NodeID,
		GossipBind: cfg.GossipBind,
		JoinAddrs:  join,
	})
	if err != nil {
		return nil, fmt.Errorf("membership start: %w", err)
	}

	// Metadata: bootstrap with the full peer list (idempotent on subsequent runs).
	peers := make([]metadata.Peer, 0, len(cfg.Peers))
	for _, p := range cfg.Peers {
		peers = append(peers, metadata.Peer{NodeID: p.NodeID, Addr: p.RaftAddr})
	}
	meta, err := metadata.Start(metadata.Options{
		NodeID:        cfg.NodeID,
		BindAddr:      cfg.RaftBind,
		AdvertiseAddr: cfg.RaftBind,
		DataDir:       cfg.RaftDir,
		Bootstrap:     len(peers) > 0,
		InitialPeers:  peers,
		LogOutput:     logOut,
	})
	if err != nil {
		_ = mem.Shutdown()
		return nil, fmt.Errorf("metadata start: %w", err)
	}

	return &Cluster{cfg: cfg, mem: mem, meta: meta}, nil
}

// Membership returns the gossip subsystem (read-only handle).
func (c *Cluster) Membership() *membership.Membership { return c.mem }

// Metadata returns the Raft subsystem (read-only handle).
func (c *Cluster) Metadata() *metadata.Metadata { return c.meta }

// Status returns a snapshot of current cluster state.
func (c *Cluster) Status() Status {
	return Status{
		NodeID:         c.cfg.NodeID,
		AliveMembers:   c.mem.Alive(),
		MetadataLeader: c.meta.Leader(),
		IsLeader:       c.meta.IsLeader(),
		FSMembers:      c.meta.FSM().Members(),
	}
}

// Shutdown stops both subsystems. Errors are joined; Membership shutdown
// is best-effort even if Metadata shutdown fails.
func (c *Cluster) Shutdown() error {
	metaErr := c.meta.Shutdown()
	memErr := c.mem.Shutdown()
	switch {
	case metaErr != nil && memErr != nil:
		return fmt.Errorf("metadata: %v; membership: %v", metaErr, memErr)
	case metaErr != nil:
		return metaErr
	case memErr != nil:
		return memErr
	}
	return nil
}
