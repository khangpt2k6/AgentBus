package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/khangpt2k6/AgentBus/internal/cluster/assigner"
	"github.com/khangpt2k6/AgentBus/internal/cluster/membership"
	"github.com/khangpt2k6/AgentBus/internal/cluster/metadata"
	"github.com/khangpt2k6/AgentBus/internal/cluster/replicator"
	"github.com/khangpt2k6/AgentBus/internal/cluster/ring"
	"github.com/khangpt2k6/AgentBus/internal/cluster/router"
	"github.com/khangpt2k6/AgentBus/internal/cluster/shardwal"
	"github.com/khangpt2k6/AgentBus/internal/cluster/transport"
)

// Default shard count when the assigner first bootstraps a cluster.
// Picked so 3-5 node deployments distribute work, while remaining cheap
// to track in the FSM.
const defaultShardCount = 32

// Cluster bundles the membership and metadata subsystems behind a single
// lifecycle, plus the M3 router/assigner used to route session traffic.
type Cluster struct {
	cfg    Config
	mem    *membership.Membership
	meta   *metadata.Metadata
	ring   *ring.Ring
	router *router.Router

	shardwalMgr *shardwal.Manager
	transport   *transport.Server
	replicator  *replicator.Replicator

	cancel context.CancelFunc
}

// Status is a snapshot of cluster state for /readyz or `cluster status` CLI.
type Status struct {
	NodeID         string
	AliveMembers   []string
	MetadataLeader string
	IsLeader       bool
	Role           string
	Term           uint64
	ShardCount     uint32
	FSMembers      map[string]string
}

// Start brings up both subsystems and the M3 routing layer. On any error,
// partial state is cleaned up.
func Start(cfg Config, logOut io.Writer) (*Cluster, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if logOut == nil {
		logOut = os.Stderr
	}

	join := make([]string, 0, len(cfg.Peers))
	for _, p := range cfg.Peers {
		if p.NodeID == cfg.NodeID {
			continue
		}
		addr := p.GossipAddr
		if addr == "" {
			addr = p.RaftAddr
		}
		join = append(join, addr)
	}
	mem, err := membership.Start(membership.Config{
		NodeID:     cfg.NodeID,
		GossipBind: cfg.GossipBind,
		JoinAddrs:  join,
		LogOutput:  logOut,
	})
	if err != nil {
		return nil, fmt.Errorf("membership start: %w", err)
	}

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

	shardwalMgr, err := shardwal.NewManager(cfg.ShardWALDir, cfg.NodeID)
	if err != nil {
		_ = meta.Shutdown()
		_ = mem.Shutdown()
		return nil, fmt.Errorf("shardwal manager: %w", err)
	}
	trSrv := transport.NewServer(shardwalMgr)
	rep := replicator.New(shardwalMgr)

	r := ring.New(128)
	rt := router.New(cfg.NodeID, meta.FSM(), aliveAdapter{mem: mem}, r)
	ctx, cancel := context.WithCancel(context.Background())

	c := &Cluster{
		cfg:         cfg,
		mem:         mem,
		meta:        meta,
		ring:        r,
		router:      rt,
		shardwalMgr: shardwalMgr,
		transport:   trSrv,
		replicator:  rep,
		cancel:      cancel,
	}

	go c.bootstrapAndAssign(ctx, logOut)
	return c, nil
}

// bootstrapAndAssign performs one-time self-registration with the FSM,
// keeps the ring in sync, and runs the assigner loop until shutdown.
func (c *Cluster) bootstrapAndAssign(ctx context.Context, logOut io.Writer) {
	// Wait for some leader to emerge before trying to Apply.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if c.meta.Leader() != "" {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}

	// Register ourselves. Only the leader's Apply succeeds; followers' Apply
	// returns ErrNotLeader and we retry on the next periodic pass below.
	c.registerSelf()

	// Bootstrap shard count once, on the leader.
	if c.meta.IsLeader() && c.meta.FSM().ShardCount() == 0 {
		_ = c.applyCmd(metadata.Command{Op: metadata.OpSetShardCount, Shard: defaultShardCount})
	}

	// If we are the leader, ensure all peers with a known ClientAddr are
	// registered in the FSM. Peers that haven't applied their own
	// OpRegisterMember yet (followers can't Apply) will be registered here.
	if c.meta.IsLeader() {
		for _, p := range c.cfg.Peers {
			if p.NodeID == c.cfg.NodeID || p.ClientAddr == "" {
				continue
			}
			_ = c.applyCmd(metadata.Command{
				Op:         metadata.OpRegisterMember,
				NodeID:     p.NodeID,
				Addr:       p.RaftAddr,
				ClientAddr: p.ClientAddr,
			})
		}
	}

	go c.refreshRingLoop(ctx)
	go c.retryRegisterLoop(ctx)
	go c.refreshShardLeadershipLoop(ctx)

	assigner.RunLoop(ctx, leaderChecker{m: c.meta}, c.meta.FSM(), aliveAdapter{mem: c.mem}, applierAdapter{m: c.meta})
	_ = logOut
}

func (c *Cluster) registerSelf() {
	_ = c.applyCmd(metadata.Command{
		Op:         metadata.OpRegisterMember,
		NodeID:     c.cfg.NodeID,
		Addr:       c.cfg.RaftBind,
		ClientAddr: c.cfg.ClientAddr,
	})
}

// retryRegisterLoop re-attempts self-registration every 2s until the FSM
// has a record for us. This covers the case where the first attempt was
// made before this node knew who the leader was.
func (c *Cluster) retryRegisterLoop(ctx context.Context) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, ok := c.meta.FSM().MemberAt(c.cfg.NodeID); ok {
				return // we're registered; nothing left to do
			}
			c.registerSelf()
		}
	}
}

func (c *Cluster) refreshRingLoop(ctx context.Context) {
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.refreshRingOnce()
		}
	}
}

// refreshShardLeadershipLoop watches the FSM and tells the Replicator
// which shards this node leads. Followers for each led shard = all alive
// members in the FSM minus self.
func (c *Cluster) refreshShardLeadershipLoop(ctx context.Context) {
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()
	owned := map[uint32]struct{}{}
	// Track the last follower set we started workers with, keyed by their
	// addresses joined. Workers are only (re)started when the set changes.
	lastFollowerKey := ""
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			leaders := c.meta.FSM().AllShardLeaders()
			want := map[uint32]struct{}{}
			for shard, leader := range leaders {
				if leader == c.cfg.NodeID {
					want[shard] = struct{}{}
				}
			}
			// Stop replicating shards we no longer lead.
			for shard := range owned {
				if _, ok := want[shard]; !ok {
					c.replicator.Drop(shard)
					delete(owned, shard)
				}
			}
			// Compute current follower set.
			followerList := c.aliveFollowers()
			addrs := c.followerAddrsFor(followerList)
			followerKey := followerSetKey(addrs)
			// (Re)start replication only for newly owned shards, or for all
			// owned shards when the follower set has changed (e.g. a node
			// joined or left). This avoids tearing down healthy streams on
			// every tick.
			followerChanged := followerKey != lastFollowerKey
			if followerChanged {
				lastFollowerKey = followerKey
			}
			for shard := range want {
				_, alreadyOwned := owned[shard]
				if !alreadyOwned || followerChanged {
					c.replicator.Add(ctx, shard, addrs)
				}
				owned[shard] = struct{}{}
			}
		}
	}
}

// followerSetKey returns a stable string key for a follower address set,
// used to detect when the set changes and workers need to be restarted.
func followerSetKey(addrs []replicator.FollowerAddr) string {
	if len(addrs) == 0 {
		return ""
	}
	key := ""
	for _, a := range addrs {
		key += a.NodeID + "=" + a.Addr + ";"
	}
	return key
}

// aliveFollowers returns NodeIDs of currently-alive members other than self.
func (c *Cluster) aliveFollowers() []string {
	out := []string{}
	for _, nid := range c.mem.Alive() {
		if nid != c.cfg.NodeID {
			out = append(out, nid)
		}
	}
	return out
}

// followerAddrsFor maps NodeIDs to (NodeID, ClientAddr) tuples. NodeIDs
// without a FSM record or empty ClientAddr are skipped.
func (c *Cluster) followerAddrsFor(nodeIDs []string) []replicator.FollowerAddr {
	out := make([]replicator.FollowerAddr, 0, len(nodeIDs))
	for _, nid := range nodeIDs {
		m, ok := c.meta.FSM().MemberAt(nid)
		if !ok || m.ClientAddr == "" {
			continue
		}
		out = append(out, replicator.FollowerAddr{NodeID: nid, Addr: m.ClientAddr})
	}
	return out
}

func (c *Cluster) refreshRingOnce() {
	want := map[string]struct{}{}
	for nid := range c.meta.FSM().Members() {
		want[nid] = struct{}{}
	}
	have := map[string]struct{}{}
	for _, n := range c.ring.Members() {
		have[n] = struct{}{}
	}
	for n := range want {
		if _, ok := have[n]; !ok {
			c.ring.AddNode(n)
		}
	}
	for n := range have {
		if _, ok := want[n]; !ok {
			c.ring.RemoveNode(n)
		}
	}
}

func (c *Cluster) applyCmd(cmd metadata.Command) error {
	b, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	return c.meta.Apply(b, 2*time.Second)
}

// Router returns the read-side routing facade.
func (c *Cluster) Router() *router.Router { return c.router }

// Membership returns the gossip subsystem (read-only handle).
func (c *Cluster) Membership() *membership.Membership { return c.mem }

// Metadata returns the Raft subsystem (read-only handle).
func (c *Cluster) Metadata() *metadata.Metadata { return c.meta }

// ShardWAL returns the shardwal Manager. Used by gRPC PublishAgent to
// write incoming agent events to the right shard.
func (c *Cluster) ShardWAL() *shardwal.Manager { return c.shardwalMgr }

// TransportServer returns the inter-node gRPC handler. cmd/broker
// registers it alongside the existing BrokerService on the main gRPC
// listener.
func (c *Cluster) TransportServer() *transport.Server { return c.transport }

// Status returns a snapshot of current cluster state.
func (c *Cluster) Status() Status {
	return Status{
		NodeID:         c.cfg.NodeID,
		AliveMembers:   c.mem.Alive(),
		MetadataLeader: c.meta.Leader(),
		IsLeader:       c.meta.IsLeader(),
		Role:           c.meta.State(),
		Term:           c.meta.Term(),
		ShardCount:     c.meta.FSM().ShardCount(),
		FSMembers:      c.meta.FSM().Members(),
	}
}

// Shutdown stops the assigner goroutine and both subsystems.
func (c *Cluster) Shutdown() error {
	if c.cancel != nil {
		c.cancel()
	}
	if c.replicator != nil {
		c.replicator.Close()
	}
	if c.shardwalMgr != nil {
		_ = c.shardwalMgr.Close()
	}
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

// --- adapters bridging cluster subsystems to assigner/router interfaces ---

type aliveAdapter struct{ mem *membership.Membership }

func (a aliveAdapter) IsAlive(nodeID string) bool {
	_, ok := a.mem.Member(nodeID)
	return ok
}
func (a aliveAdapter) AliveMembers() []string { return a.mem.Alive() }

type leaderChecker struct{ m *metadata.Metadata }

func (l leaderChecker) IsLeader() bool { return l.m.IsLeader() }

type applierAdapter struct{ m *metadata.Metadata }

func (a applierAdapter) Apply(c metadata.Command) error {
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return a.m.Apply(b, 2*time.Second)
}
