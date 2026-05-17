package router

import (
	"testing"

	"github.com/khangpt2k6/AgentBus/internal/cluster/metadata"
	"github.com/khangpt2k6/AgentBus/internal/cluster/ring"
)

// fakeMembership lets tests assert which nodes the router considers alive
// without spinning up real gossip.
type fakeMembership struct{ alive map[string]bool }

func (f *fakeMembership) IsAlive(nodeID string) bool { return f.alive[nodeID] }

func newFSMWith(t *testing.T, shardCount uint32, members map[string]string, leaders map[uint32]string) *metadata.FSM {
	t.Helper()
	f := metadata.NewFSM()
	helper := newApplyHelper(t)
	helper.apply(f, metadata.Command{Op: metadata.OpSetShardCount, Shard: shardCount})
	for nid, clientAddr := range members {
		helper.apply(f, metadata.Command{
			Op:         metadata.OpRegisterMember,
			NodeID:     nid,
			Addr:       "raft://" + nid,
			ClientAddr: clientAddr,
		})
	}
	for s, nid := range leaders {
		helper.apply(f, metadata.Command{Op: metadata.OpSetShardLeader, Shard: s, NodeID: nid})
	}
	return f
}

func TestRouter_LocalOrRedirectIsConsistent(t *testing.T) {
	f := newFSMWith(t, 32,
		map[string]string{"n1": "n1:9095", "n2": "n2:9095"},
		map[uint32]string{0: "n1", 1: "n2"},
	)
	r := New("n1", f, &fakeMembership{alive: map[string]bool{"n1": true, "n2": true}}, freshRing("n1", "n2"))

	dec := r.RouteSession("acme", "support", "sessA")
	if dec.ShardID >= 32 {
		t.Errorf("ShardID = %d out of range", dec.ShardID)
	}
	// IsLocal must be consistent with LeaderNodeID.
	if dec.LeaderNodeID == "n1" && !dec.IsLocal {
		t.Errorf("LeaderNodeID=n1 but IsLocal=false")
	}
	if dec.LeaderNodeID == "n2" && dec.IsLocal {
		t.Errorf("LeaderNodeID=n2 but IsLocal=true (self is n1)")
	}
}

func TestRouter_RedirectIncludesClientAddr(t *testing.T) {
	f := newFSMWith(t, 4,
		map[string]string{"n1": "n1:9095", "n2": "n2:9095"},
		map[uint32]string{0: "n2", 1: "n2", 2: "n2", 3: "n2"}, // pin all shards to n2
	)
	r := New("n1", f, &fakeMembership{alive: map[string]bool{"n1": true, "n2": true}}, freshRing("n1", "n2"))

	dec := r.RouteSession("acme", "support", "any-session")
	if dec.IsLocal {
		t.Fatalf("expected redirect to n2, got local")
	}
	if dec.LeaderNodeID != "n2" {
		t.Errorf("LeaderNodeID = %q, want n2", dec.LeaderNodeID)
	}
	if dec.LeaderClientAddr != "n2:9095" {
		t.Errorf("LeaderClientAddr = %q, want n2:9095", dec.LeaderClientAddr)
	}
}

func TestRouter_NoLeaderWhenUnassigned(t *testing.T) {
	f := newFSMWith(t, 4,
		map[string]string{"n1": "n1:9095"},
		map[uint32]string{},
	)
	r := New("n1", f, &fakeMembership{alive: map[string]bool{"n1": true}}, freshRing("n1"))

	dec := r.RouteSession("acme", "support", "sessA")
	if dec.LeaderNodeID != "" {
		t.Errorf("expected empty LeaderNodeID for unassigned shard, got %q", dec.LeaderNodeID)
	}
	if dec.IsLocal {
		t.Errorf("expected IsLocal=false for unassigned shard")
	}
}

func TestRouter_DeadLeaderProducesEmptyLeader(t *testing.T) {
	f := newFSMWith(t, 4,
		map[string]string{"n1": "n1:9095", "n2": "n2:9095"},
		map[uint32]string{0: "n2", 1: "n2", 2: "n2", 3: "n2"},
	)
	r := New("n1", f, &fakeMembership{alive: map[string]bool{"n1": true}}, freshRing("n1", "n2"))

	dec := r.RouteSession("acme", "support", "sessA")
	if dec.LeaderNodeID != "" {
		t.Errorf("expected empty LeaderNodeID when leader is dead, got %q", dec.LeaderNodeID)
	}
}

func freshRing(nodes ...string) *ring.Ring {
	r := ring.New(64)
	for _, n := range nodes {
		r.AddNode(n)
	}
	return r
}
