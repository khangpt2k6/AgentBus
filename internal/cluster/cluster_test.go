//go:build cluster_integration

package cluster

import (
	"bytes"
	"fmt"
	"net"
	"testing"
	"time"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

func TestThreeNodeCluster_FormsAndElects(t *testing.T) {
	const N = 3
	raftPorts := make([]int, N)
	gossipPorts := make([]int, N)
	grpcPorts := make([]int, N)
	for i := 0; i < N; i++ {
		raftPorts[i] = freePort(t)
		gossipPorts[i] = freePort(t)
		grpcPorts[i] = freePort(t)
	}

	peers := make([]Peer, N)
	for i := 0; i < N; i++ {
		peers[i] = Peer{
			NodeID:     fmt.Sprintf("n%d", i+1),
			RaftAddr:   fmt.Sprintf("127.0.0.1:%d", raftPorts[i]),
			GossipAddr: fmt.Sprintf("127.0.0.1:%d", gossipPorts[i]),
		}
	}

	var clusters [N]*Cluster
	for i := 0; i < N; i++ {
		cfg := Config{
			NodeID:     fmt.Sprintf("n%d", i+1),
			RaftBind:   fmt.Sprintf("127.0.0.1:%d", raftPorts[i]),
			GossipBind: fmt.Sprintf("127.0.0.1:%d", gossipPorts[i]),
			ClientAddr: fmt.Sprintf("127.0.0.1:%d", grpcPorts[i]),
			RaftDir:    t.TempDir(),
			Peers:      peers,
		}
		c, err := Start(cfg, &bytes.Buffer{})
		if err != nil {
			t.Fatalf("Start n%d: %v", i+1, err)
		}
		clusters[i] = c
		defer c.Shutdown()
	}

	if !waitFor(10*time.Second, func() bool {
		for _, c := range clusters {
			if len(c.Membership().Alive()) != N {
				return false
			}
		}
		return true
	}) {
		t.Fatal("gossip did not converge within 10s")
	}

	if !waitFor(10*time.Second, func() bool {
		leaders := 0
		for _, c := range clusters {
			if c.Metadata().IsLeader() {
				leaders++
			}
		}
		return leaders == 1
	}) {
		t.Fatal("metadata Raft did not elect a single leader within 10s")
	}

	// M3 expectation: within another ~15s, the assigner has populated
	// shard leadership for all 32 shards across the 3 nodes.
	if !waitFor(20*time.Second, func() bool {
		for _, c := range clusters {
			if c.Metadata().IsLeader() {
				if c.Metadata().FSM().ShardCount() != 32 {
					return false
				}
				return len(c.Metadata().FSM().AllShardLeaders()) == 32
			}
		}
		return false
	}) {
		for i, c := range clusters {
			t.Logf("n%d ShardCount=%d AssignedLeaders=%d",
				i+1,
				c.Metadata().FSM().ShardCount(),
				len(c.Metadata().FSM().AllShardLeaders()),
			)
		}
		t.Fatal("assigner did not populate shard leadership within 20s")
	}

	// Every node's router should route a sample session somewhere non-empty.
	for i, c := range clusters {
		dec := c.Router().RouteSession("acme", "support-bot", "sessA")
		if dec.LeaderNodeID == "" {
			t.Errorf("n%d router returned empty LeaderNodeID for sessA", i+1)
		}
	}
}

func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}
