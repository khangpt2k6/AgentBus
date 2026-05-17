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
	for i := 0; i < N; i++ {
		raftPorts[i] = freePort(t)
		gossipPorts[i] = freePort(t)
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

	// Wait for gossip convergence: all nodes see all peers.
	if !waitFor(10*time.Second, func() bool {
		for _, c := range clusters {
			if len(c.Membership().Alive()) != N {
				return false
			}
		}
		return true
	}) {
		for i, c := range clusters {
			t.Logf("n%d Alive=%v", i+1, c.Membership().Alive())
		}
		t.Fatal("gossip did not converge within 10s")
	}

	// Wait for exactly one metadata Raft leader to emerge.
	if !waitFor(10*time.Second, func() bool {
		leaders := 0
		for _, c := range clusters {
			if c.Metadata().IsLeader() {
				leaders++
			}
		}
		return leaders == 1
	}) {
		for i, c := range clusters {
			t.Logf("n%d IsLeader=%v Leader=%q", i+1, c.Metadata().IsLeader(), c.Metadata().Leader())
		}
		t.Fatal("metadata Raft did not elect a single leader within 10s")
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
