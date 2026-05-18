//go:build cluster_integration

package cluster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	pb "github.com/khangpt2k6/AgentBus/proto"
	"google.golang.org/grpc"
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

func startCluster(t *testing.T) (clusters [3]*Cluster, cleanup func()) {
	t.Helper()
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
			ClientAddr: fmt.Sprintf("127.0.0.1:%d", grpcPorts[i]),
		}
	}

	var servers [N]*grpc.Server
	var listeners [N]net.Listener

	for i := 0; i < N; i++ {
		cfg := Config{
			NodeID:      fmt.Sprintf("n%d", i+1),
			RaftBind:    fmt.Sprintf("127.0.0.1:%d", raftPorts[i]),
			GossipBind:  fmt.Sprintf("127.0.0.1:%d", gossipPorts[i]),
			ClientAddr:  fmt.Sprintf("127.0.0.1:%d", grpcPorts[i]),
			RaftDir:     t.TempDir(),
			ShardWALDir: t.TempDir(),
			Peers:       peers,
		}
		c, err := Start(cfg, &bytes.Buffer{})
		if err != nil {
			t.Fatalf("Start n%d: %v", i+1, err)
		}
		clusters[i] = c

		// Start a gRPC server on ClientAddr and register the cluster's transport
		// server. The replicator needs an actual listener to connect to.
		lis, err := net.Listen("tcp", cfg.ClientAddr)
		if err != nil {
			t.Fatalf("listen ClientAddr n%d: %v", i+1, err)
		}
		gs := grpc.NewServer()
		pb.RegisterClusterServiceServer(gs, c.TransportServer())
		go gs.Serve(lis) //nolint:errcheck
		listeners[i] = lis
		servers[i] = gs
	}

	cleanup = func() {
		for _, c := range clusters {
			if c != nil {
				_ = c.Shutdown()
			}
		}
		for _, gs := range servers {
			if gs != nil {
				gs.Stop()
			}
		}
	}
	return clusters, cleanup
}

func TestThreeNodeCluster_FormsAndElects(t *testing.T) {
	clusters, cleanup := startCluster(t)
	defer cleanup()

	if !waitFor(10*time.Second, func() bool {
		for _, c := range clusters {
			if len(c.Membership().Alive()) != 3 {
				return false
			}
		}
		return true
	}) {
		t.Fatal("gossip did not converge within 10s")
	}
	if !waitFor(10*time.Second, func() bool {
		n := 0
		for _, c := range clusters {
			if c.Metadata().IsLeader() {
				n++
			}
		}
		return n == 1
	}) {
		t.Fatal("metadata Raft did not elect a single leader within 10s")
	}

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
		t.Fatal("assigner did not populate shard leadership within 20s")
	}

	for i, c := range clusters {
		dec := c.Router().RouteSession("acme", "support-bot", "sessA")
		if dec.LeaderNodeID == "" {
			t.Errorf("n%d router returned empty LeaderNodeID for sessA", i+1)
		}
	}
}

func TestThreeNodeCluster_ReplicatesAgentEvents(t *testing.T) {
	clusters, cleanup := startCluster(t)
	defer cleanup()

	// Wait for shard topology + assignment AND all nodes registered with ClientAddr.
	if !waitFor(30*time.Second, func() bool {
		for _, c := range clusters {
			if c.Metadata().IsLeader() {
				if len(c.Metadata().FSM().AllShardLeaders()) != 32 {
					return false
				}
				// All 3 nodes must have ClientAddr registered.
				for _, nc := range clusters {
					m, ok := c.Metadata().FSM().MemberAt(nc.cfg.NodeID)
					if !ok || m.ClientAddr == "" {
						return false
					}
				}
				return true
			}
		}
		return false
	}) {
		t.Fatal("shards not assigned or nodes not fully registered within 30s")
	}

	tenant, project, session := "acme", "support", "sessA"
	dec := clusters[0].Router().RouteSession(tenant, project, session)
	if dec.LeaderNodeID == "" {
		t.Fatal("no leader for test session")
	}
	shardID := dec.ShardID

	var leader *Cluster
	for _, c := range clusters {
		if c.cfg.NodeID == dec.LeaderNodeID {
			leader = c
		}
	}
	if leader == nil {
		t.Fatal("could not find leader cluster")
	}

	// Append 10 entries directly through the leader's shardwal. (This test
	// focuses on replication; Plan 2a tested the gRPC handler.)
	shard, _ := leader.ShardWAL().Shard(shardID)
	for i := 0; i < 10; i++ {
		payload, _ := json.Marshal(map[string]any{"i": i, "tenant": tenant, "session": session})
		if _, err := shard.Append(payload); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	followers := []*Cluster{}
	for _, c := range clusters {
		if c.cfg.NodeID != dec.LeaderNodeID {
			followers = append(followers, c)
		}
	}

	if !waitFor(5*time.Second, func() bool {
		for _, f := range followers {
			s, err := f.ShardWAL().Shard(shardID)
			if err != nil || s.Tail() != 10 {
				return false
			}
		}
		return true
	}) {
		for _, f := range followers {
			s, _ := f.ShardWAL().Shard(shardID)
			t.Logf("follower %s shard %d tail=%d", f.cfg.NodeID, shardID, s.Tail())
		}
		t.Fatal("followers did not catch up within 5s")
	}

	if hwm := leader.ShardWAL().HWM(shardID).Mark(); hwm < 10 {
		t.Fatalf("leader HWM = %d, want >= 10", hwm)
	}
}

func TestThreeNodeCluster_NonLeaderKillPreservesData(t *testing.T) {
	clusters, cleanup := startCluster(t)
	defer cleanup()

	if !waitFor(30*time.Second, func() bool {
		for _, c := range clusters {
			if c.Metadata().IsLeader() {
				if len(c.Metadata().FSM().AllShardLeaders()) != 32 {
					return false
				}
				// All 3 nodes must have ClientAddr registered.
				for _, nc := range clusters {
					m, ok := c.Metadata().FSM().MemberAt(nc.cfg.NodeID)
					if !ok || m.ClientAddr == "" {
						return false
					}
				}
				return true
			}
		}
		return false
	}) {
		t.Fatal("shards not assigned or nodes not fully registered within 30s")
	}

	tenant, project, session := "acme", "support", "sessB"
	dec := clusters[0].Router().RouteSession(tenant, project, session)
	shardID := dec.ShardID

	var leader *Cluster
	var followers []*Cluster
	for _, c := range clusters {
		if c.cfg.NodeID == dec.LeaderNodeID {
			leader = c
		} else {
			followers = append(followers, c)
		}
	}
	if leader == nil || len(followers) < 2 {
		t.Fatalf("expected 1 leader + 2 followers, got leader=%v followers=%d", leader != nil, len(followers))
	}

	shard, _ := leader.ShardWAL().Shard(shardID)
	for i := 0; i < 5; i++ {
		if _, err := shard.Append([]byte(fmt.Sprintf("e%d", i))); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if !waitFor(5*time.Second, func() bool {
		for _, f := range followers {
			s, _ := f.ShardWAL().Shard(shardID)
			if s.Tail() != 5 {
				return false
			}
		}
		return true
	}) {
		t.Fatal("initial replication did not converge")
	}

	// Kill one follower (Shutdown). Data should remain on surviving follower.
	killed := followers[0]
	_ = killed.Shutdown()
	clusters[indexOf(clusters[:], killed)] = nil

	for i := 5; i < 10; i++ {
		if _, err := shard.Append([]byte(fmt.Sprintf("e%d", i))); err != nil {
			t.Fatalf("append after kill: %v", err)
		}
	}

	survivor := followers[1]
	if !waitFor(5*time.Second, func() bool {
		s, _ := survivor.ShardWAL().Shard(shardID)
		return s.Tail() == 10
	}) {
		s, _ := survivor.ShardWAL().Shard(shardID)
		t.Fatalf("survivor follower tail = %d, want 10", s.Tail())
	}
}

func indexOf(cs []*Cluster, target *Cluster) int {
	for i, c := range cs {
		if c == target {
			return i
		}
	}
	return -1
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
