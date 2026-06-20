//go:build cluster_integration

package cluster

// Failure-injection test: prove that every event which was quorum-committed
// before the shard leader is killed survives on the remaining nodes, byte for
// byte and in order (zero data loss), and that survivors agree (no split-brain).
//
// This is the correctness counterpart to the throughput report: it does not
// measure speed, it measures that the durability guarantee actually holds when
// a node dies mid-flight.
//
// Run:
//   GOQUEUE_BENCH=1 go test ./internal/cluster -tags cluster_integration \
//       -run TestZeroDataLossOnLeaderKill -count=1 -v -timeout 5m

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestZeroDataLossOnLeaderKill(t *testing.T) {
	clusters, cleanup := startCluster(t)
	defer cleanup()

	// Wait for shard assignment + all members registered.
	if !waitFor(30*time.Second, func() bool {
		for _, c := range clusters {
			if c == nil {
				continue
			}
			if c.Metadata().IsLeader() {
				if len(c.Metadata().FSM().AllShardLeaders()) != 32 {
					return false
				}
				for _, nc := range clusters {
					if nc == nil {
						continue
					}
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
		t.Fatal("cluster did not converge within 30s")
	}

	tenant, project, session := "acme", "support", "sessDL"
	dec := clusters[0].Router().RouteSession(tenant, project, session)
	if dec.LeaderNodeID == "" {
		t.Fatal("no shard leader for session")
	}
	shardID := dec.ShardID

	var leader *Cluster
	leaderIdx := -1
	for i, c := range clusters {
		if c != nil && c.cfg.NodeID == dec.LeaderNodeID {
			leader, leaderIdx = c, i
		}
	}
	if leader == nil {
		t.Fatal("leader cluster not found")
	}

	shard, err := leader.ShardWAL().Shard(shardID)
	if err != nil {
		t.Fatalf("shard: %v", err)
	}
	hwm := leader.ShardWAL().HWM(shardID)

	// Write N events with verifiable payloads, then wait for the whole batch to
	// reach quorum durability (acked by all replicas) before we kill anything.
	const n = 1000
	want := make([][]byte, n)
	var lastOff uint64
	for i := 0; i < n; i++ {
		want[i] = []byte(fmt.Sprintf("evt-%06d", i))
		lastOff, err = shard.Append(want[i])
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := hwm.WaitFor(ctx, lastOff+1); err != nil {
		cancel()
		t.Fatalf("batch did not reach quorum: %v", err)
	}
	cancel()
	t.Logf("wrote %d quorum-committed events to shard %d (leader %s)", n, shardID, dec.LeaderNodeID)

	// Kill the leader.
	if err := leader.Shutdown(); err != nil {
		t.Logf("leader shutdown returned: %v", err)
	}
	clusters[leaderIdx] = nil
	t.Logf("killed leader %s", dec.LeaderNodeID)

	// Every survivor must hold all N events, in order, byte-for-byte.
	survivors := 0
	for _, c := range clusters {
		if c == nil {
			continue
		}
		survivors++
		s, err := c.ShardWAL().Shard(shardID)
		if err != nil {
			t.Fatalf("survivor %s shard: %v", c.cfg.NodeID, err)
		}
		got := make([][]byte, 0, n)
		if err := s.Replay(0, func(off uint64, payload []byte) error {
			got = append(got, append([]byte(nil), payload...))
			return nil
		}); err != nil {
			t.Fatalf("survivor %s replay: %v", c.cfg.NodeID, err)
		}
		if len(got) < n {
			t.Fatalf("DATA LOSS on %s: have %d events, want %d", c.cfg.NodeID, len(got), n)
		}
		for i := 0; i < n; i++ {
			if string(got[i]) != string(want[i]) {
				t.Fatalf("CORRUPTION on %s at offset %d: got %q want %q", c.cfg.NodeID, i, got[i], want[i])
			}
		}
		t.Logf("survivor %s: all %d events intact and in order", c.cfg.NodeID, n)
	}
	if survivors < 2 {
		t.Fatalf("expected 2 survivors, got %d", survivors)
	}
	t.Logf("ZERO DATA LOSS verified: %d survivors each hold all %d quorum-committed events after leader kill", survivors, n)
}
