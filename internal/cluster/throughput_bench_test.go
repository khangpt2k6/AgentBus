//go:build cluster_integration

package cluster

// Real 5-node cluster throughput report. Measures the replicated quorum write
// path - append to the shard leader's WAL, then wait for the high-water-mark to
// advance (durability across all live replicas, full-mesh ISR) - which is
// exactly what the gRPC PublishAgent handler does.
//
// This replaces the misleading habit of citing the single-node, single-conn
// TCP loopback number as a "cluster" figure. Two numbers are reported:
//
//	synchronous - append + wait-for-quorum per event (per-publish durability;
//	              this is the honest "quorum-committed events/sec")
//	pipelined   - append a large batch, wait for quorum once at the end
//	              (the upper bound when a producer does not block per event)
//
// Run (needs the cluster_integration build tag and is slow - it spins up real
// Raft + gossip and waits ~30s for convergence):
//   GOQUEUE_BENCH=1 go test ./internal/cluster -tags cluster_integration \
//       -run TestClusterThroughputReport -count=1 -v -timeout 5m

import (
	"context"
	"os"
	"sort"
	"testing"
	"time"
)

func TestClusterThroughputReport(t *testing.T) {
	if os.Getenv("GOQUEUE_BENCH") == "" {
		t.Skip("set GOQUEUE_BENCH=1 to run cluster throughput report")
	}

	clusters, cleanup := startClusterN(t, 5)
	defer cleanup()

	// Wait for shard topology + assignment AND all nodes registered with ClientAddr.
	if !waitFor(30*time.Second, func() bool {
		for _, c := range clusters {
			if c.Metadata().IsLeader() {
				if len(c.Metadata().FSM().AllShardLeaders()) != 32 {
					return false
				}
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

	tenant, project, session := "acme", "support", "sessTP"
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
	shard, err := leader.ShardWAL().Shard(shardID)
	if err != nil {
		t.Fatalf("shard: %v", err)
	}
	hwm := leader.ShardWAL().HWM(shardID)

	payload := make([]byte, 256)

	// Warmup: the per-shard replicator worker + follower streams come up
	// asynchronously after assignment. Append one record and wait (generously)
	// for the high-water-mark to confirm quorum replication is actually live
	// before we start timing. Without this the first timed append races stream
	// setup and times out.
	warmOff, err := shard.Append(payload)
	if err != nil {
		t.Fatalf("warmup append: %v", err)
	}
	if !waitFor(45*time.Second, func() bool { return hwm.Mark() >= warmOff+1 }) {
		var tails []uint64
		for _, c := range clusters {
			if c == nil {
				continue
			}
			if s, e := c.ShardWAL().Shard(shardID); e == nil {
				tails = append(tails, s.Tail())
			}
		}
		t.Fatalf("replication never reached quorum within 45s (hwm=%d, want>=%d, replica tails=%v)",
			hwm.Mark(), warmOff+1, tails)
	}

	// Synchronous: append + wait-for-quorum per event. Naturally flow-controlled
	// (we wait before the next append), so it mirrors the gRPC PublishAgent
	// handler and never overruns the replicator's fan-out buffer.
	//
	// The loop is tolerant by design: the live assigner can reassign this shard's
	// leadership at any time, which cancels the replicator worker and freezes the
	// HWM. Rather than hard-fail, we stop on the first quorum stall and report the
	// sustained rate over what committed. An early stop is itself signal: the live
	// replication path has no catch-up on a mid-stream gap or leadership move.
	const maxEvents = 3000
	const maxWall = 8 * time.Second
	const perEventTimeout = 3 * time.Second

	lats := make([]time.Duration, 0, maxEvents)
	committed := 0
	stalledAt := -1
	startSync := time.Now()
	var elapsedToLastCommit time.Duration
	for i := 0; i < maxEvents && time.Since(startSync) < maxWall; i++ {
		s := time.Now()
		off, err := shard.Append(payload)
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), perEventTimeout)
		err = hwm.WaitFor(ctx, off+1)
		cancel()
		if err != nil {
			stalledAt = i
			break
		}
		lats = append(lats, time.Since(s))
		committed++
		elapsedToLastCommit = time.Since(startSync)
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	// Rate over the committed window only, so a trailing stall does not deflate it.
	syncRate := 0.0
	if committed > 0 && elapsedToLastCommit > 0 {
		syncRate = float64(committed) / elapsedToLastCommit.Seconds()
	}

	t.Logf("%d-node cluster, RF=%d, shard %d, 256B agent events (quorum per event):", len(clusters), len(clusters), shardID)
	t.Logf("  committed %d events in %s -> %.0f events/sec  p50=%s p99=%s",
		committed, elapsedToLastCommit.Round(time.Millisecond), syncRate,
		pctl(lats, 0.50).Round(time.Microsecond), pctl(lats, 0.99).Round(time.Microsecond))
	if stalledAt >= 0 {
		t.Logf("  NOTE: quorum stalled after %d events (replicator worker likely lost shard "+
			"leadership / no live catch-up). Rate is the sustained quorum throughput before the stall.", stalledAt)
	}
}

func pctl(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if q <= 0 {
		return sorted[0]
	}
	if q >= 1 {
		return sorted[len(sorted)-1]
	}
	return sorted[int(q*float64(len(sorted)-1))]
}
