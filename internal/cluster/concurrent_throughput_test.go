//go:build cluster_integration

package cluster

// End-to-end cluster throughput under CONCURRENT sessions (the realistic shape:
// a broker serves many sessions at once). Each session writes to its shard and
// waits for quorum per event; many sessions run in parallel, so aggregate
// throughput reflects per-shard parallelism plus shard-log group commit on hot
// shards - far above the synchronous single-session number.
//
// Run:
//   GOQUEUE_BENCH=1 go test ./internal/cluster -tags cluster_integration \
//       -run TestClusterConcurrentThroughputReport -count=1 -v -timeout 5m

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestClusterConcurrentThroughputReport(t *testing.T) {
	if os.Getenv("GOQUEUE_BENCH") == "" {
		t.Skip("set GOQUEUE_BENCH=1 to run concurrent cluster throughput report")
	}

	clusters, cleanup := startClusterN(t, 5)
	defer cleanup()

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

	// Resolve a set of sessions to their (shard, leader).
	type target struct {
		shard  uint32
		leader *Cluster
	}
	const sessions = 32
	var targets []target
	for i := 0; i < sessions; i++ {
		dec := clusters[0].Router().RouteSession("acme", "tp", fmt.Sprintf("sess-%d", i))
		var ldr *Cluster
		for _, c := range clusters {
			if c != nil && c.cfg.NodeID == dec.LeaderNodeID {
				ldr = c
			}
		}
		if ldr != nil {
			targets = append(targets, target{dec.ShardID, ldr})
		}
	}
	if len(targets) == 0 {
		t.Fatal("no routable sessions")
	}

	payload := make([]byte, 256)
	const maxWall = 8 * time.Second
	var total int64

	var wg sync.WaitGroup
	start := time.Now()
	for _, tg := range targets {
		wg.Add(1)
		go func(tg target) {
			defer wg.Done()
			shard, err := tg.leader.ShardWAL().Shard(tg.shard)
			if err != nil {
				return
			}
			hwm := tg.leader.ShardWAL().HWM(tg.shard)
			for time.Since(start) < maxWall {
				off, err := shard.Append(payload)
				if err != nil {
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err = hwm.WaitFor(ctx, off+1)
				cancel()
				if err != nil {
					return
				}
				atomic.AddInt64(&total, 1)
			}
		}(tg)
	}
	wg.Wait()
	elapsed := time.Since(start)
	rate := float64(total) / elapsed.Seconds()

	t.Logf("%d-node cluster, RF=%d, %d concurrent sessions, 256B agent events:", len(clusters), len(clusters), len(targets))
	t.Logf("  %d quorum-committed events in %s -> %.0f events/sec aggregate",
		total, elapsed.Round(time.Millisecond), rate)
}
