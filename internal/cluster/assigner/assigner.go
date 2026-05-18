// Package assigner runs on the metadata Raft leader and round-robins
// unassigned (or dead-leader) shards onto alive cluster members via
// metadata.Command writes.
//
// Plan() is the pure decision function (tested directly). RunLoop() is
// the production goroutine; it ticks every 2s and only calls Plan when
// this node is the Raft leader.
package assigner

import (
	"context"
	"sort"
	"time"

	"github.com/khangpt2k6/AgentBus/internal/cluster/metadata"
)

// Applier is the write-side interface the assigner needs. In production
// this is satisfied by an adapter around metadata.Metadata.Apply.
type Applier interface {
	Apply(metadata.Command) error
}

// Liveness reports which cluster members are currently alive.
type Liveness interface {
	IsAlive(nodeID string) bool
	AliveMembers() []string
}

// LeaderChecker reports whether this node is currently the Raft leader.
type LeaderChecker interface {
	IsLeader() bool
}

// Plan inspects FSM state, decides which shards need (re)assignment, and
// writes the corresponding commands via applier. Returns the number of
// commands issued.
func Plan(fsm *metadata.FSM, live Liveness, applier Applier) int {
	count := fsm.ShardCount()
	if count == 0 {
		return 0
	}
	alive := live.AliveMembers()
	if len(alive) == 0 {
		return 0
	}
	sort.Strings(alive)

	leaders := fsm.AllShardLeaders()
	issued := 0
	next := 0
	for s := uint32(0); s < count; s++ {
		current := leaders[s]
		if current != "" && live.IsAlive(current) {
			continue
		}
		target := alive[next%len(alive)]
		next++
		err := applier.Apply(metadata.Command{
			Op:     metadata.OpSetShardLeader,
			Shard:  s,
			NodeID: target,
		})
		if err == nil {
			issued++
		}
	}
	return issued
}

// RunLoop ticks every 2s and runs Plan when this node is the Raft leader.
// Exits when ctx is canceled.
func RunLoop(ctx context.Context, leader LeaderChecker, fsm *metadata.FSM, live Liveness, applier Applier) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if !leader.IsLeader() {
				continue
			}
			_ = Plan(fsm, live, applier)
		}
	}
}
