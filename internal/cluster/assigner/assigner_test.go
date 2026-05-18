package assigner

import (
	"testing"

	"github.com/khangpt2k6/AgentBus/internal/cluster/metadata"
)

type fakeApplier struct {
	cmds []metadata.Command
}

func (f *fakeApplier) Apply(cmd metadata.Command) error {
	f.cmds = append(f.cmds, cmd)
	return nil
}

type fakeLive struct{ alive map[string]bool }

func (f *fakeLive) IsAlive(nodeID string) bool { return f.alive[nodeID] }
func (f *fakeLive) AliveMembers() []string {
	out := make([]string, 0, len(f.alive))
	for nid, a := range f.alive {
		if a {
			out = append(out, nid)
		}
	}
	return out
}

func TestAssigner_AssignsUnassignedShards(t *testing.T) {
	fsm := metadata.NewFSM()
	apply(t, fsm, metadata.Command{Op: metadata.OpSetShardCount, Shard: 4})
	apply(t, fsm, metadata.Command{Op: metadata.OpRegisterMember, NodeID: "n1", Addr: "r1", ClientAddr: "c1"})
	apply(t, fsm, metadata.Command{Op: metadata.OpRegisterMember, NodeID: "n2", Addr: "r2", ClientAddr: "c2"})

	app := &fakeApplier{}
	live := &fakeLive{alive: map[string]bool{"n1": true, "n2": true}}

	cmds := Plan(fsm, live, app)
	if cmds != 4 {
		t.Fatalf("Plan() issued %d commands, want 4 (one per shard)", cmds)
	}
	// Replay assigner's commands into the FSM to reflect what production would do.
	for _, c := range app.cmds {
		apply(t, fsm, c)
	}
	leaders := fsm.AllShardLeaders()
	if len(leaders) != 4 {
		t.Errorf("AllShardLeaders len = %d, want 4", len(leaders))
	}
	counts := map[string]int{}
	for _, nid := range leaders {
		counts[nid]++
	}
	if counts["n1"] != 2 || counts["n2"] != 2 {
		t.Errorf("uneven distribution: %v", counts)
	}
}

func TestAssigner_LeavesAssignedAlone(t *testing.T) {
	fsm := metadata.NewFSM()
	apply(t, fsm, metadata.Command{Op: metadata.OpSetShardCount, Shard: 2})
	apply(t, fsm, metadata.Command{Op: metadata.OpRegisterMember, NodeID: "n1", Addr: "r1", ClientAddr: "c1"})
	apply(t, fsm, metadata.Command{Op: metadata.OpRegisterMember, NodeID: "n2", Addr: "r2", ClientAddr: "c2"})
	apply(t, fsm, metadata.Command{Op: metadata.OpSetShardLeader, Shard: 0, NodeID: "n1"})
	apply(t, fsm, metadata.Command{Op: metadata.OpSetShardLeader, Shard: 1, NodeID: "n2"})

	app := &fakeApplier{}
	live := &fakeLive{alive: map[string]bool{"n1": true, "n2": true}}

	if cmds := Plan(fsm, live, app); cmds != 0 {
		t.Fatalf("Plan() with no changes issued %d commands, want 0", cmds)
	}
}

func TestAssigner_ReassignsDeadLeader(t *testing.T) {
	fsm := metadata.NewFSM()
	apply(t, fsm, metadata.Command{Op: metadata.OpSetShardCount, Shard: 2})
	apply(t, fsm, metadata.Command{Op: metadata.OpRegisterMember, NodeID: "n1", Addr: "r1", ClientAddr: "c1"})
	apply(t, fsm, metadata.Command{Op: metadata.OpRegisterMember, NodeID: "n2", Addr: "r2", ClientAddr: "c2"})
	apply(t, fsm, metadata.Command{Op: metadata.OpSetShardLeader, Shard: 0, NodeID: "n2"})
	apply(t, fsm, metadata.Command{Op: metadata.OpSetShardLeader, Shard: 1, NodeID: "n2"})

	app := &fakeApplier{}
	live := &fakeLive{alive: map[string]bool{"n1": true, "n2": false}}

	if cmds := Plan(fsm, live, app); cmds != 2 {
		t.Fatalf("Plan() with dead leader issued %d commands, want 2", cmds)
	}
	for _, c := range app.cmds {
		apply(t, fsm, c)
	}
	leaders := fsm.AllShardLeaders()
	if leaders[0] != "n1" || leaders[1] != "n1" {
		t.Errorf("shards not reassigned to n1: %v", leaders)
	}
}

func apply(t *testing.T, fsm *metadata.FSM, c metadata.Command) {
	t.Helper()
	if err := (&fsmApplier{fsm: fsm}).Apply(c); err != nil {
		t.Fatalf("apply: %v", err)
	}
}

type fsmApplier struct{ fsm *metadata.FSM }

func (a *fsmApplier) Apply(c metadata.Command) error {
	helper := jsonApplyHelper{}
	return helper.do(a.fsm, c)
}
