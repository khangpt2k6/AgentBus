package metadata

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/hashicorp/raft"
)

func applyCmd(t *testing.T, f *FSM, c Command) interface{} {
	t.Helper()
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return f.Apply(&raft.Log{Data: b})
}

func TestFSM_AddAndRemoveMember(t *testing.T) {
	f := NewFSM()
	applyCmd(t, f, Command{Op: OpAddMember, NodeID: "n1", Addr: "127.0.0.1:7001"})
	applyCmd(t, f, Command{Op: OpAddMember, NodeID: "n2", Addr: "127.0.0.1:7002"})
	if got, want := len(f.Members()), 2; got != want {
		t.Fatalf("Members len = %d, want %d", got, want)
	}
	applyCmd(t, f, Command{Op: OpRemoveMember, NodeID: "n1"})
	if got, want := len(f.Members()), 1; got != want {
		t.Fatalf("after remove, len = %d, want %d", got, want)
	}
}

func TestFSM_SetShardLeader(t *testing.T) {
	f := NewFSM()
	applyCmd(t, f, Command{Op: OpSetShardLeader, Shard: 7, NodeID: "n3"})
	if got := f.ShardLeader(7); got != "n3" {
		t.Fatalf("ShardLeader(7) = %q, want %q", got, "n3")
	}
	if got := f.ShardLeader(8); got != "" {
		t.Fatalf("ShardLeader(8) = %q, want empty", got)
	}
}

func TestFSM_SnapshotRestoreRoundTrip(t *testing.T) {
	src := NewFSM()
	applyCmd(t, src, Command{Op: OpAddMember, NodeID: "n1", Addr: "a:1"})
	applyCmd(t, src, Command{Op: OpAddMember, NodeID: "n2", Addr: "a:2"})
	applyCmd(t, src, Command{Op: OpSetShardLeader, Shard: 4, NodeID: "n2"})

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&memSink{Buffer: &buf}); err != nil {
		t.Fatalf("persist: %v", err)
	}

	dst := NewFSM()
	if err := dst.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if got, want := len(dst.Members()), 2; got != want {
		t.Fatalf("restored Members len = %d, want %d", got, want)
	}
	if got := dst.ShardLeader(4); got != "n2" {
		t.Fatalf("restored ShardLeader(4) = %q, want %q", got, "n2")
	}
}

// memSink satisfies raft.SnapshotSink for in-memory testing.
type memSink struct{ *bytes.Buffer }

func (m *memSink) ID() string    { return "test-snap" }
func (m *memSink) Cancel() error { return nil }
func (m *memSink) Close() error  { return nil }

func TestFSM_RegisterMemberWithClientAddr(t *testing.T) {
	f := NewFSM()
	applyCmd(t, f, Command{Op: OpRegisterMember, NodeID: "n1", Addr: "127.0.0.1:7001", ClientAddr: "127.0.0.1:9095"})
	m, ok := f.MemberAt("n1")
	if !ok {
		t.Fatalf("MemberAt(\"n1\") = false, want true")
	}
	if m.RaftAddr != "127.0.0.1:7001" {
		t.Errorf("RaftAddr = %q", m.RaftAddr)
	}
	if m.ClientAddr != "127.0.0.1:9095" {
		t.Errorf("ClientAddr = %q", m.ClientAddr)
	}
}

func TestFSM_SetShardCount(t *testing.T) {
	f := NewFSM()
	if got := f.ShardCount(); got != 0 {
		t.Fatalf("initial ShardCount = %d, want 0", got)
	}
	applyCmd(t, f, Command{Op: OpSetShardCount, Shard: 32})
	if got := f.ShardCount(); got != 32 {
		t.Fatalf("after set, ShardCount = %d, want 32", got)
	}
}

func TestFSM_SnapshotRestorePreservesV2Fields(t *testing.T) {
	src := NewFSM()
	applyCmd(t, src, Command{Op: OpRegisterMember, NodeID: "n1", Addr: "raft:1", ClientAddr: "grpc:1"})
	applyCmd(t, src, Command{Op: OpSetShardCount, Shard: 32})
	applyCmd(t, src, Command{Op: OpSetShardLeader, Shard: 7, NodeID: "n1"})

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&memSink{Buffer: &buf}); err != nil {
		t.Fatalf("persist: %v", err)
	}

	dst := NewFSM()
	if err := dst.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if got := dst.ShardCount(); got != 32 {
		t.Errorf("restored ShardCount = %d, want 32", got)
	}
	if m, ok := dst.MemberAt("n1"); !ok || m.ClientAddr != "grpc:1" {
		t.Errorf("restored ClientAddr = %v (ok=%v)", m, ok)
	}
	if got := dst.ShardLeader(7); got != "n1" {
		t.Errorf("restored ShardLeader(7) = %q", got)
	}
}
