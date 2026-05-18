package replicator

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/khangpt2k6/AgentBus/internal/cluster/shardwal"
	"github.com/khangpt2k6/AgentBus/internal/cluster/transport"
	pb "github.com/khangpt2k6/AgentBus/proto"
	"google.golang.org/grpc"
)

// startFollower starts an in-memory follower gRPC server backed by a
// shardwal manager. Returns listen addr, manager (test can inspect), and stop fn.
func startFollower(t *testing.T, nodeID string) (addr string, mgr *shardwal.Manager, stop func()) {
	t.Helper()
	dir := t.TempDir()
	m, err := shardwal.NewManager(dir, nodeID)
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	srv := transport.NewServer(m)
	gs := grpc.NewServer()
	pb.RegisterClusterServiceServer(gs, srv)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go gs.Serve(lis)
	return lis.Addr().String(), m, func() {
		gs.Stop()
		_ = lis.Close()
		_ = m.Close()
	}
}

func TestReplicator_FanOutToFollowers(t *testing.T) {
	leaderMgr, err := shardwal.NewManager(t.TempDir(), "leader")
	if err != nil {
		t.Fatalf("leader manager: %v", err)
	}
	defer leaderMgr.Close()

	f1Addr, f1Mgr, stop1 := startFollower(t, "f1")
	defer stop1()
	f2Addr, f2Mgr, stop2 := startFollower(t, "f2")
	defer stop2()

	rep := New(leaderMgr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rep.Add(ctx, 7, []FollowerAddr{
		{NodeID: "f1", Addr: f1Addr},
		{NodeID: "f2", Addr: f2Addr},
	})
	defer rep.Drop(7)

	// Append 5 entries on the leader.
	leaderShard, err := leaderMgr.Shard(7)
	if err != nil {
		t.Fatalf("shard: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := leaderShard.Append([]byte{byte('a' + i)}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	// Wait up to 3s for both followers to have all 5 entries AND the HWM
	// to catch up to 5 (record count, not last index).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		f1s, _ := f1Mgr.Shard(7)
		f2s, _ := f2Mgr.Shard(7)
		hwm := leaderMgr.HWM(7).Mark()
		if f1s.Tail() == 5 && f2s.Tail() == 5 && hwm >= 5 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	f1s, _ := f1Mgr.Shard(7)
	f2s, _ := f2Mgr.Shard(7)
	t.Fatalf("replication did not converge: f1.tail=%d f2.tail=%d hwm=%d (want 5/5/>=5)",
		f1s.Tail(), f2s.Tail(), leaderMgr.HWM(7).Mark())
}

func TestReplicator_DropStopsReplication(t *testing.T) {
	leaderMgr, _ := shardwal.NewManager(t.TempDir(), "leader")
	defer leaderMgr.Close()
	fAddr, fMgr, stop := startFollower(t, "f1")
	defer stop()

	rep := New(leaderMgr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rep.Add(ctx, 2, []FollowerAddr{{NodeID: "f1", Addr: fAddr}})

	leaderShard, _ := leaderMgr.Shard(2)
	_, _ = leaderShard.Append([]byte("a"))
	// Wait for first entry to propagate.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		fs, _ := fMgr.Shard(2)
		if fs.Tail() >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	rep.Drop(2)
	// Subsequent appends should NOT be replicated.
	_, _ = leaderShard.Append([]byte("b"))
	time.Sleep(200 * time.Millisecond)
	fs, _ := fMgr.Shard(2)
	if fs.Tail() != 1 {
		t.Fatalf("after Drop, follower tail = %d, want 1 (replication stopped)", fs.Tail())
	}
}
