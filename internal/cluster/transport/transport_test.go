package transport

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/khangpt2k6/AgentBus/internal/cluster/shardwal"
	pb "github.com/khangpt2k6/AgentBus/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestServer_ReplicateWritesAndAcks(t *testing.T) {
	dir := t.TempDir()
	mgr, err := shardwal.NewManager(dir, "follower-1")
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	defer mgr.Close()

	srv := NewServer(mgr)
	gs := grpc.NewServer()
	pb.RegisterClusterServiceServer(gs, srv)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go gs.Serve(lis)
	defer gs.Stop()

	cc, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cc.Close()

	client := pb.NewClusterServiceClient(cc)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.Replicate(ctx)
	if err != nil {
		t.Fatalf("replicate: %v", err)
	}

	for i := uint64(0); i < 3; i++ {
		if err := stream.Send(&pb.AppendEntry{
			ShardId: 5, Offset: i, Payload: []byte("hi"), LeaderNodeId: "leader",
		}); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}

	for i := uint64(0); i < 3; i++ {
		ack, err := stream.Recv()
		if err != nil {
			t.Fatalf("recv ack %d: %v", i, err)
		}
		if ack.ShardId != 5 || ack.LastOffset != i {
			t.Fatalf("ack[%d] = shard=%d off=%d, want 5/%d", i, ack.ShardId, ack.LastOffset, i)
		}
	}

	shard, err := mgr.Shard(5)
	if err != nil {
		t.Fatalf("get shard: %v", err)
	}
	if shard.Tail() != 3 {
		t.Fatalf("shard tail = %d, want 3", shard.Tail())
	}
}

func TestServer_CatchUpStreamsFromOffset(t *testing.T) {
	dir := t.TempDir()
	mgr, err := shardwal.NewManager(dir, "leader")
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	defer mgr.Close()

	sh, err := mgr.Shard(9)
	if err != nil {
		t.Fatalf("shard: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := sh.Append([]byte{byte('a' + i)}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	srv := NewServer(mgr)
	gs := grpc.NewServer()
	pb.RegisterClusterServiceServer(gs, srv)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go gs.Serve(lis)
	defer gs.Stop()

	cc, _ := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer cc.Close()
	client := pb.NewClusterServiceClient(cc)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.CatchUp(ctx, &pb.CatchUpRequest{ShardId: 9, FromOffset: 2})
	if err != nil {
		t.Fatalf("catchup: %v", err)
	}
	var got []uint64
	for {
		entry, err := stream.Recv()
		if err != nil {
			break
		}
		got = append(got, entry.Offset)
	}
	if len(got) != 3 || got[0] != 2 || got[2] != 4 {
		t.Fatalf("catchup offsets = %v, want [2 3 4]", got)
	}
}

func TestClient_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	mgr, err := shardwal.NewManager(dir, "follower-2")
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	defer mgr.Close()
	srv := NewServer(mgr)
	gs := grpc.NewServer()
	pb.RegisterClusterServiceServer(gs, srv)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go gs.Serve(lis)
	defer gs.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cl, err := Dial(lis.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cl.Close()

	stream, err := cl.OpenReplicate(ctx)
	if err != nil {
		t.Fatalf("open replicate: %v", err)
	}
	if err := stream.Send(&pb.AppendEntry{ShardId: 3, Offset: 0, Payload: []byte("x")}); err != nil {
		t.Fatalf("send: %v", err)
	}
	ack, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if ack.LastOffset != 0 {
		t.Fatalf("ack offset = %d, want 0", ack.LastOffset)
	}
}
