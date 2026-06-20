// Package transport implements the gRPC ClusterService server + client
// used for inter-node communication: replication and catchup.
package transport

import (
	"fmt"
	"io"

	"github.com/khangpt2k6/AgentBus/internal/cluster/shardwal"
	pb "github.com/khangpt2k6/AgentBus/proto"
)

// Server is the inter-node RPC handler. Receives entries from shard
// leaders and writes them to the local shardwal Manager.
type Server struct {
	pb.UnimplementedClusterServiceServer
	mgr *shardwal.Manager
}

// NewServer wires a ClusterService server to mgr.
func NewServer(mgr *shardwal.Manager) *Server {
	return &Server{mgr: mgr}
}

// Replicate handles a leader-to-follower stream. For each AppendEntry it
// applies the record (if new) and acks the follower's last-held offset.
//
// The handler is idempotent in the offset: the leader streams a shard from
// offset 0 on every (re)connect, so a follower that already has a prefix skips
// it rather than erroring. This makes reconnects and leadership changes
// self-healing - a follower is never wedged by a gap, because the leader always
// re-streams from a point at or before the follower's tail.
//
//	offset <  tail : already applied, skip and re-ack
//	offset == tail : the next record, append
//	offset >  tail : a real gap (cannot apply out of order) -> error so the
//	                 leader rebuilds the stream from the start
func (s *Server) Replicate(stream pb.ClusterService_ReplicateServer) error {
	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		shard, err := s.mgr.Shard(entry.ShardId)
		if err != nil {
			return err
		}
		tail := shard.Tail()
		switch {
		case entry.Offset < tail:
			// Already have it; fall through to re-ack our current position.
		case entry.Offset == tail:
			if _, err := shard.Append(entry.Payload); err != nil {
				return err
			}
		default: // entry.Offset > tail
			return fmt.Errorf("shardwal gap on shard %d: have tail %d, leader sent offset %d",
				entry.ShardId, tail, entry.Offset)
		}
		// Ack the last offset we hold (tail-1). tail is >= 1 here: the append
		// branch raised it, and the skip branch requires offset < tail so tail
		// was already >= 1.
		if err := stream.Send(&pb.AppendAck{
			ShardId:    entry.ShardId,
			LastOffset: shard.Tail() - 1,
			NodeId:     s.mgr.SelfID(),
		}); err != nil {
			return err
		}
	}
}

// CatchUp streams shard entries from the given offset.
func (s *Server) CatchUp(req *pb.CatchUpRequest, stream pb.ClusterService_CatchUpServer) error {
	shard, err := s.mgr.Shard(req.ShardId)
	if err != nil {
		return err
	}
	return shard.Replay(req.FromOffset, func(offset uint64, payload []byte) error {
		return stream.Send(&pb.AppendEntry{
			ShardId: req.ShardId,
			Offset:  offset,
			Payload: payload,
		})
	})
}
