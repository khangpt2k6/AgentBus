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
// appends to the local shard log and sends an AppendAck back.
//
// Note: we trust the offset on the wire to match what we'll assign
// locally (the follower's Tail must equal the entry's Offset). If it
// doesn't, we return the inconsistency as a stream error and let the
// leader either CatchUp or rebuild the stream.
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
		if shard.Tail() != entry.Offset {
			return fmt.Errorf("shardwal tail mismatch on shard %d: have %d, leader sent offset %d",
				entry.ShardId, shard.Tail(), entry.Offset)
		}
		off, err := shard.Append(entry.Payload)
		if err != nil {
			return err
		}
		if err := stream.Send(&pb.AppendAck{
			ShardId:    entry.ShardId,
			LastOffset: off,
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
