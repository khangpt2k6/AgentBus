package transport

import (
	"context"
	"fmt"

	pb "github.com/khangpt2k6/AgentBus/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client is a thin wrapper around gRPC ClusterService client. Each shard
// leader holds one Client per follower address. The Client outlives any
// single stream; callers open Replicate streams on demand.
type Client struct {
	cc *grpc.ClientConn
	c  pb.ClusterServiceClient
}

// Dial opens a new client connection. addr is host:port (gRPC).
func Dial(addr string) (*Client, error) {
	if addr == "" {
		return nil, fmt.Errorf("transport: empty addr")
	}
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{cc: cc, c: pb.NewClusterServiceClient(cc)}, nil
}

// OpenReplicate opens a new bidirectional replicate stream. Caller owns
// the stream and is responsible for closing it (CloseSend).
func (c *Client) OpenReplicate(ctx context.Context) (pb.ClusterService_ReplicateClient, error) {
	return c.c.Replicate(ctx)
}

// CatchUp opens a server-streaming catchup. Returns a stream that yields
// entries starting at fromOffset until EOF.
func (c *Client) CatchUp(ctx context.Context, shardID uint32, fromOffset uint64) (pb.ClusterService_CatchUpClient, error) {
	return c.c.CatchUp(ctx, &pb.CatchUpRequest{ShardId: shardID, FromOffset: fromOffset})
}

// Close shuts the underlying gRPC connection.
func (c *Client) Close() error { return c.cc.Close() }
