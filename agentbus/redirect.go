package agentbus

import (
	"context"
	"errors"

	pb "github.com/khangpt2k6/AgentBus/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// notLeaderHint extracts a leader address hint from a gRPC error returned
// by PublishAgent. Returns ("", false) if the error is not a redirect.
func notLeaderHint(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	st, ok := status.FromError(err)
	if !ok {
		return "", false
	}
	if st.Code() != codes.FailedPrecondition {
		return "", false
	}
	for _, d := range st.Details() {
		if hint, ok := d.(*pb.NotLeaderError); ok {
			return hint.LeaderAddr, true
		}
	}
	return "", false
}

// dialLeader builds a transient gRPC client connection to the redirect
// target. The caller is responsible for closing it.
func dialLeader(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	if addr == "" {
		return nil, errors.New("agentbus: empty leader hint; cluster has no current leader for this shard")
	}
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}
