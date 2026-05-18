package agentbus

import (
	"context"
	"net"
	"testing"
	"time"

	pb "github.com/khangpt2k6/AgentBus/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stubServer accepts PublishAgent and either redirects (first call) or
// accepts (second call). Embeds the unimplemented broker server so all
// other methods return Unimplemented cleanly.
type stubServer struct {
	pb.UnimplementedBrokerServiceServer
	calls      int
	redirectTo string
	t          *testing.T
}

func (s *stubServer) PublishAgent(ctx context.Context, req *pb.PublishAgentRequest) (*pb.PublishAgentResponse, error) {
	s.calls++
	if s.calls == 1 && s.redirectTo != "" {
		st := status.New(codes.FailedPrecondition, "not the leader of this session's shard")
		st, _ = st.WithDetails(&pb.NotLeaderError{LeaderAddr: s.redirectTo})
		return nil, st.Err()
	}
	return &pb.PublishAgentResponse{Offset: 42}, nil
}

func freePortSDK(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

func startStub(t *testing.T, redirectTo string) (addr string, srv *stubServer, stop func()) {
	t.Helper()
	port := freePortSDK(t)
	addr = "127.0.0.1:" + itoaSDK(port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	srv = &stubServer{redirectTo: redirectTo, t: t}
	pb.RegisterBrokerServiceServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	return addr, srv, func() { gs.Stop(); _ = lis.Close() }
}

func itoaSDK(n int) string {
	if n == 0 {
		return "0"
	}
	var b [16]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func TestSDK_PublishAgentFollowsRedirect(t *testing.T) {
	// Stub B accepts; Stub A redirects to B.
	addrB, stubB, stopB := startStub(t, "")
	defer stopB()
	addrA, stubA, stopA := startStub(t, addrB)
	defer stopA()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := Connect(ctx, addrA)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close()

	if _, err := c.PublishAgent(ctx, AgentEvent{
		Tenant: "acme", Project: "support", SessionID: "s1",
		AgentID: "p1", Type: "tool.call",
		Payload: []byte("{}"),
	}); err != nil {
		t.Fatalf("PublishAgent: %v", err)
	}

	if stubA.calls != 1 || stubB.calls != 1 {
		t.Fatalf("call counts: A=%d B=%d, want 1/1", stubA.calls, stubB.calls)
	}
}

func TestSDK_PublishAgentNoLeaderReturnsError(t *testing.T) {
	// Stub A redirects with empty leader addr (no current leader).
	addrA, _, stopA := startStub(t, "")
	defer stopA()

	// Override redirectTo to force the NOT_LEADER with empty hint.
	// We'll create a custom server manually.
	port := freePortSDK(t)
	addr := "127.0.0.1:" + itoaSDK(port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	_ = addrA // unused var kept to avoid confusion
	gs := grpc.NewServer()
	emptyHintSrv := &emptyHintServer{}
	pb.RegisterBrokerServiceServer(gs, emptyHintSrv)
	go func() { _ = gs.Serve(lis) }()
	defer func() { gs.Stop(); _ = lis.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := Connect(ctx, addr)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close()

	_, pubErr := c.PublishAgent(ctx, AgentEvent{
		Tenant: "acme", Project: "support", SessionID: "s2",
		AgentID: "p1", Type: "tool.call",
		Payload: []byte("{}"),
	})
	if pubErr == nil {
		t.Fatal("expected error for no-leader redirect, got nil")
	}
}

type emptyHintServer struct {
	pb.UnimplementedBrokerServiceServer
}

func (s *emptyHintServer) PublishAgent(_ context.Context, _ *pb.PublishAgentRequest) (*pb.PublishAgentResponse, error) {
	st := status.New(codes.FailedPrecondition, "no leader elected yet")
	st, _ = st.WithDetails(&pb.NotLeaderError{LeaderAddr: ""})
	return nil, st.Err()
}
