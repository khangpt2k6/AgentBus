package controlplane

import (
	"context"
	"sync"
	"testing"
	"time"

	goqueuev1 "github.com/khangpt2k6/AgentBus/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// compile-time check that Service satisfies the generated server interface.
var _ goqueuev1.ControlPlaneServer = (*Service)(nil)

// fakeInboxStream implements grpc.ServerStreamingServer[RoutedEventMsg] for tests.
type fakeInboxStream struct {
	ctx  context.Context
	mu   sync.Mutex
	sent []*goqueuev1.RoutedEventMsg
}

func (f *fakeInboxStream) Send(m *goqueuev1.RoutedEventMsg) error {
	f.mu.Lock()
	f.sent = append(f.sent, m)
	f.mu.Unlock()
	return nil
}
func (f *fakeInboxStream) count() int        { f.mu.Lock(); defer f.mu.Unlock(); return len(f.sent) }
func (f *fakeInboxStream) Context() context.Context     { return f.ctx }
func (f *fakeInboxStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeInboxStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeInboxStream) SetTrailer(metadata.MD)       {}
func (f *fakeInboxStream) SendMsg(any) error            { return nil }
func (f *fakeInboxStream) RecvMsg(any) error            { return nil }

var _ grpc.ServerStreamingServer[goqueuev1.RoutedEventMsg] = (*fakeInboxStream)(nil)

func TestServiceRegisterAndList(t *testing.T) {
	svc := NewService(New())
	ctx := context.Background()
	if _, err := svc.RegisterAgent(ctx, &goqueuev1.RegisterAgentRequest{AgentId: "planner", Capabilities: []string{"plan"}}); err != nil {
		t.Fatal(err)
	}
	resp, err := svc.ListAgents(ctx, &goqueuev1.ListAgentsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetAgents()) != 1 || resp.GetAgents()[0].GetId() != "planner" {
		t.Errorf("agents = %+v", resp.GetAgents())
	}

	hb, _ := svc.Heartbeat(ctx, &goqueuev1.HeartbeatRequest{AgentId: "planner"})
	if !hb.GetKnown() {
		t.Error("heartbeat on registered agent should report known")
	}
}

func TestServiceGetSessionState(t *testing.T) {
	cp := New()
	cp.Ingest(ev("tool.call", "planner", "s1"))
	svc := NewService(cp)

	rs, err := svc.GetSessionState(context.Background(), &goqueuev1.SessionStateRequest{
		Tenant: "acme", Project: "bot", SessionId: "s1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !rs.GetFound() || rs.GetActiveAgent() != "planner" || rs.GetStatus() != "running" {
		t.Errorf("run state = %+v", rs)
	}

	missing, _ := svc.GetSessionState(context.Background(), &goqueuev1.SessionStateRequest{
		Tenant: "acme", Project: "bot", SessionId: "nope",
	})
	if missing.GetFound() {
		t.Error("unknown session should report found=false")
	}
}

func TestServiceOpenInboxStreamsHandoff(t *testing.T) {
	cp := New()
	cp.Register("escalator", nil)
	cp.OpenInbox("escalator") // ensure the box exists before delivery
	cp.Ingest(ev("tool.call", "planner", "s1"))
	cp.Ingest(handoffEv("planner", "escalator", "s1"))

	svc := NewService(cp)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &fakeInboxStream{ctx: ctx}

	errc := make(chan error, 1)
	go func() { errc <- svc.OpenInbox(&goqueuev1.OpenInboxRequest{AgentId: "escalator"}, stream) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && stream.count() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if stream.count() != 1 {
		t.Fatalf("expected 1 streamed event, got %d", stream.count())
	}
	if stream.sent[0].GetType() != "handoff" {
		t.Errorf("streamed event = %+v", stream.sent[0])
	}
	cancel()
	<-errc // OpenInbox returns on ctx cancel
}
