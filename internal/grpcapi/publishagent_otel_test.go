package grpcapi

import (
	"context"
	"testing"

	"github.com/khangpt2k6/AgentBus/internal/agentstream"
	pbv1 "github.com/khangpt2k6/AgentBus/proto"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// PublishAgent must anchor its span to the session-derived trace id and tag it
// with agent.session.id, so a search by session returns every agent event.
func TestPublishAgent_SessionDerivedTrace(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prev)

	s := newTestServer(t)
	req := &pbv1.PublishAgentRequest{Event: &pbv1.AgentEvent{
		Tenant:    "acme",
		Project:   "support",
		SessionId: "sess-42",
		Type:      "tool.call",
		AgentId:   "planner",
		Payload:   []byte("{}"),
	}}
	if _, err := s.PublishAgent(context.Background(), req); err != nil {
		t.Fatalf("publish agent: %v", err)
	}
	_ = tp.ForceFlush(context.Background())

	want := agentstream.SessionTraceID("acme", "support", "sess-42")
	var found bool
	for _, sp := range sr.Ended() {
		if sp.Name() != "BrokerService.PublishAgent" {
			continue
		}
		found = true
		if sp.SpanContext().TraceID() != want {
			t.Errorf("trace id = %v, want session-derived %v", sp.SpanContext().TraceID(), want)
		}
		var hasSession bool
		for _, a := range sp.Attributes() {
			if string(a.Key) == agentstream.AttrAgentSession && a.Value.AsString() == "sess-42" {
				hasSession = true
			}
		}
		if !hasSession {
			t.Errorf("span missing %s=sess-42 attribute", agentstream.AttrAgentSession)
		}
	}
	if !found {
		t.Fatal("no BrokerService.PublishAgent span was recorded")
	}
}
