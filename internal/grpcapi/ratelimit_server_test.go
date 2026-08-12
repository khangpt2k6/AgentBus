package grpcapi

import (
	"context"
	"testing"

	"github.com/khangpt2k6/EventBus/internal/broker"
	"github.com/khangpt2k6/EventBus/internal/consumer"
	"github.com/khangpt2k6/EventBus/internal/ratelimit"
	goqueuev1 "github.com/khangpt2k6/EventBus/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func agentReq(tenant string) *goqueuev1.PublishAgentRequest {
	return &goqueuev1.PublishAgentRequest{
		Event: &goqueuev1.AgentEvent{
			Tenant:    tenant,
			Project:   "support-bot",
			SessionId: "sess-1",
			AgentId:   "planner",
			Type:      "tool.call",
			Payload:   []byte(`{"tool":"search"}`),
		},
	}
}

func TestPublishAgentThrottlesNoisyTenantInIsolation(t *testing.T) {
	srv := NewServer(broker.New(), consumer.NewManager(), nil, nil)
	srv.SetRateLimiter(ratelimit.New(1, 2)) // burst 2 events per tenant

	ctx := context.Background()

	// The noisy tenant drains its burst, then gets throttled.
	for i := 0; i < 2; i++ {
		if _, err := srv.PublishAgent(ctx, agentReq("noisy")); err != nil {
			t.Fatalf("burst publish %d for noisy tenant should pass: %v", i, err)
		}
	}
	_, err := srv.PublishAgent(ctx, agentReq("noisy"))
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted for noisy tenant, got %v", err)
	}

	// A different tenant is unaffected by the noisy one: fault isolation.
	for i := 0; i < 2; i++ {
		if _, err := srv.PublishAgent(ctx, agentReq("quiet")); err != nil {
			t.Fatalf("quiet tenant publish %d must not be throttled by the noisy tenant: %v", i, err)
		}
	}
}

func TestPublishAgentWithoutLimiterIsUnlimited(t *testing.T) {
	srv := NewServer(broker.New(), consumer.NewManager(), nil, nil)
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		if _, err := srv.PublishAgent(ctx, agentReq("t")); err != nil {
			t.Fatalf("with no limiter, publish %d should pass: %v", i, err)
		}
	}
}
