package grpcapi

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/khangpt2k6/AgentBus/internal/broker"
	"github.com/khangpt2k6/AgentBus/internal/consumer"
	goqueuev1 "github.com/khangpt2k6/AgentBus/proto"
)

func TestFetchRejectsEmptyTopic(t *testing.T) {
	srv := NewServer(broker.New(), consumer.NewManager(), nil, nil)
	_, err := srv.Fetch(context.Background(), &goqueuev1.FetchRequest{Partition: 0})
	if err == nil {
		t.Fatal("expected error for empty topic")
	}
}

func TestFetchRejectsNegativePartition(t *testing.T) {
	srv := NewServer(broker.New(), consumer.NewManager(), nil, nil)
	_, err := srv.Fetch(context.Background(), &goqueuev1.FetchRequest{Topic: "x", Partition: -1})
	if err == nil {
		t.Fatal("expected error for negative partition")
	}
}

func TestFetchReturnsEmptyForMissingTopic(t *testing.T) {
	srv := NewServer(broker.New(), consumer.NewManager(), nil, nil)
	resp, err := srv.Fetch(context.Background(), &goqueuev1.FetchRequest{
		Topic: "nonexistent", Partition: 99,
	})
	if err != nil {
		t.Fatalf("should not error: %v", err)
	}
	if len(resp.Messages) != 0 {
		t.Errorf("expected empty messages, got %d", len(resp.Messages))
	}
}

func TestFetchReturnsPublishedMessages(t *testing.T) {
	b := broker.New()
	srv := NewServer(b, consumer.NewManager(), nil, nil)

	// Publish 3 messages on partition 0.
	for i := 0; i < 3; i++ {
		if _, err := srv.Publish(context.Background(), &goqueuev1.PublishRequest{
			Topic:     "events",
			Partition: 0,
			Payload:   []byte("msg-" + string(rune('0'+i))),
		}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	resp, err := srv.Fetch(context.Background(), &goqueuev1.FetchRequest{
		Topic: "events", Partition: 0, FromOffset: 0, MaxCount: 10,
	})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(resp.Messages) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(resp.Messages))
	}
	if resp.NextOffset != 3 {
		t.Errorf("next_offset: got %d, want 3", resp.NextOffset)
	}
	if resp.Tail != 3 {
		t.Errorf("tail: got %d, want 3", resp.Tail)
	}
}

func TestFetchSessionFilterReturnsOnlyMatches(t *testing.T) {
	b := broker.New()
	srv := NewServer(b, consumer.NewManager(), nil, nil)

	// Publish three envelopes — two for sess-42, one for sess-99 — all on
	// the same explicit partition so they appear in the same fetch.
	publish := func(session, agentID string) {
		env := map[string]any{
			"version":    "v1",
			"type":       "tool.call",
			"tenant":     "acme",
			"project":    "bot",
			"session_id": session,
			"agent_id":   agentID,
			"created_at": "2026-01-01T00:00:00Z",
			"payload":    map[string]any{},
		}
		body, _ := json.Marshal(env)
		if _, err := srv.Publish(context.Background(), &goqueuev1.PublishRequest{
			Topic: "agent-events", Partition: 0, Payload: body,
		}); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	publish("sess-42", "planner")
	publish("sess-99", "planner")
	publish("sess-42", "executor")

	resp, err := srv.Fetch(context.Background(), &goqueuev1.FetchRequest{
		Topic: "agent-events", Partition: 0, MaxCount: 10,
		SessionFilter: &goqueuev1.SessionFilter{
			Tenant: "acme", Project: "bot", SessionId: "sess-42",
		},
	})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(resp.Messages) != 2 {
		t.Fatalf("expected 2 matches, got %d", len(resp.Messages))
	}
	for _, m := range resp.Messages {
		var got map[string]any
		_ = json.Unmarshal(m.Payload, &got)
		if got["session_id"] != "sess-42" {
			t.Errorf("filter leaked: session_id=%v", got["session_id"])
		}
	}
}

func TestFetchSessionFilterEmptyFieldsAreWildcards(t *testing.T) {
	b := broker.New()
	srv := NewServer(b, consumer.NewManager(), nil, nil)

	body, _ := json.Marshal(map[string]any{
		"version": "v1", "type": "x", "tenant": "acme", "project": "p",
		"session_id": "s1", "agent_id": "a", "created_at": "2026-01-01T00:00:00Z",
		"payload": map[string]any{},
	})
	_, _ = srv.Publish(context.Background(), &goqueuev1.PublishRequest{
		Topic: "t", Partition: 0, Payload: body,
	})

	// Empty filter (all wildcards) → matches.
	resp, _ := srv.Fetch(context.Background(), &goqueuev1.FetchRequest{
		Topic: "t", Partition: 0, MaxCount: 10,
		SessionFilter: &goqueuev1.SessionFilter{},
	})
	if len(resp.Messages) != 1 {
		t.Fatalf("empty filter should match, got %d", len(resp.Messages))
	}
}

func TestSessionFilterMatchesRespectsAllFields(t *testing.T) {
	body, _ := json.Marshal(map[string]any{
		"tenant": "acme", "project": "bot", "session_id": "sess-1",
	})
	cases := []struct {
		name   string
		filter *goqueuev1.SessionFilter
		want   bool
	}{
		{"exact match", &goqueuev1.SessionFilter{Tenant: "acme", Project: "bot", SessionId: "sess-1"}, true},
		{"wrong tenant", &goqueuev1.SessionFilter{Tenant: "other", Project: "bot", SessionId: "sess-1"}, false},
		{"wrong project", &goqueuev1.SessionFilter{Tenant: "acme", Project: "other", SessionId: "sess-1"}, false},
		{"wrong session", &goqueuev1.SessionFilter{Tenant: "acme", Project: "bot", SessionId: "sess-2"}, false},
		{"only tenant set", &goqueuev1.SessionFilter{Tenant: "acme"}, true},
		{"nil filter", nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := sessionFilterMatches(body, tc.filter); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSessionFilterRejectsInvalidJSON(t *testing.T) {
	if sessionFilterMatches([]byte("not json"), &goqueuev1.SessionFilter{Tenant: "x"}) {
		t.Error("non-JSON payload should not match a non-empty filter")
	}
}
