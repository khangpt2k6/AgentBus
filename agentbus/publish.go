package agentbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	pb "github.com/khangpt2k6/AgentBus/proto"
)

// PublishResult describes a successful publish.
type PublishResult struct {
	Partition int32
	Offset    int64
}

// Publish sends payload to topic. Routing uses round-robin partitioning.
//
// For per-session ordering, prefer PublishWithKey or PublishAgent — Publish
// does NOT guarantee any ordering relationship across calls.
func (c *Client) Publish(ctx context.Context, topic string, payload []byte) (PublishResult, error) {
	return c.publishRaw(ctx, topic, "", -1, payload)
}

// PublishWithKey sends payload to the partition determined by hash(key).
// Two publishes with the same key always land on the same partition, so
// per-key ordering is preserved.
func (c *Client) PublishWithKey(ctx context.Context, topic, key string, payload []byte) (PublishResult, error) {
	if key == "" {
		return PublishResult{}, errors.New("agentbus: PublishWithKey requires a non-empty key (use Publish for round-robin)")
	}
	return c.publishRaw(ctx, topic, key, -1, payload)
}

// PublishToPartition writes explicitly to the named partition. Use only when
// you have a reason to override the broker's partition assignment.
func (c *Client) PublishToPartition(ctx context.Context, topic string, partition int32, payload []byte) (PublishResult, error) {
	if partition < 0 {
		return PublishResult{}, fmt.Errorf("agentbus: partition must be >= 0, got %d", partition)
	}
	return c.publishRaw(ctx, topic, "", partition, payload)
}

func (c *Client) publishRaw(ctx context.Context, topic, key string, partition int32, payload []byte) (PublishResult, error) {
	if topic == "" {
		return PublishResult{}, errors.New("agentbus: topic is required")
	}
	res, err := c.api.Publish(ctx, &pb.PublishRequest{
		Topic:     topic,
		Key:       key,
		Partition: partition,
		Payload:   payload,
	})
	if err != nil {
		return PublishResult{}, err
	}
	return PublishResult{Partition: res.Partition, Offset: res.Offset}, nil
}

// AgentEvent describes a structured event from a multi-agent system. The
// triple (Tenant, Project, SessionID) selects a stable partition, so events
// for the same session are always delivered in order.
type AgentEvent struct {
	// Routing — required.
	Tenant    string
	Project   string
	SessionID string
	AgentID   string

	// What happened — required.
	Type string // "tool.call", "tool.result", "token.chunk", "handoff", ...

	// Optional context.
	Step    string // pipeline step name
	Attempt int    // 1-based retry attempt
	Payload []byte // application data (JSON object recommended)

	// Optional metadata. Zero values are filled in by the SDK.
	Version   string    // defaults to "v1"
	CreatedAt time.Time // defaults to time.Now() in UTC
}

// DefaultAgentTopic is the conventional destination topic for agent events.
const DefaultAgentTopic = "agent-events"

// PublishAgent emits a structured agent event. The session triple
// (Tenant/Project/SessionID) is used as the routing key, so all events for
// the same session land on the same partition in order.
//
// The event is encoded as the standard JSON envelope:
//
//	{"version":"v1","type":"...","tenant":"...","project":"...",
//	 "session_id":"...","agent_id":"...","step":"...","attempt":N,
//	 "created_at":"<RFC3339Nano>","payload":<raw JSON>}
//
// If Payload is empty, "{}" is used. If Payload is set, it must be valid
// JSON (the broker will accept any bytes, but downstream tooling expects
// JSON to parse it).
func (c *Client) PublishAgent(ctx context.Context, ev AgentEvent) (PublishResult, error) {
	return c.PublishAgentTo(ctx, DefaultAgentTopic, ev)
}

// PublishAgentTo is like PublishAgent but lets the caller override the
// destination topic. Useful for DLQ flows (e.g. "agent-events.dlq").
func (c *Client) PublishAgentTo(ctx context.Context, topic string, ev AgentEvent) (PublishResult, error) {
	if err := validateAgentEvent(ev); err != nil {
		return PublishResult{}, err
	}
	encoded, err := encodeAgentEvent(ev)
	if err != nil {
		return PublishResult{}, err
	}
	key := sessionKey(ev.Tenant, ev.Project, ev.SessionID)
	return c.publishRaw(ctx, topic, key, -1, encoded)
}

func validateAgentEvent(ev AgentEvent) error {
	if strings.TrimSpace(ev.Type) == "" {
		return errors.New("agentbus: AgentEvent.Type is required")
	}
	if strings.TrimSpace(ev.Tenant) == "" {
		return errors.New("agentbus: AgentEvent.Tenant is required")
	}
	if strings.TrimSpace(ev.Project) == "" {
		return errors.New("agentbus: AgentEvent.Project is required")
	}
	if strings.TrimSpace(ev.SessionID) == "" {
		return errors.New("agentbus: AgentEvent.SessionID is required")
	}
	if strings.TrimSpace(ev.AgentID) == "" {
		return errors.New("agentbus: AgentEvent.AgentID is required")
	}
	return nil
}

// envelope mirrors the wire format the broker recognises. Defined here in
// the public SDK so callers don't have to import internal packages.
type envelope struct {
	Version   string          `json:"version"`
	Type      string          `json:"type"`
	Tenant    string          `json:"tenant"`
	Project   string          `json:"project"`
	SessionID string          `json:"session_id"`
	AgentID   string          `json:"agent_id"`
	Step      string          `json:"step,omitempty"`
	Attempt   int             `json:"attempt,omitempty"`
	CreatedAt string          `json:"created_at"`
	Payload   json.RawMessage `json:"payload"`
}

func encodeAgentEvent(ev AgentEvent) ([]byte, error) {
	version := ev.Version
	if version == "" {
		version = "v1"
	}
	createdAt := ev.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	payload := ev.Payload
	if len(payload) == 0 {
		payload = []byte(`{}`)
	}
	if !json.Valid(payload) {
		return nil, errors.New("agentbus: AgentEvent.Payload must be valid JSON")
	}
	return json.Marshal(envelope{
		Version:   version,
		Type:      ev.Type,
		Tenant:    ev.Tenant,
		Project:   ev.Project,
		SessionID: ev.SessionID,
		AgentID:   ev.AgentID,
		Step:      ev.Step,
		Attempt:   ev.Attempt,
		CreatedAt: createdAt.Format(time.RFC3339Nano),
		Payload:   payload,
	})
}

func sessionKey(tenant, project, sessionID string) string {
	return strings.TrimSpace(tenant) + "/" + strings.TrimSpace(project) + "/" + strings.TrimSpace(sessionID)
}
