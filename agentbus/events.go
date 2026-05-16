package agentbus

import (
	"context"
	"encoding/json"
	"time"
)

// Standard AgentEvent.Type values. AgentBus does not enforce these — any
// string works — but using the constants keeps producers and consumers in
// sync, and makes session-replay tooling readable.
const (
	EventTypeToolCall    = "tool.call"
	EventTypeToolResult  = "tool.result"
	EventTypeTokenChunk  = "token.chunk"
	EventTypeHandoff     = "handoff"
	EventTypeThinkStart  = "think.start"
	EventTypeThinkEnd    = "think.end"
	EventTypeError       = "error"
	EventTypeComplete    = "complete"
)

// ToolCall describes an agent invoking a tool. Use PublishToolCall to send.
type ToolCall struct {
	Tool      string          `json:"tool"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
	CallID    string          `json:"call_id,omitempty"` // correlates with ToolResult
}

// PublishToolCall is sugar over PublishAgent for the common tool-invocation
// event. The session triple still drives ordering.
func (c *Client) PublishToolCall(ctx context.Context, sess SessionRef, call ToolCall) (PublishResult, error) {
	body, err := json.Marshal(call)
	if err != nil {
		return PublishResult{}, err
	}
	return c.PublishAgent(ctx, AgentEvent{
		Tenant: sess.Tenant, Project: sess.Project, SessionID: sess.SessionID,
		AgentID: sess.AgentID, Type: EventTypeToolCall, Step: call.Tool,
		Payload: body,
	})
}

// ToolResult is the corresponding completion event for a ToolCall.
type ToolResult struct {
	CallID  string          `json:"call_id,omitempty"`
	Tool    string          `json:"tool"`
	Output  json.RawMessage `json:"output,omitempty"`
	Error   string          `json:"error,omitempty"`
	Latency time.Duration   `json:"latency,omitempty"`
}

func (c *Client) PublishToolResult(ctx context.Context, sess SessionRef, result ToolResult) (PublishResult, error) {
	body, err := json.Marshal(result)
	if err != nil {
		return PublishResult{}, err
	}
	return c.PublishAgent(ctx, AgentEvent{
		Tenant: sess.Tenant, Project: sess.Project, SessionID: sess.SessionID,
		AgentID: sess.AgentID, Type: EventTypeToolResult, Step: result.Tool,
		Payload: body,
	})
}

// TokenChunk is one streamed chunk from an LLM. Many of these typically
// arrive per session; consumers should expect high volume.
type TokenChunk struct {
	Text  string `json:"text"`
	Index int    `json:"index,omitempty"`
	Done  bool   `json:"done,omitempty"`
}

func (c *Client) PublishTokenChunk(ctx context.Context, sess SessionRef, chunk TokenChunk) (PublishResult, error) {
	body, err := json.Marshal(chunk)
	if err != nil {
		return PublishResult{}, err
	}
	return c.PublishAgent(ctx, AgentEvent{
		Tenant: sess.Tenant, Project: sess.Project, SessionID: sess.SessionID,
		AgentID: sess.AgentID, Type: EventTypeTokenChunk,
		Payload: body,
	})
}

// Handoff signals one agent passing control to another within the same session.
type Handoff struct {
	FromAgent string          `json:"from_agent"`
	ToAgent   string          `json:"to_agent"`
	Reason    string          `json:"reason,omitempty"`
	Context   json.RawMessage `json:"context,omitempty"`
}

func (c *Client) PublishHandoff(ctx context.Context, sess SessionRef, h Handoff) (PublishResult, error) {
	body, err := json.Marshal(h)
	if err != nil {
		return PublishResult{}, err
	}
	return c.PublishAgent(ctx, AgentEvent{
		Tenant: sess.Tenant, Project: sess.Project, SessionID: sess.SessionID,
		AgentID: sess.AgentID, Type: EventTypeHandoff,
		Payload: body,
	})
}

// SessionRef is the routing-and-attribution metadata shared by every event
// in an agent run. Build it once and pass it to the Publish* helpers.
type SessionRef struct {
	Tenant    string
	Project   string
	SessionID string
	AgentID   string
}
