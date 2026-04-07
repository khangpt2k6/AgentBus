package agentstream

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Event is a standard message envelope for multi-agent streaming workloads.
type Event struct {
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

// SessionKey keeps event ordering stable per tenant/project/session.
func SessionKey(tenant, project, sessionID string) string {
	return strings.TrimSpace(tenant) + "/" + strings.TrimSpace(project) + "/" + strings.TrimSpace(sessionID)
}

func (e Event) Validate() error {
	if strings.TrimSpace(e.Type) == "" {
		return fmt.Errorf("event type is required")
	}
	if strings.TrimSpace(e.Tenant) == "" {
		return fmt.Errorf("tenant is required")
	}
	if strings.TrimSpace(e.Project) == "" {
		return fmt.Errorf("project is required")
	}
	if strings.TrimSpace(e.SessionID) == "" {
		return fmt.Errorf("session_id is required")
	}
	if strings.TrimSpace(e.AgentID) == "" {
		return fmt.Errorf("agent_id is required")
	}
	return nil
}

func (e Event) Marshal() ([]byte, error) {
	out := e
	if out.Version == "" {
		out.Version = "v1"
	}
	if out.CreatedAt == "" {
		out.CreatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if out.Payload == nil {
		out.Payload = json.RawMessage(`{}`)
	}
	if err := out.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(out)
}
