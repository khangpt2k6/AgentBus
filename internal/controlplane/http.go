package controlplane

import (
	"encoding/json"
	"net/http"
	"time"
)

// agentJSON / sessionJSON are the read-only HTTP shapes for control-plane state.
type agentJSON struct {
	ID             string    `json:"id"`
	Status         string    `json:"status"`
	Capabilities   []string  `json:"capabilities,omitempty"`
	CurrentSession string    `json:"current_session,omitempty"`
	LastSeen       time.Time `json:"last_seen"`
}

type sessionJSON struct {
	SessionKey   string    `json:"session_key"`
	ActiveAgent  string    `json:"active_agent,omitempty"`
	PendingAgent string    `json:"pending_agent,omitempty"`
	StepCount    int       `json:"step_count"`
	Status       string    `json:"status"`
	LastEventAt  time.Time `json:"last_event_at"`
}

// RegisterHTTP mounts read-only observability endpoints on the given mux:
//
//	GET /api/cp/agents              -> all registered agents
//	GET /api/cp/sessions            -> all session run-states
//	GET /api/cp/sessions?key=K      -> one session run-state (404 if unknown)
func (cp *ControlPlane) RegisterHTTP(mux *http.ServeMux) {
	mux.HandleFunc("/api/cp/agents", cp.handleAgents)
	mux.HandleFunc("/api/cp/sessions", cp.handleSessions)
}

func (cp *ControlPlane) handleAgents(w http.ResponseWriter, r *http.Request) {
	if !writeCORS(w, r) {
		return
	}
	agents := cp.ListAgents()
	out := make([]agentJSON, 0, len(agents))
	for _, a := range agents {
		out = append(out, agentJSON{
			ID:             a.ID,
			Status:         string(a.Status),
			Capabilities:   a.Capabilities,
			CurrentSession: a.CurrentSession,
			LastSeen:       a.LastSeen,
		})
	}
	writeJSON(w, out)
}

func (cp *ControlPlane) handleSessions(w http.ResponseWriter, r *http.Request) {
	if !writeCORS(w, r) {
		return
	}
	if k := r.URL.Query().Get("key"); k != "" {
		rs, ok := cp.sessions.Get(k)
		if !ok {
			http.Error(w, "session not found", http.StatusNotFound)
			return
		}
		writeJSON(w, toSessionJSON(rs))
		return
	}
	sessions := cp.ListSessions()
	out := make([]sessionJSON, 0, len(sessions))
	for _, rs := range sessions {
		out = append(out, toSessionJSON(rs))
	}
	writeJSON(w, out)
}

func toSessionJSON(rs RunState) sessionJSON {
	return sessionJSON{
		SessionKey:   rs.SessionKey,
		ActiveAgent:  rs.ActiveAgent,
		PendingAgent: rs.PendingAgent,
		StepCount:    rs.StepCount,
		Status:       string(rs.Status),
		LastEventAt:  rs.LastEventAt,
	}
}

// writeCORS mirrors the broker's /api/stats handler: allow cross-origin reads
// and answer preflight. Returns false if the request was a handled OPTIONS.
func writeCORS(w http.ResponseWriter, r *http.Request) bool {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, v any) {
	_ = json.NewEncoder(w).Encode(v)
}
