package controlplane

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func newTestServer(cp *ControlPlane) *httptest.Server {
	mux := http.NewServeMux()
	cp.RegisterHTTP(mux)
	return httptest.NewServer(mux)
}

func TestHTTPAgents(t *testing.T) {
	cp := New()
	cp.Register("planner", []string{"plan"})
	cp.Ingest(ev("tool.call", "planner", "s1"))

	srv := newTestServer(cp)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/cp/agents")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var agents []agentJSON
	if err := json.NewDecoder(resp.Body).Decode(&agents); err != nil {
		t.Fatal(err)
	}
	if len(agents) != 1 || agents[0].ID != "planner" || agents[0].Status != "busy" {
		t.Errorf("agents = %+v", agents)
	}
}

func TestHTTPSessionsAll(t *testing.T) {
	cp := New()
	cp.Ingest(ev("tool.call", "planner", "s1"))
	cp.Ingest(ev("complete", "planner", "s2"))

	srv := newTestServer(cp)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/cp/sessions")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var sessions []sessionJSON
	if err := json.NewDecoder(resp.Body).Decode(&sessions); err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 2 {
		t.Fatalf("want 2 sessions, got %d: %+v", len(sessions), sessions)
	}
}

func TestHTTPSessionByKey(t *testing.T) {
	cp := New()
	cp.Ingest(ev("tool.call", "planner", "s1"))

	srv := newTestServer(cp)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/cp/sessions?key=" + key("s1"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var rs sessionJSON
	if err := json.NewDecoder(resp.Body).Decode(&rs); err != nil {
		t.Fatal(err)
	}
	if rs.ActiveAgent != "planner" || rs.Status != "running" {
		t.Errorf("session = %+v", rs)
	}
}

func TestHTTPSessionByKeyNotFound(t *testing.T) {
	cp := New()
	srv := newTestServer(cp)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/cp/sessions?key=acme/bot/missing")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}
