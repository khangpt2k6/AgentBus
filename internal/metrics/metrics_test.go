package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestMetricsRegistration(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)

	m.PublishedTotal.Inc()
	m.PublishedTotal.Inc()
	m.ConsumedTotal.Inc()
	m.ConsumerLag.WithLabelValues("orders", "payment-svc").Set(42)
	m.ObservePublishLatency(time.Now().Add(-5 * time.Millisecond))
	m.IncAgentEvent("agent-events", "tool.call")
	m.IncAgentRetry("agent-events", "tool.call")
	m.IncAgentDLQ("agent-events.dlq", "tool.call")
	m.SetRaftState("broker-1", "leader", "broker-1", 3)
	m.IncRaftLeaderChange("broker-1")

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}

	names := make(map[string]bool)
	for _, f := range families {
		names[f.GetName()] = true
	}

	want := []string{
		"goqueue_messages_published_total",
		"goqueue_messages_consumed_total",
		"goqueue_consumer_lag",
		"goqueue_publish_latency_seconds",
		"goqueue_agent_events_published_total",
		"goqueue_agent_event_retries_total",
		"goqueue_agent_event_dlq_total",
		"goqueue_raft_role",
		"goqueue_raft_term",
		"goqueue_raft_leader",
		"goqueue_raft_leader_changes_total",
	}
	for _, n := range want {
		if !names[n] {
			t.Errorf("metric %q not found in registry", n)
		}
	}
}

func TestObserveAgentPayload(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)

	payload := []byte(`{"version":"v1","type":"tool.call","tenant":"acme","project":"support","session_id":"sess-1","agent_id":"planner","attempt":2,"created_at":"2026-04-03T10:00:00Z","payload":{"tool":"search"}}`)
	m.ObserveAgentPayload("agent-events.dlq", payload)
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}

	assertCounterValue(t, families, "goqueue_agent_events_published_total", map[string]string{"topic": "agent-events.dlq", "event_type": "tool.call"}, 1)
	assertCounterValue(t, families, "goqueue_agent_event_retries_total", map[string]string{"topic": "agent-events.dlq", "event_type": "tool.call"}, 1)
	assertCounterValue(t, families, "goqueue_agent_event_dlq_total", map[string]string{"topic": "agent-events.dlq", "event_type": "tool.call"}, 1)
}

func assertCounterValue(t *testing.T, families []*dto.MetricFamily, name string, labels map[string]string, want float64) {
	t.Helper()
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		for _, metric := range fam.GetMetric() {
			if metric.GetCounter() == nil {
				continue
			}
			if labelsMatch(metric.GetLabel(), labels) {
				if got := metric.GetCounter().GetValue(); got != want {
					t.Fatalf("%s counter=%v want=%v", name, got, want)
				}
				return
			}
		}
	}
	t.Fatalf("counter %s with labels not found", name)
}

func labelsMatch(pairs []*dto.LabelPair, want map[string]string) bool {
	if len(pairs) != len(want) {
		return false
	}
	for _, p := range pairs {
		if want[p.GetName()] != p.GetValue() {
			return false
		}
	}
	return true
}
