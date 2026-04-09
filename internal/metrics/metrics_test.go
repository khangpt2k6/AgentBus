package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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
