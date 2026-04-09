package metrics

import (
	"net/http"
	"time"

	"github.com/2006t/goqueue/internal/agentstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	PublishedTotal     prometheus.Counter
	ConsumedTotal      prometheus.Counter
	ConsumerLag        *prometheus.GaugeVec
	PublishLatency     prometheus.Histogram
	AgentEventsTotal   *prometheus.CounterVec
	AgentRetriesTotal  *prometheus.CounterVec
	AgentDLQTotal      *prometheus.CounterVec
	RaftRole           *prometheus.GaugeVec
	RaftTerm           *prometheus.GaugeVec
	RaftLeader         *prometheus.GaugeVec
	RaftLeaderChanges  *prometheus.CounterVec
	PartitionFillPct   *prometheus.GaugeVec
	PartitionEvictions *prometheus.GaugeVec
}

func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		PublishedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "goqueue_messages_published_total",
			Help: "Total number of published messages.",
		}),
		ConsumedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "goqueue_messages_consumed_total",
			Help: "Total number of consumed messages.",
		}),
		ConsumerLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "goqueue_consumer_lag",
			Help: "Current consumer lag by topic and group.",
		}, []string{"topic", "group"}),
		PublishLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "goqueue_publish_latency_seconds",
			Help:    "Publish handler latency in seconds.",
			Buckets: prometheus.DefBuckets,
		}),
		AgentEventsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "goqueue_agent_events_published_total",
			Help: "Total published agent events by topic and event type.",
		}, []string{"topic", "event_type"}),
		AgentRetriesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "goqueue_agent_event_retries_total",
			Help: "Total retried agent events by topic and event type.",
		}, []string{"topic", "event_type"}),
		AgentDLQTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "goqueue_agent_event_dlq_total",
			Help: "Total agent events routed to DLQ topics by topic and event type.",
		}, []string{"topic", "event_type"}),
		RaftRole: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "goqueue_raft_role",
			Help: "Current raft role of a node (one-hot by role label).",
		}, []string{"node_id", "role"}),
		RaftTerm: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "goqueue_raft_term",
			Help: "Current raft term by node.",
		}, []string{"node_id"}),
		RaftLeader: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "goqueue_raft_leader",
			Help: "Current leader id seen by node (always value 1 for active leader label).",
		}, []string{"node_id", "leader_id"}),
		RaftLeaderChanges: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "goqueue_raft_leader_changes_total",
			Help: "Number of observed raft leader changes per node.",
		}, []string{"node_id"}),
		PartitionFillPct: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "goqueue_partition_fill_pct",
			Help: "Ring buffer fill percentage per topic and partition (0–100).",
		}, []string{"topic", "partition"}),
		PartitionEvictions: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "goqueue_partition_evictions_total",
			Help: "Cumulative messages evicted from the ring buffer per topic and partition.",
		}, []string{"topic", "partition"}),
	}
	reg.MustRegister(
		m.PublishedTotal,
		m.ConsumedTotal,
		m.ConsumerLag,
		m.PublishLatency,
		m.AgentEventsTotal,
		m.AgentRetriesTotal,
		m.AgentDLQTotal,
		m.RaftRole,
		m.RaftTerm,
		m.RaftLeader,
		m.RaftLeaderChanges,
		m.PartitionFillPct,
		m.PartitionEvictions,
	)
	return m
}

func (m *Metrics) ObservePublishLatency(start time.Time) {
	m.PublishLatency.Observe(time.Since(start).Seconds())
}

func (m *Metrics) SetRaftState(nodeID, role, leaderID string, term int64) {
	roles := []string{"leader", "follower", "candidate", "standalone"}
	for _, r := range roles {
		val := 0.0
		if r == role {
			val = 1
		}
		m.RaftRole.WithLabelValues(nodeID, r).Set(val)
	}
	m.RaftTerm.WithLabelValues(nodeID).Set(float64(term))
	m.RaftLeader.WithLabelValues(nodeID, leaderID).Set(1)
}

func (m *Metrics) IncRaftLeaderChange(nodeID string) {
	m.RaftLeaderChanges.WithLabelValues(nodeID).Inc()
}

func (m *Metrics) SetPartitionFillPct(topic, partition string, pct float64) {
	m.PartitionFillPct.WithLabelValues(topic, partition).Set(pct)
}

func (m *Metrics) SetPartitionEvictions(topic, partition string, n float64) {
	m.PartitionEvictions.WithLabelValues(topic, partition).Set(n)
}

func (m *Metrics) IncAgentEvent(topic, eventType string) {
	m.AgentEventsTotal.WithLabelValues(topic, eventType).Inc()
}

func (m *Metrics) IncAgentRetry(topic, eventType string) {
	m.AgentRetriesTotal.WithLabelValues(topic, eventType).Inc()
}

func (m *Metrics) IncAgentDLQ(topic, eventType string) {
	m.AgentDLQTotal.WithLabelValues(topic, eventType).Inc()
}

// ObserveAgentPayload updates agent-event counters when payload matches
// a valid agentstream event envelope.
func (m *Metrics) ObserveAgentPayload(topic string, payload []byte) {
	ev, ok := agentstream.ParseEvent(payload)
	if !ok {
		return
	}
	m.IncAgentEvent(topic, ev.Type)
	if ev.Attempt > 1 {
		m.IncAgentRetry(topic, ev.Type)
	}
	if agentstream.IsDLQTopic(topic) {
		m.IncAgentDLQ(topic, ev.Type)
	}
}

func Handler(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}
