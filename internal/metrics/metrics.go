package metrics

import (
	"net/http"
	"time"

	"github.com/khangpt2k6/EventBus/internal/agentstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	PublishedTotal      prometheus.Counter
	ConsumedTotal       prometheus.Counter
	ConsumerLag         *prometheus.GaugeVec
	PublishLatency      prometheus.Histogram
	AgentEventsTotal    *prometheus.CounterVec
	AgentRetriesTotal   *prometheus.CounterVec
	AgentDLQTotal       *prometheus.CounterVec
	AgentThrottledTotal prometheus.Counter
	RaftRole            *prometheus.GaugeVec
	RaftTerm            *prometheus.GaugeVec
	RaftLeader          *prometheus.GaugeVec
	RaftLeaderChanges   *prometheus.CounterVec
	PartitionFillPct    *prometheus.GaugeVec
	PartitionEvictions  *prometheus.GaugeVec

	CPActiveAgents     prometheus.Gauge
	CPSessions         *prometheus.GaugeVec
	CPHandoffsRouted   prometheus.Counter
	CPHandoffsUnrouted prometheus.Counter
	CPEscalations      prometheus.Counter

	WFEventsTotal          *prometheus.CounterVec
	WFExecutions           *prometheus.GaugeVec
	WFLeasesGranted        prometheus.Counter
	WFLeasesExpired        prometheus.Counter
	WFRetries              prometheus.Counter
	WFCompletionsRejected  prometheus.Counter
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
		AgentThrottledTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "goqueue_agent_events_throttled_total",
			Help: "Total agent events rejected by the per-tenant rate limiter (noisy-agent isolation).",
		}),
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
			Help: "Ring buffer fill percentage per topic and partition (0-100).",
		}, []string{"topic", "partition"}),
		PartitionEvictions: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "goqueue_partition_evictions_total",
			Help: "Cumulative messages evicted from the ring buffer per topic and partition.",
		}, []string{"topic", "partition"}),
		CPActiveAgents: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "goqueue_cp_active_agents",
			Help: "Agents the control plane considers active (not offline).",
		}),
		CPSessions: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "goqueue_cp_sessions",
			Help: "Agent sessions by control-plane run status (running/waiting/failed/completed).",
		}, []string{"status"}),
		CPHandoffsRouted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "goqueue_cp_handoffs_routed_total",
			Help: "Agent handoff events delivered to the target agent's inbox.",
		}),
		CPHandoffsUnrouted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "goqueue_cp_handoffs_unrouted_total",
			Help: "Agent handoff events that could not be delivered (no open inbox or full).",
		}),
		CPEscalations: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "goqueue_cp_escalations_total",
			Help: "Sessions auto-escalated to the escalation agent after a terminal error.",
		}),
		WFEventsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "goqueue_wf_events_total",
			Help: "Workflow state-transition events appended to the log by event type.",
		}, []string{"event_type"}),
		WFExecutions: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "goqueue_wf_executions",
			Help: "Workflow executions by status (pending/running/retrying/completed/failed).",
		}, []string{"status"}),
		WFLeasesGranted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "goqueue_wf_leases_granted_total",
			Help: "Task leases granted to workers.",
		}),
		WFLeasesExpired: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "goqueue_wf_leases_expired_total",
			Help: "Task leases reclaimed after their deadline passed without completion or heartbeat.",
		}),
		WFRetries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "goqueue_wf_retries_total",
			Help: "Workflow executions re-enqueued for another attempt.",
		}),
		WFCompletionsRejected: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "goqueue_wf_completions_rejected_total",
			Help: "Completion claims rejected by the exactly-once gate (stale attempt, terminal state, or duplicate).",
		}),
	}
	reg.MustRegister(
		m.PublishedTotal,
		m.ConsumedTotal,
		m.ConsumerLag,
		m.PublishLatency,
		m.AgentEventsTotal,
		m.AgentRetriesTotal,
		m.AgentDLQTotal,
		m.AgentThrottledTotal,
		m.RaftRole,
		m.RaftTerm,
		m.RaftLeader,
		m.RaftLeaderChanges,
		m.PartitionFillPct,
		m.PartitionEvictions,
		m.CPActiveAgents,
		m.CPSessions,
		m.CPHandoffsRouted,
		m.CPHandoffsUnrouted,
		m.CPEscalations,
		m.WFEventsTotal,
		m.WFExecutions,
		m.WFLeasesGranted,
		m.WFLeasesExpired,
		m.WFRetries,
		m.WFCompletionsRejected,
	)
	return m
}

// Control-plane metrics. These method signatures satisfy the
// controlplane.CPMetrics interface so a *Metrics can be passed straight in.

func (m *Metrics) SetActiveAgents(n int) { m.CPActiveAgents.Set(float64(n)) }

func (m *Metrics) SetSessions(status string, n int) {
	m.CPSessions.WithLabelValues(status).Set(float64(n))
}

func (m *Metrics) IncHandoffRouted() { m.CPHandoffsRouted.Inc() }

func (m *Metrics) IncHandoffUnrouted() { m.CPHandoffsUnrouted.Inc() }

func (m *Metrics) IncEscalation() { m.CPEscalations.Inc() }

// Workflow runtime metrics. These method signatures satisfy the
// workflow.Metrics interface so a *Metrics can be passed straight in.

func (m *Metrics) IncWorkflowEvent(evType string) { m.WFEventsTotal.WithLabelValues(evType).Inc() }

func (m *Metrics) SetWorkflowExecutions(status string, n int) {
	m.WFExecutions.WithLabelValues(status).Set(float64(n))
}

func (m *Metrics) IncWorkflowLeaseGranted() { m.WFLeasesGranted.Inc() }

func (m *Metrics) IncWorkflowLeaseExpired() { m.WFLeasesExpired.Inc() }

func (m *Metrics) IncWorkflowRetry() { m.WFRetries.Inc() }

func (m *Metrics) IncWorkflowCompletionRejected() { m.WFCompletionsRejected.Inc() }

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

// IncAgentThrottled records one agent event rejected by the per-tenant rate
// limiter that isolates noisy producers.
func (m *Metrics) IncAgentThrottled() {
	m.AgentThrottledTotal.Inc()
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
