# OpenTelemetry session traces

AgentBus emits OTEL spans for every publish. Two integrations make these spans powerful for AI ops:

1. **Session attributes** on every span so you can find every event in a session with a single search.
2. **Session-derived trace_id** so events from a session that started without an upstream trace context still get grouped into one Jaeger/Tempo trace.

You get this for free — no agent-side instrumentation required.

---

## Searching in Jaeger / Tempo / Grafana Traces

Every broker span for an agent event carries these attributes:

| Attribute | Example |
|---|---|
| `agent.session.tenant` | `acme` |
| `agent.session.project` | `support-bot` |
| `agent.session.id` | `sess-42` |
| `agent.id` | `planner` |
| `agent.event.type` | `tool.call` |
| `agent.event.step` | `retrieve-context` |
| `agent.event.attempt` | `3` |

Open Jaeger and search:

```
service = goqueue.grpcapi
agent.session.id = sess-42
```

You see every Publish the broker handled for `sess-42`, ordered by time. Click any span for its full attributes including step, attempt, and partition.

This works **without modifying your agent code** — the broker introspects the envelope and tags the span automatically.

---

## Auto-derived trace IDs

When a producer publishes an agent event **without** propagating an OTEL trace context (e.g. a script, a cron job, a serverless function that didn't set up tracing), the broker synthesizes a parent SpanContext using:

```
trace_id = sha256("agentbus/session/" + tenant + "/" + project + "/" + session)[:16]
```

This is deterministic: every event in `sess-42` always lands under the same `trace_id`. In Grafana / Tempo you can derive the trace_id from a session id, paste it into the trace search, and see the whole session as one trace.

**Important caveat**: when a producer DOES propagate trace context (e.g. from an upstream HTTP request), we respect it and do NOT override. The producer's distributed trace stays intact. Synthesized session traces only kick in for "orphan" events.

---

## Compose-stack quickstart

The bundled `docker-compose.yml` already runs Tempo and the OTEL collector. After `docker compose up --build`:

| Service | URL |
|---|---|
| Grafana (Explore → Tempo) | http://localhost:3000 |
| Tempo | http://localhost:3200 |

Publish a few session events:

```bash
goqueue publish-agent --grpc --addr localhost:9095 \
  --tenant acme --project bot --session sess-trace-demo --agent planner \
  --type tool.call --payload '{"tool":"search"}'

goqueue publish-agent --grpc --addr localhost:9095 \
  --tenant acme --project bot --session sess-trace-demo --agent planner \
  --type tool.result --payload '{"results":42}'
```

In Grafana → Explore → Tempo → Search → "Service: goqueue.grpcapi" → filter by `agent.session.id=sess-trace-demo`. The two publishes appear as spans under one trace.

---

## Compute the session trace ID yourself

Want to construct the URL `http://grafana:3000/explore?query=<trace_id>` in alert templates? Same derivation in any language:

```python
import hashlib

def session_trace_id(tenant: str, project: str, session: str) -> str:
    key = f"agentbus/session/{tenant.strip()}/{project.strip()}/{session.strip()}"
    return hashlib.sha256(key.encode()).hexdigest()[:32]

print(session_trace_id("acme", "bot", "sess-42"))
# -> e.g. "2af1b03e5d7b8c9d0e1f2a3b4c5d6e7f"
```

Or in Go from the SDK:

```go
import "github.com/khangpt2k6/AgentBus/internal/agentstream"
// (Note: agentstream is internal; expose this from the public agentbus
// package in a future release if you need it.)
```

---

## Attribute coverage

Currently tagged spans:

| Span | When | Attributes |
|---|---|---|
| `BrokerService.Publish` (gRPC) | every Publish RPC | `topic`, `key`, `payload_bytes`, `requested_partition`, plus all agent attributes if envelope detected |
| `BrokerService.Consume` (gRPC) | every Consume stream | `topic`, `group`, `requested_partition`, `partition`, `consume.batch` events |
| `BrokerService.Fetch` (gRPC) | every Fetch RPC | `topic`, `partition`, `from_offset`, `max_count`, `returned`, `scanned`, `filtered` |

Future work: also tag Consume/Fetch spans with session attributes when the response contains agent envelopes.
