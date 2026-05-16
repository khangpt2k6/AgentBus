# Retry & DLQ

Agent Bus has a **broker-native** retry policy. You don't need a sidecar, a CLI operator, or a separate worker — the broker itself decides whether a failed event gets re-published with `attempt+1` or routed to a dead-letter topic.

## The rule

```
if attempt + 1 > max-attempts:
    publish → <topic>.dlq (or --dlq-topic)
    metric:  goqueue_agent_event_dlq_total++
else:
    wait(delay)
    publish → <topic> with attempt = attempt + 1
    metric:  goqueue_agent_event_retries_total++
```

That's it. No exponential backoff state, no separate scheduler. The simplicity is the feature.

## How to trigger it

Two paths:

1. **Consumer reports failure** — when your consumer nacks an agent event, the broker applies the policy and decides where it goes next.
2. **CLI operator command** — useful for tests and one-offs:

```bash
goqueue retry-agent --grpc --addr localhost:9095 \
  --topic agent-events --max-attempts 3 --delay 2s \
  --event '{
    "version":"v1","type":"tool.call",
    "tenant":"acme","project":"support-bot","session_id":"sess-42",
    "agent_id":"planner","attempt":1,
    "created_at":"2026-04-03T10:00:00Z",
    "payload":{"tool":"search","query":"latest order status"}
  }'
```

`--max-attempts 3` means the event will be retried at most 3 times before going to DLQ.

## DLQ topic naming

By default the dead-letter topic is the original topic with a `.dlq` suffix:

| Original | DLQ |
|---|---|
| `agent-events` | `agent-events.dlq` |
| `orders` | `orders.dlq` |

Override with `--dlq-topic` if you want a different sink (e.g. centralized DLQ across topics).

## Observability

Three counters tell you the whole story:

| Metric | Meaning |
|---|---|
| `goqueue_agent_events_published_total` | every successful publish (originals + retries) |
| `goqueue_agent_event_retries_total` | events that were re-published with `attempt+1` |
| `goqueue_agent_event_dlq_total` | events that crossed `max-attempts` and went to DLQ |

A healthy system has `retries / published` low and steady; spikes mean a tool or agent is failing. `dlq_total > 0` is always worth investigating — these are events your consumers gave up on entirely.

## Designing consumers around it

- **Be idempotent.** A retried event has the same logical identity (`session_id`, `step`, etc.) but `attempt > 1`. Don't double-charge cards or double-call APIs.
- **Tune `max-attempts` per topic.** Token-chunk streams may want `max-attempts=1` (drop fast). Tool-call retries may want `3` or `5`.
- **Drain the DLQ.** Treat it as a queue, not a graveyard. Build a small consumer that re-injects or alerts on DLQ entries.
