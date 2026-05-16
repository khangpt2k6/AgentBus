# Webhook subscriber

Integrate non-Go consumers (or anything that speaks HTTP) by having AgentBus POST each event to a URL. Bridge between AgentBus and Slack, PagerDuty, AWS Lambda URLs, n8n, Zapier — whatever consumes JSON over HTTPS.

## Run it

```bash
goqueue webhook \
  --grpc --addr localhost:9095 \
  --topic agent-events \
  --group slack-alerts \
  --url https://hooks.slack.com/services/T0/B0/xyz
```

That's a long-running consumer process. It subscribes to `agent-events` under group `slack-alerts` (so it resumes from the last commit if restarted) and POSTs every message to the Slack webhook.

## Request shape

| | |
|---|---|
| Method | `POST` |
| `Content-Type` | `application/json` |
| Body | the event payload (the envelope JSON for agent events) |

Headers added on every request:

| Header | Meaning |
|---|---|
| `X-Agentbus-Offset` | broker offset on the partition |
| `X-Agentbus-Partition` | partition number |
| `X-Agentbus-Timestamp` | RFC3339Nano UTC |
| `X-Agentbus-Attempt` | delivery attempt (1 on first try) |
| `X-Agentbus-Tenant` / `Project` / `Session` / `Type` | extracted from the envelope if present |

Extra headers via `--header 'Key: Value'` (repeatable) — useful for auth:

```bash
goqueue webhook --url https://api.example.com/hooks/agentbus \
  --header 'Authorization: Bearer s3cr3t' \
  --header 'X-Source: agentbus-prod'
```

## Retry semantics

| Response | Action |
|---|---|
| 2xx | success, advance offset |
| 4xx (except 408 / 429) | permanent — log and skip |
| 408, 429, 5xx, network error | retry with exponential backoff |

After `--max-attempts` (default 5) consecutive failures, the event is dropped and the consumer group advances. This prevents a dead endpoint from wedging the entire pipeline. Configure with `--max-attempts 0` to drop on the first failure or set a large value if you want longer perseverance.

Backoff doubles each attempt, starting at `--backoff` (default 500ms), capped at 30s.

## Production patterns

### Idempotent consumers

Retries happen. The consumer endpoint should treat `(X-Agentbus-Partition, X-Agentbus-Offset)` as a deduplication key.

### Fan out to many endpoints

Run one `goqueue webhook` per destination, each with its own consumer group. Each maintains its own progress, so a slow endpoint can't slow the others.

```bash
goqueue webhook --group slack-alerts --url https://hooks.slack.com/... &
goqueue webhook --group pagerduty   --url https://events.pagerduty.com/... &
goqueue webhook --group dwh-export  --url https://intake.dwh.example.com/... &
```

### Filter by event type at the consumer

The broker doesn't filter by `Type` server-side (yet). Receive everything and discard at the consumer:

```python
# Flask example
@app.post("/agentbus")
def on_event():
    if request.headers.get("X-Agentbus-Type") != "tool.result":
        return "skip", 200  # ack, but no-op
    process(request.json)
    return "ok", 200
```

### Slack format

Slack expects a specific JSON shape. Map at the consumer:

```js
// Vercel/Netlify function
export default async (req, res) => {
  const ev = req.body;
  await fetch(process.env.SLACK_URL, {
    method: "POST",
    body: JSON.stringify({
      text: `${ev.type} (${ev.agent_id}) on session ${ev.session_id}`
    })
  });
  res.status(200).end();
};
```

---

## Comparison with raw subscribe

```
goqueue consume --grpc ...      # raw gRPC stream, you write the consumer
goqueue webhook --grpc ...      # HTTP fan-out, you write a webhook endpoint
```

Same broker semantics (consumer group offsets, partition routing). Webhook is the right call when the consumer is already an HTTP service, or when you can't run Go in the consumer's environment.
