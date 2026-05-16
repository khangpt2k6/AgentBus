# Debug an agent run

When a multi-agent workflow goes sideways at 3am, you have one thing: a session ID. AgentBus turns that into a full chronological trace — every tool call, every retry, every handoff — without paying for a hosted observability vendor.

This is the workflow that distinguishes AgentBus from a generic queue.

---

## The story

Your support bot processed `sess-42` and produced wrong output. You have logs that say "agent failed at step retrieve-context, attempt 3". You want to see the *entire* run, in order.

```bash
goqueue session replay \
  --grpc --addr localhost:9095 \
  --tenant acme --project support-bot --session sess-42
```

Output:

```
[14:02:11.394] off=21084  tool.call      retrieve-context  agent=planner
    {"tool":"search","query":"latest order"}
[14:02:11.811] off=21085  tool.result    retrieve-context  agent=planner
    {"results":[],"latency_ms":417}
[14:02:11.812] off=21086  tool.call      retrieve-context  (attempt 2)  agent=planner
    {"tool":"search","query":"order acme-1042"}
[14:02:12.114] off=21087  tool.result    retrieve-context  (attempt 2)  agent=planner
    {"error":"timeout"}
[14:02:14.118] off=21088  tool.call      retrieve-context  (attempt 3)  agent=planner
    {"tool":"search","query":"order acme-1042","db":"cold-storage"}
[14:02:14.612] off=21089  tool.result    retrieve-context  (attempt 3)  agent=planner
    {"error":"connection refused"}
[14:02:14.613] off=21090  handoff                                       agent=planner
    {"from_agent":"planner","to_agent":"escalator","reason":"3 failed attempts"}

7 event(s) for session sess-42
```

You now know:
- Search agent tried 3 times
- Cold-storage DB was down
- Handed off to escalator

Take that trace, fix the DB issue, re-run.

---

## How it works under the hood

Every agent event lands on a partition selected by `hash(tenant/project/session)`. Same session → same partition → ordering preserved. The new `Fetch` gRPC reads from arbitrary offsets without touching consumer-group state, so replay doesn't disturb live consumers.

The CLI:
1. Computes which partition holds the session
2. Scans from offset 0 (or `--from`) in pages
3. Filters envelopes that match the triple
4. Pretty-prints chronologically

For a session with 30 events on a partition that has 30M total events, the scan reads 30M envelopes server-side but only **returns** the 30 to you. (A future optimization would push the filter to the broker; today client-side filter is fine because envelopes are small.)

---

## Use it from your own code

The same flow is available via the SDK:

```go
import "github.com/khangpt2k6/AgentBus/agentbus"

client, _ := agentbus.Connect(ctx, "localhost:9095")
defer client.Close()

events, err := client.ReplaySession(ctx, agentbus.SessionRef{
    Tenant:    "acme",
    Project:   "support-bot",
    SessionID: "sess-42",
}, agentbus.ReplayOptions{})

for _, ev := range events {
    fmt.Printf("[%s] %s %s\n", ev.CreatedAt, ev.Type, ev.Payload)
}
```

`DecodedEvent` is the envelope already parsed for you — `Type`, `Step`, `Attempt`, `Payload` as `json.RawMessage`.

---

## Live tail

For watching an active run:

```bash
goqueue session tail --grpc --addr localhost:9095 \
  --tenant acme --project support-bot --session sess-42
```

Or programmatically:

```go
sub, err := client.TailSession(ctx, sess, agentbus.ReplayOptions{})
defer sub.Close()
for {
    ev, err := sub.Next(ctx)
    if errors.Is(err, agentbus.ErrSubscriptionClosed) { break }
    // ...
}
```

---

## Typed events

The SDK ships helpers for the common event types, so your code stays readable:

```go
sess := agentbus.SessionRef{
    Tenant:    "acme",
    Project:   "support-bot",
    SessionID: "sess-42",
    AgentID:   "planner",
}

client.PublishToolCall(ctx, sess, agentbus.ToolCall{
    Tool:      "search",
    CallID:    "call-001",
    Arguments: json.RawMessage(`{"query":"latest order"}`),
})

client.PublishToolResult(ctx, sess, agentbus.ToolResult{
    CallID:  "call-001",
    Tool:    "search",
    Output:  json.RawMessage(`{"hits":42}`),
    Latency: 320 * time.Millisecond,
})

client.PublishHandoff(ctx, sess, agentbus.Handoff{
    FromAgent: "planner",
    ToAgent:   "escalator",
    Reason:    "3 failed attempts",
})

client.PublishTokenChunk(ctx, sess, agentbus.TokenChunk{
    Text:  "I'll search",
    Index: 0,
})
```

Under the hood these all call `PublishAgent` with the right `Type` and Marshal'd payload. Constants are defined as `EventTypeToolCall`, `EventTypeToolResult`, etc.

---

## Output formats

```bash
# Human-readable (default)
goqueue session replay ... --format pretty

# JSONL — one event per line, pipeable into jq / your log store
goqueue session replay ... --format jsonl | jq 'select(.Type == "tool.call")'
```

---

## Caveats

- **Partition count must match the broker's**: pass `--partitions N` (or `ReplayOptions{PartitionCount: N}`) if you've overridden the broker's default.
- **Ring-buffer eviction**: replays only see what's still in the topic. If the ring has wrapped, older events are gone. Check `events[0].Offset` vs the partition's head if you suspect truncation.
- **No server-side filter yet**: a session in a topic with 100M unrelated events still scans the whole partition (~MB per page). Practically fine; flagged here so you know the cost shape.
