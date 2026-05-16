# Show HN drafts

Three variants of the same post, optimized for different surfaces. Pick one when you're ready to ship.

---

## A. Hacker News — short and direct (recommended)

**Title**
```
Show HN: AgentBus – session-ordered event bus for AI agents with built-in replay
```
*(80 chars — under the HN cap. No emoji, no marketing words.)*

**Body** (paste in the comment box)

> Hi HN, I built AgentBus because debugging multi-agent AI workflows is painful.
>
> A multi-agent run produces a bursty stream of token chunks, tool calls, retries, and handoffs. When something fails at 3am you have one breadcrumb — a session id. Most teams pick between two bad options: a lightweight queue that's easy to run but hard to replay, or Kafka/RabbitMQ which is powerful but expensive to operate for this use case.
>
> AgentBus targets that gap. It's a single Go binary that:
>
> - Routes by `tenant/project/session` so per-conversation ordering is stable
> - Persists every event to a CRC32C'd write-ahead log
> - Ships with `goqueue session replay --session sess-42` — returns every event for that session, in order, ready to debug
> - Tags Prometheus + OpenTelemetry traces with `agent.session.id` so you can search by session in Jaeger/Tempo without instrumenting your agent code
> - Has a webhook subscriber (`goqueue webhook --url ...`) for non-Go consumers (Slack, PagerDuty, Lambda)
>
> The replay feature is the part I'm most curious to get feedback on. It's a self-hosted alternative to LangSmith-style traces — same workflow (session id in, full trace out) but you own the data and the runtime.
>
> Stack: Go, gRPC, TCP, CRC32C WAL, single-node today. Multi-node with replication is designed but not built (see `docs/distributed-v1-design.md`).
>
> Repo: https://github.com/khangpt2k6/AgentBus
> Docs: https://khangpt2k6.github.io/AgentBus/
> Quickstart:
>
>     curl -sSfL https://raw.githubusercontent.com/khangpt2k6/AgentBus/main/install.sh | sh
>     broker --grpc-addr=:9095
>     go get github.com/khangpt2k6/AgentBus/agentbus@latest
>
> Would love feedback on:
> 1. Is the session-replay flow useful enough on its own, or does it need a UI to be compelling?
> 2. Where would you draw the line between "agent event bus" and "generic durable queue"?
> 3. Any obvious holes in the WAL/replay design?

**Tips for posting**:
- Submit Tuesday–Thursday 8–11am PT for max visibility.
- Reply quickly to early comments — those drive most of the score.
- Have a friend or two ready to ask substantive questions, NOT upvote (mods can detect rings).

---

## B. Long-form blog post (Medium / dev.to / personal blog)

### Title options
- "Building AgentBus: a session-ordered event bus that makes debugging AI agents not awful"
- "I got tired of grep'ing through agent logs at 3am, so I built AgentBus"
- "AgentBus: an open-source alternative to LangSmith for self-hosters"

### Body (~700 words)

A few months ago I was on call for a support bot that uses a planner agent calling tools, sometimes handing off to a second agent for escalation. It failed at 3am. I had a session id (`sess-9f2c-...`) and a stack trace that said `attempt 3 failed in retrieve-context`.

What I didn't have: any way to see what happened in the other two attempts, what tools the planner tried, or what the handoff payload looked like. Our queue was a Redis Stream — fine for fan-out, useless for replay.

I tried the obvious things first.

**Option 1: send every agent event to a centralized log aggregator.** Works, but the log is unstructured. Filtering by session means I'm grep'ing JSON blobs and hoping I get the timestamps right.

**Option 2: pay for LangSmith / Helicone.** Solves the replay problem but ties our agent code to a cloud vendor's SDK, and our compliance team doesn't love it.

**Option 3: bolt session indexing onto Kafka.** Could work. Also requires running Kafka.

I wanted something in between: a queue that already understands the shape of an agent run, with replay as a first-class operation. I couldn't find one, so I built AgentBus.

#### What it is

AgentBus is a single Go binary. It's a partitioned event bus with one structural choice that changes everything downstream: events are routed by a `(tenant, project, session_id)` triple, hashed to a partition. Two events from the same session always land on the same partition. Within a partition, order is strictly preserved.

That tiny choice unlocks the rest. Because all events for `sess-42` live on one partition, "give me the trace for sess-42" is just "scan partition N from offset 0, filter to sess-42." That's the whole feature.

```bash
goqueue session replay --grpc --addr localhost:9095 \
  --tenant acme --project support-bot --session sess-42
```

```
[14:02:11.394] off=21084  tool.call      retrieve-context  agent=planner
[14:02:11.811] off=21085  tool.result    retrieve-context  agent=planner
[14:02:14.118] off=21088  tool.call      retrieve-context  (attempt 3)  agent=planner
[14:02:14.613] off=21090  handoff                          agent=planner
```

You have the run. You can fix the bug.

#### Things I'm proud of

**Group-commit WAL.** Every event hits a write-ahead log before consumers see it. To keep fsync from killing throughput, concurrent writers are coalesced into a single fsync per "generation" — one writer holds the lock during the slow syscall while others fill the next batch.

**WAL records carry CRC32C.** A flipped bit on disk doesn't silently corrupt your replay.

**OpenTelemetry session tracing without agent-side glue.** The broker peeks at the envelope JSON and tags every Publish span with `agent.session.id`, `agent.event.type`, etc. Search by session in Jaeger or Tempo. If the producer didn't propagate trace context, the broker synthesizes a deterministic `trace_id` from the session id — same session always groups into one trace, even for orphan events.

**Webhook subscriber.** `goqueue webhook --url https://hooks.slack.com/...` POSTs every event to an HTTP endpoint with retry, backoff, and standard tagging headers. Non-Go consumers (serverless functions, third-party SaaS, no-code platforms) become first-class without speaking gRPC.

#### What it's not

It's not Kafka. Single-node today, no cross-node replication. There's a [distributed-v1 design doc](https://github.com/khangpt2k6/AgentBus/blob/main/docs/distributed-v1-design.md) but it's not built. If your workload is multi-megabit per partition with strict cross-DC failover, AgentBus is the wrong tool.

It's also not magic. Replay reads from the WAL ring buffer — once the buffer wraps, older events are gone. For long-term storage you ship to S3 / your warehouse, same as any other event bus.

#### Try it

```bash
docker run -d -p 9095:9095 ghcr.io/khangpt2k6/goqueue:latest --grpc-addr=:9095
go get github.com/khangpt2k6/AgentBus/agentbus@latest
```

Source: [github.com/khangpt2k6/AgentBus](https://github.com/khangpt2k6/AgentBus)
Docs: [khangpt2k6.github.io/AgentBus](https://khangpt2k6.github.io/AgentBus/)

I'd genuinely love feedback — on the design, on what's missing, on whether the session-replay flow is enough or needs a UI to land. File an issue or reply here.

---

## C. /r/golang — "I made this" flavor

**Title**
```
[Show] AgentBus - a session-ordered event bus in Go, with built-in agent run replay
```

**Body**

> Built this over the past few weeks. It's a single Go binary that acts as an event bus, with one twist: events are routed by `tenant/project/session` so per-conversation ordering is preserved across the whole run. That makes the "give me the entire trace of session X" operation cheap — which is the main thing I built it for.
>
> Stack: standard library + gRPC + a hand-rolled WAL with group-commit fsync and CRC32C. No external dependencies at runtime. ~5k lines.
>
> Some Go-specific bits I'm happy with:
>
> - Group-commit pattern via `sync.Cond` so concurrent writers share one fsync per generation. The hot path under lock is just buffered writes; the slow `f.Sync()` happens with the mutex released.
> - WAL replay handles v1, v2, and v3 record formats in the same scanner (v3 added a CRC32C trailer). Backward-compatible upgrades.
> - SDK uses `iter.Seq`... actually no, it uses an iterator-style `Subscription.Next(ctx) (Message, error)` because `iter.Seq` doesn't compose well with `ctx`. Boring is good.
> - Public SDK at `github.com/khangpt2k6/AgentBus/agentbus`. Internal packages are firmly under `internal/`.
>
> Repo: https://github.com/khangpt2k6/AgentBus
> Docs: https://khangpt2k6.github.io/AgentBus/
>
> Curious for code review and bikeshedding on the API surface. The replay flow is what I want feedback on most.

---

## Pre-launch checklist

Before you post:

- [ ] GHCR image is public (Settings → Packages → goqueue → Visibility = Public)
- [ ] `go get github.com/khangpt2k6/AgentBus/agentbus@latest` actually works (Go proxy may need 5–10 min after a release to index)
- [ ] Latest docs deploy is live (https://khangpt2k6.github.io/AgentBus/)
- [ ] README has the install one-liner front and center
- [ ] You have ~2 hours blocked off to reply to comments
- [ ] You've drafted answers to the obvious questions:
  - "How is this different from Kafka / NATS / Redis Streams?"
  - "Why not just use LangSmith?"
  - "What's the scale ceiling per node?"
  - "What's the licensing? (MIT)"
- [ ] You have a 30-second screencast or terminal-cast (`asciinema`) of the replay flow ready to paste as a reply

Good luck. The bar on HN is honest writing, an interesting technical choice, and a working demo. AgentBus has all three.
