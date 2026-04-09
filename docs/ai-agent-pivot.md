# AI Agent Pivot Plan

## Objective

Position `GoQueue` as a session-ordered event bus for multi-agent AI workloads with lower operational overhead than heavyweight broker stacks.

## Codebase Assessment

### Strengths already present

- `internal/broker`: partitioned routing and key-based partition selection
- `internal/wal`: WAL replay and record metadata (`topic`, `key`, `partition`)
- `internal/grpcapi`: typed streaming API for producer/consumer integration
- `internal/metrics` + `internal/telemetry`: Prometheus + OpenTelemetry baseline
- `cmd/goqueue`: simple CLI surface for experiments and demos

### Gaps for AI-native queue workloads

- no first-class delayed retry scheduling
- no dead-letter queue path
- no priority publish/consume policy
- no per-session metrics labels

## Phase Plan

### Phase 1 (now)

- standardize event envelope for agent traffic (`internal/agentstream`)
- route by session key (`tenant/project/session`)
- add CLI retry/DLQ operator command (`retry-agent`) for failed events
- add broker-side Prometheus counters for agent events/retries/DLQ
- document workload/problem framing in README

### Phase 2

- move delayed retry + dead-letter from CLI-level operator flow into broker-level policy
- add retry metadata propagation and max-attempt enforcement
- expose per-session lag/retry counters in metrics

### Phase 3

- add policy config per topic (`retention`, `max_attempt`, `retry_delay`)
- optimize memory pressure under high fan-out sessions
- publish AI-workload benchmark report and SLO guide

## Non-goals (current)

- replacing Kafka for all use cases
- implementing full distributed consensus in this phase
