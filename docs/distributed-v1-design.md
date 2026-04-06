# GoQueue Distributed v1 Design

## Goal
Ship a true replicated broker mode where multiple nodes form one logical queue system with leader-based writes and follower replication.

## Current baseline
- Multi-node Compose exists for local demos, but nodes are independent.
- `raft-*` fields currently represent runtime labels and dashboard state, not consensus behavior.
- WAL replay works per node only.

## v1 scope
- Leader/follower replication for publish path.
- Durable replicated log before client ACK (quorum commit).
- Follower catch-up from persisted log after restart.
- Read path from leader only in v1.
- Cluster membership fixed by config for v1.

## Milestones
1. Consensus substrate (raft state machine package, append/commit flow).
2. Replicated WAL (ACK only after quorum commit).
3. Broker integration (leader write routing and follower rejection/redirect).
4. Recovery and catch-up (snapshot/log replay and follower catch-up).
5. Operational surfaces (real leader/term/index metrics and readiness semantics).
6. Failure validation (leader crash, lagging follower, partition heal).

## Non-goals (v1)
- Dynamic membership changes.
- Exactly-once semantics.
- Cross-region replication.

## Exit criteria
- 3-node cluster runs as one logical broker.
- Committed writes survive leader restart.
- CI includes cluster integration tests for failover and replay consistency.
