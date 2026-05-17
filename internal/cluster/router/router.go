// Package router is the read-side facade used by gRPC handlers to decide
// whether to serve a session-keyed publish locally or redirect the client
// to the current shard leader. The router holds no mutable state of its
// own; it composes:
//
//   - Ring: hash sessionKey -> shardID
//   - FSM:  shardID -> NodeID (leader), NodeID -> ClientAddr
//   - Membership: liveness check ("is that leader alive right now?")
//
// All methods are safe for concurrent use because the underlying
// components are.
package router

import (
	"strings"

	"github.com/khangpt2k6/AgentBus/internal/cluster/metadata"
	"github.com/khangpt2k6/AgentBus/internal/cluster/ring"
)

// LivenessChecker is the minimum membership-like interface the router needs.
type LivenessChecker interface {
	IsAlive(nodeID string) bool
}

// Decision is the result of routing one session.
type Decision struct {
	ShardID          uint32
	LeaderNodeID     string // "" if no leader is currently assigned or alive
	LeaderClientAddr string // empty if LeaderNodeID == ""
	IsLocal          bool   // true if this node is the leader
}

// Router is the read-side routing facade.
type Router struct {
	selfNodeID string
	fsm        *metadata.FSM
	live       LivenessChecker
	r          *ring.Ring
}

// New builds a router for `selfNodeID` over the given subsystems.
func New(selfNodeID string, fsm *metadata.FSM, live LivenessChecker, r *ring.Ring) *Router {
	return &Router{selfNodeID: selfNodeID, fsm: fsm, live: live, r: r}
}

// SessionKey is the deterministic key used for shard hashing.
// Mirrors agentstream.SessionKey but lives here to avoid an import cycle.
func SessionKey(tenant, project, sessionID string) string {
	return strings.TrimSpace(tenant) + "/" + strings.TrimSpace(project) + "/" + strings.TrimSpace(sessionID)
}

// RouteSession is the primary entry point. It hashes the session to a
// shard, looks up the current leader, and returns a Decision the gRPC
// handler can act on.
func (rt *Router) RouteSession(tenant, project, sessionID string) Decision {
	count := rt.fsm.ShardCount()
	if count == 0 {
		return Decision{}
	}
	key := SessionKey(tenant, project, sessionID)
	shard := hashToShard(key, count)

	leader := rt.fsm.ShardLeader(shard)
	if leader == "" || !rt.live.IsAlive(leader) {
		return Decision{ShardID: shard}
	}

	if leader == rt.selfNodeID {
		return Decision{ShardID: shard, LeaderNodeID: leader, IsLocal: true}
	}

	m, ok := rt.fsm.MemberAt(leader)
	if !ok || m.ClientAddr == "" {
		return Decision{ShardID: shard, LeaderNodeID: leader}
	}
	return Decision{ShardID: shard, LeaderNodeID: leader, LeaderClientAddr: m.ClientAddr}
}

// Ring exposes the underlying ring for the assigner.
func (rt *Router) Ring() *ring.Ring { return rt.r }

// FSM exposes the underlying FSM for the assigner.
func (rt *Router) FSM() *metadata.FSM { return rt.fsm }

func hashToShard(key string, count uint32) uint32 {
	h := fnv32a(key)
	return h % count
}

func fnv32a(s string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime32
	}
	return h
}
