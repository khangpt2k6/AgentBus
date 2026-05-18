// Package metadata implements the cluster's metadata control plane:
// a hashicorp/raft FSM whose committed state is the source of truth for
// cluster membership and shard leadership assignments.
package metadata

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

// Op identifies the mutation requested by a Raft log entry.
type Op string

const (
	OpAddMember      Op = "add_member"      // legacy: NodeID + Addr only (raft addr)
	OpRegisterMember Op = "register_member" // full record: NodeID + Addr + ClientAddr
	OpRemoveMember   Op = "remove_member"
	OpSetShardLeader Op = "set_shard_leader"
	OpSetShardCount  Op = "set_shard_count" // Shard carries the count
)

// Command is the JSON envelope serialized into every Raft log entry.
type Command struct {
	Op         Op     `json:"op"`
	NodeID     string `json:"node_id,omitempty"`
	Addr       string `json:"addr,omitempty"`
	ClientAddr string `json:"client_addr,omitempty"`
	Shard      uint32 `json:"shard,omitempty"`
}

// Member is a registered cluster node with all the addresses needed to
// route clients and replicate data.
type Member struct {
	NodeID     string
	RaftAddr   string
	ClientAddr string
}

// FSM is the in-memory projection of all committed Raft log entries.
type FSM struct {
	mu           sync.RWMutex
	members      map[string]Member
	shardLeaders map[uint32]string
	shardCount   uint32
}

func NewFSM() *FSM {
	return &FSM{
		members:      make(map[string]Member),
		shardLeaders: make(map[uint32]string),
	}
}

// Apply is called by Raft for every committed log entry on every node.
func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return fmt.Errorf("fsm: bad command: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch cmd.Op {
	case OpAddMember:
		if cmd.NodeID == "" {
			return fmt.Errorf("add_member: empty node_id")
		}
		existing := f.members[cmd.NodeID]
		f.members[cmd.NodeID] = Member{
			NodeID:     cmd.NodeID,
			RaftAddr:   cmd.Addr,
			ClientAddr: existing.ClientAddr,
		}
	case OpRegisterMember:
		if cmd.NodeID == "" {
			return fmt.Errorf("register_member: empty node_id")
		}
		f.members[cmd.NodeID] = Member{
			NodeID:     cmd.NodeID,
			RaftAddr:   cmd.Addr,
			ClientAddr: cmd.ClientAddr,
		}
	case OpRemoveMember:
		delete(f.members, cmd.NodeID)
		for s, nid := range f.shardLeaders {
			if nid == cmd.NodeID {
				delete(f.shardLeaders, s)
			}
		}
	case OpSetShardLeader:
		if cmd.NodeID == "" {
			return fmt.Errorf("set_shard_leader: empty node_id")
		}
		f.shardLeaders[cmd.Shard] = cmd.NodeID
	case OpSetShardCount:
		f.shardCount = cmd.Shard
	default:
		return fmt.Errorf("unknown op %q", cmd.Op)
	}
	return nil
}

// Members returns a copy of the raft-addr map for safe read by callers.
// Preserves the legacy NodeID -> RaftAddr shape used by existing callers
// (cluster.Cluster.Status()). For full records, use MemberAt.
func (f *FSM) Members() map[string]string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[string]string, len(f.members))
	for k, v := range f.members {
		out[k] = v.RaftAddr
	}
	return out
}

// MemberAt returns the full Member record for nodeID.
func (f *FSM) MemberAt(nodeID string) (Member, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	m, ok := f.members[nodeID]
	return m, ok
}

// ShardLeader returns the nodeID currently leading shardID, or "" if unset.
func (f *FSM) ShardLeader(shardID uint32) string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.shardLeaders[shardID]
}

// ShardCount returns the configured cluster-wide shard count.
func (f *FSM) ShardCount() uint32 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.shardCount
}

// AllShardLeaders returns a copy of the shard-leader map.
func (f *FSM) AllShardLeaders() map[uint32]string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[uint32]string, len(f.shardLeaders))
	for k, v := range f.shardLeaders {
		out[k] = v
	}
	return out
}

// snapshotPayload v2.
type snapshotPayload struct {
	Version      int               `json:"v"`
	Members      map[string]Member `json:"members"`
	ShardLeaders map[uint32]string `json:"shard_leaders"`
	ShardCount   uint32            `json:"shard_count"`
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	payload := snapshotPayload{
		Version:      2,
		Members:      make(map[string]Member, len(f.members)),
		ShardLeaders: make(map[uint32]string, len(f.shardLeaders)),
		ShardCount:   f.shardCount,
	}
	for k, v := range f.members {
		payload.Members[k] = v
	}
	for k, v := range f.shardLeaders {
		payload.ShardLeaders[k] = v
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return &fsmSnapshot{data: b}, nil
}

// Restore loads a snapshot. Tolerates v1 layout (members as map[string]string).
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	var probe struct {
		Version int `json:"v"`
	}
	_ = json.Unmarshal(data, &probe)

	f.mu.Lock()
	defer f.mu.Unlock()
	switch probe.Version {
	case 2:
		var p snapshotPayload
		if err := json.Unmarshal(data, &p); err != nil {
			return err
		}
		f.members = p.Members
		f.shardLeaders = p.ShardLeaders
		f.shardCount = p.ShardCount
	default:
		var p struct {
			Members      map[string]string `json:"members"`
			ShardLeaders map[uint32]string `json:"shard_leaders"`
		}
		if err := json.Unmarshal(data, &p); err != nil {
			return err
		}
		f.members = make(map[string]Member, len(p.Members))
		for nid, raftAddr := range p.Members {
			f.members[nid] = Member{NodeID: nid, RaftAddr: raftAddr}
		}
		f.shardLeaders = p.ShardLeaders
		f.shardCount = 0
	}
	if f.members == nil {
		f.members = make(map[string]Member)
	}
	if f.shardLeaders == nil {
		f.shardLeaders = make(map[uint32]string)
	}
	return nil
}

type fsmSnapshot struct{ data []byte }

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
