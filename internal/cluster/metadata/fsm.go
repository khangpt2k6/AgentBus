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
	OpAddMember      Op = "add_member"
	OpRemoveMember   Op = "remove_member"
	OpSetShardLeader Op = "set_shard_leader"
)

// Command is the JSON envelope serialized into every Raft log entry.
// Keeping the wire format JSON keeps debugging easy; switch to protobuf
// only if FSM throughput becomes a bottleneck (it won't in v1).
type Command struct {
	Op     Op     `json:"op"`
	NodeID string `json:"node_id,omitempty"`
	Addr   string `json:"addr,omitempty"`
	Shard  uint32 `json:"shard,omitempty"`
}

// FSM is the in-memory projection of all committed Raft log entries.
type FSM struct {
	mu           sync.RWMutex
	members      map[string]string // nodeID -> raft addr
	shardLeaders map[uint32]string // shardID -> nodeID
}

// NewFSM creates an empty FSM ready for use.
func NewFSM() *FSM {
	return &FSM{
		members:      make(map[string]string),
		shardLeaders: make(map[uint32]string),
	}
}

// Apply is called by Raft for every committed log entry on every node.
// Must be deterministic and side-effect-free outside the FSM itself.
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
		f.members[cmd.NodeID] = cmd.Addr
	case OpRemoveMember:
		delete(f.members, cmd.NodeID)
		// Drop any shard leadership held by the removed node.
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
	default:
		return fmt.Errorf("unknown op %q", cmd.Op)
	}
	return nil
}

// Members returns a copy of the members map for safe read by callers.
func (f *FSM) Members() map[string]string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[string]string, len(f.members))
	for k, v := range f.members {
		out[k] = v
	}
	return out
}

// ShardLeader returns the nodeID currently leading shardID, or "" if unset.
func (f *FSM) ShardLeader(shardID uint32) string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.shardLeaders[shardID]
}

// snapshotPayload is the on-disk format of an FSM snapshot.
type snapshotPayload struct {
	Members      map[string]string `json:"members"`
	ShardLeaders map[uint32]string `json:"shard_leaders"`
}

// Snapshot is invoked by Raft when it decides to compact the log.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	payload := snapshotPayload{
		Members:      make(map[string]string, len(f.members)),
		ShardLeaders: make(map[uint32]string, len(f.shardLeaders)),
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

// Restore loads a snapshot into the FSM, replacing all in-memory state.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var payload snapshotPayload
	if err := json.NewDecoder(rc).Decode(&payload); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.members = payload.Members
	f.shardLeaders = payload.ShardLeaders
	if f.members == nil {
		f.members = make(map[string]string)
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
