// Package replicator handles leader-side fan-out of new shardwal entries
// to a shard's followers. One Replicator instance per broker; Add and
// Drop are called by the cluster orchestration layer as shard leadership
// changes.
package replicator

import (
	"context"
	"io"
	"log"
	"sync"
	"time"

	"github.com/khangpt2k6/AgentBus/internal/cluster/shardwal"
	"github.com/khangpt2k6/AgentBus/internal/cluster/transport"
	pb "github.com/khangpt2k6/AgentBus/proto"
)

// FollowerAddr identifies one replication peer.
type FollowerAddr struct {
	NodeID string
	Addr   string
}

// Replicator runs per-broker. Add(shardID, followers) starts streaming for
// a shard; Drop(shardID) stops it.
type Replicator struct {
	mgr *shardwal.Manager

	mu     sync.Mutex
	shards map[uint32]*shardWorker
}

// New builds a Replicator over the broker's shardwal Manager.
func New(mgr *shardwal.Manager) *Replicator {
	return &Replicator{
		mgr:    mgr,
		shards: make(map[uint32]*shardWorker),
	}
}

// Add starts replication of shardID to the provided followers. If a worker
// already exists for shardID, it's torn down and replaced.
func (r *Replicator) Add(ctx context.Context, shardID uint32, followers []FollowerAddr) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.shards[shardID]; ok {
		existing.cancel()
	}
	w := newShardWorker(ctx, r.mgr, shardID, followers)
	r.shards[shardID] = w
	go w.run()
}

// Drop stops replication of shardID. Safe to call if shardID isn't running.
func (r *Replicator) Drop(shardID uint32) {
	r.mu.Lock()
	w, ok := r.shards[shardID]
	delete(r.shards, shardID)
	r.mu.Unlock()
	if ok {
		w.cancel()
	}
}

// Close stops all workers.
func (r *Replicator) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, w := range r.shards {
		w.cancel()
	}
	r.shards = nil
}

type shardWorker struct {
	ctx       context.Context
	cancel    context.CancelFunc
	mgr       *shardwal.Manager
	shardID   uint32
	followers []FollowerAddr
}

func newShardWorker(parent context.Context, mgr *shardwal.Manager, shardID uint32, followers []FollowerAddr) *shardWorker {
	ctx, cancel := context.WithCancel(parent)
	return &shardWorker{
		ctx:       ctx,
		cancel:    cancel,
		mgr:       mgr,
		shardID:   shardID,
		followers: followers,
	}
}

func (w *shardWorker) run() {
	hwm := w.mgr.HWM(w.shardID)
	replicaIDs := []string{w.mgr.SelfID()}
	for _, f := range w.followers {
		replicaIDs = append(replicaIDs, f.NodeID)
	}
	hwm.SetReplicas(replicaIDs)

	// Local shardwal Subscribe — single source of all entries to fan out.
	shard, err := w.mgr.Shard(w.shardID)
	if err != nil {
		log.Printf("replicator shard %d: open: %v", w.shardID, err)
		return
	}
	ch, cancelSub := shard.Subscribe(w.ctx, 0)
	defer cancelSub()

	// Per-follower goroutines: each owns its connection + Replicate stream.
	type followerCh struct {
		entries chan *pb.AppendEntry
	}
	chans := make(map[string]followerCh)
	for _, f := range w.followers {
		fch := followerCh{entries: make(chan *pb.AppendEntry, 256)}
		chans[f.NodeID] = fch
		go w.followerLoop(f, fch.entries, hwm)
	}

	// Fan out every entry to all followers, and update self HWM as we
	// commit them locally. HWM uses "count of records ack'd" semantics
	// (offset+1), so a record at offset N satisfies WaitFor(N+1).
	for {
		select {
		case <-w.ctx.Done():
			for _, fc := range chans {
				close(fc.entries)
			}
			return
		case rec, ok := <-ch:
			if !ok {
				return
			}
			hwm.Update(w.mgr.SelfID(), rec.Offset+1)
			entry := &pb.AppendEntry{
				ShardId:      w.shardID,
				Offset:       rec.Offset,
				Payload:      rec.Payload,
				LeaderNodeId: w.mgr.SelfID(),
			}
			for _, fc := range chans {
				select {
				case fc.entries <- entry:
				default:
					// Buffer full — slow follower will fall behind; rely on
					// reconnect / catchup to recover.
				}
			}
		}
	}
}

// followerLoop maintains a single Replicate stream to one follower,
// reconnecting on error. Acks are funneled into the HWM tracker.
func (w *shardWorker) followerLoop(f FollowerAddr, entries chan *pb.AppendEntry, hwm *shardwal.HighWaterMark) {
	for w.ctx.Err() == nil {
		_ = w.runOneFollowerSession(f, entries, hwm)
		if w.ctx.Err() != nil {
			return
		}
		// Brief backoff before reconnect to avoid tight loop on persistent error.
		select {
		case <-w.ctx.Done():
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func (w *shardWorker) runOneFollowerSession(f FollowerAddr, entries chan *pb.AppendEntry, hwm *shardwal.HighWaterMark) error {
	cl, err := transport.Dial(f.Addr)
	if err != nil {
		return err
	}
	defer cl.Close()
	stream, err := cl.OpenReplicate(w.ctx)
	if err != nil {
		return err
	}
	// Receive acks in a goroutine, push to HWM.
	errCh := make(chan error, 1)
	go func() {
		for {
			ack, err := stream.Recv()
			if err == io.EOF {
				errCh <- nil
				return
			}
			if err != nil {
				errCh <- err
				return
			}
			hwm.Update(ack.NodeId, ack.LastOffset+1)
		}
	}()
	// Forward entries.
	for {
		select {
		case <-w.ctx.Done():
			_ = stream.CloseSend()
			<-errCh
			return nil
		case entry, ok := <-entries:
			if !ok {
				_ = stream.CloseSend()
				<-errCh
				return nil
			}
			if err := stream.Send(entry); err != nil {
				_ = stream.CloseSend()
				<-errCh
				return err
			}
		}
	}
}
