// Package replicator handles leader-side fan-out of new shardwal entries
// to a shard's followers. One Replicator instance per broker; Add and
// Drop are called by the cluster orchestration layer as shard leadership
// changes.
//
// Each follower gets its own independent shardwal Subscribe cursor rather than
// sharing one fan-out buffer. Subscribe does "backfill from disk at offset N,
// then live tail", so a follower that is behind (new, reconnecting, or picked
// up after a leadership change) is caught up automatically with no separate
// catch-up path and no possibility of a dropped entry wedging it. A slow
// follower exerts backpressure on its own goroutine only, never on the local
// append path or on other followers.
package replicator

import (
	"context"
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

// reconnectBackoff is how long a follower goroutine waits before redialing
// after a stream error, to avoid a tight loop on a persistently failing peer.
const reconnectBackoff = 200 * time.Millisecond

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

	shard, err := w.mgr.Shard(w.shardID)
	if err != nil {
		log.Printf("replicator shard %d: open: %v", w.shardID, err)
		return
	}

	var wg sync.WaitGroup

	// Self-updater: advance this leader's own HWM entry as it appends locally.
	// The leader is durable the instant Append returns, so self tracks the tail.
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.selfUpdateLoop(shard, hwm)
	}()

	// One independent goroutine + Subscribe cursor + stream per follower.
	for _, f := range w.followers {
		wg.Add(1)
		go func(f FollowerAddr) {
			defer wg.Done()
			w.followerLoop(f, shard, hwm)
		}(f)
	}

	wg.Wait()
}

// selfUpdateLoop keeps the leader's own HWM entry equal to its durable tail.
func (w *shardWorker) selfUpdateLoop(shard *shardwal.Shard, hwm *shardwal.HighWaterMark) {
	self := w.mgr.SelfID()
	start := shard.Tail()
	hwm.Update(self, start)

	ch, cancelSub := shard.Subscribe(w.ctx, start)
	defer cancelSub()
	for {
		select {
		case <-w.ctx.Done():
			return
		case rec, ok := <-ch:
			if !ok {
				return
			}
			hwm.Update(self, rec.Offset+1)
		}
	}
}

// followerLoop maintains replication to one follower, reconnecting on error
// until the worker is cancelled.
func (w *shardWorker) followerLoop(f FollowerAddr, shard *shardwal.Shard, hwm *shardwal.HighWaterMark) {
	for w.ctx.Err() == nil {
		err := w.runOneFollowerSession(f, shard, hwm)
		if w.ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Printf("replicator shard %d follower %s: %v", w.shardID, f.NodeID, err)
		}
		select {
		case <-w.ctx.Done():
			return
		case <-time.After(reconnectBackoff):
		}
	}
}

// runOneFollowerSession opens a stream to one follower and streams the shard
// from offset 0. The follower skips records it already has (idempotent), so a
// reconnect or leadership change re-streams from the follower's real position
// with no gap and no mismatch. Returns when the stream breaks or the worker is
// cancelled; nil on clean cancellation.
func (w *shardWorker) runOneFollowerSession(f FollowerAddr, shard *shardwal.Shard, hwm *shardwal.HighWaterMark) error {
	cl, err := transport.Dial(f.Addr)
	if err != nil {
		return err
	}
	defer cl.Close()

	// Session context so we can abort the stream (and unblock the ack receiver)
	// independently of, but subordinate to, the worker context.
	sessCtx, sessCancel := context.WithCancel(w.ctx)
	defer sessCancel()

	stream, err := cl.OpenReplicate(sessCtx)
	if err != nil {
		return err
	}

	// Ack receiver: feeds follower acks into the HWM. Exits when the stream
	// breaks or sessCtx is cancelled. recvErr is buffered so its final send
	// never blocks even if no one reads it.
	recvErr := make(chan error, 1)
	recvDone := make(chan struct{})
	go func() {
		defer close(recvDone)
		for {
			ack, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			hwm.Update(ack.NodeId, ack.LastOffset+1)
		}
	}()

	ch, cancelSub := shard.Subscribe(sessCtx, 0)
	defer cancelSub()

	var sendErr error
loop:
	for {
		select {
		case <-sessCtx.Done():
			break loop
		case e := <-recvErr:
			sendErr = e
			break loop
		case rec, ok := <-ch:
			if !ok {
				break loop
			}
			if err := stream.Send(&pb.AppendEntry{
				ShardId:      w.shardID,
				Offset:       rec.Offset,
				Payload:      rec.Payload,
				LeaderNodeId: w.mgr.SelfID(),
			}); err != nil {
				sendErr = err
				break loop
			}
		}
	}

	sessCancel()        // abort the stream so the ack receiver unblocks
	_ = stream.CloseSend()
	<-recvDone          // wait for the receiver goroutine to exit (no leak)

	if w.ctx.Err() != nil {
		return nil // worker cancelled (drop / leadership change): not an error
	}
	return sendErr
}
