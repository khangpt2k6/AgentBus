package workflow

import (
	"sync"
	"time"

	"github.com/khangpt2k6/EventBus/internal/agentstream"
)

// Store holds every tracked execution plus the derived scheduling indexes:
// a FIFO pending queue per task type and long-poll wakeups for idle
// workers. All indexes are re-derived from the fold, so a store rebuilt by
// replaying the log schedules identically to one that lived through the
// original events.
type Store struct {
	mu     sync.Mutex
	execs  map[string]*Execution
	queues map[string][]string      // task type -> FIFO of execution keys (lazily compacted)
	queued map[string]bool          // execution key -> currently enqueued
	wake   map[string]chan struct{} // task type -> closed-and-replaced on new work

	// Incremental indexes so the hot paths never scan the whole map:
	// counts backs the gauges, running bounds the expiry sweep to
	// in-flight executions, and terminalQ retires old terminal entries
	// when retainTerminal is set (the log remains the source of truth;
	// this only bounds the in-memory index).
	counts         map[Status]int
	running        map[string]struct{}
	terminalQ      []string
	retainTerminal int // 0 = keep every terminal execution in memory
}

// NewStore returns an empty store.
func NewStore() *Store {
	return &Store{
		execs:   make(map[string]*Execution),
		queues:  make(map[string][]string),
		queued:  make(map[string]bool),
		wake:    make(map[string]chan struct{}),
		counts:  make(map[Status]int),
		running: make(map[string]struct{}),
	}
}

// Apply folds one envelope into the store and reports whether state
// changed. Unknown or stale events are deterministic no-ops.
func (s *Store) Apply(e agentstream.Event) bool {
	if !IsWorkflowEvent(e.Type) {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	key := agentstream.SessionKey(e.Tenant, e.Project, e.SessionID)
	x, ok := s.execs[key]
	if e.Type == EventSubmitted {
		if ok {
			return false // duplicate submit is an idempotent no-op
		}
		nx, valid := newExecution(e)
		if !valid {
			return false
		}
		s.execs[key] = nx
		s.counts[nx.Status]++
		s.reconcileQueueLocked(key, nx)
		return true
	}
	if !ok {
		return false
	}
	prev := x.Status
	changed := x.apply(e)
	if changed {
		if x.Status != prev {
			s.counts[prev]--
			s.counts[x.Status]++
			if prev == StatusRunning {
				delete(s.running, key)
			}
			if x.Status == StatusRunning {
				s.running[key] = struct{}{}
			}
			if x.Terminal() {
				s.terminalQ = append(s.terminalQ, key)
				s.evictTerminalLocked()
			}
		}
		s.reconcileQueueLocked(key, x)
	}
	return changed
}

// evictTerminalLocked retires the oldest terminal executions beyond the
// retention cap. Their full history stays on the log; only the in-memory
// index forgets them.
func (s *Store) evictTerminalLocked() {
	if s.retainTerminal <= 0 {
		return
	}
	for len(s.terminalQ) > s.retainTerminal {
		key := s.terminalQ[0]
		s.terminalQ = s.terminalQ[1:]
		x, ok := s.execs[key]
		if !ok || !x.Terminal() {
			continue
		}
		s.counts[x.Status]--
		delete(s.execs, key)
		delete(s.queued, key)
	}
}

// reconcileQueueLocked keeps the pending queue consistent with the folded
// status. Dequeue is lazy: entries whose execution is no longer pending are
// skipped at pop time.
func (s *Store) reconcileQueueLocked(key string, x *Execution) {
	if x.Status == StatusPending && !s.queued[key] && !x.reserved {
		s.queues[x.TaskType] = append(s.queues[x.TaskType], key)
		s.queued[key] = true
		if ch, ok := s.wake[x.TaskType]; ok {
			close(ch)
			delete(s.wake, x.TaskType)
		}
	}
}

// ReserveNext pops the oldest pending execution of a task type and parks it
// (reserved) so no other lease can grab it while its wf.leased event is
// being appended. Returns nil when nothing is pending.
func (s *Store) ReserveNext(taskType string) *Execution {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.queues[taskType]
	for len(q) > 0 {
		key := q[0]
		q = q[1:]
		s.queued[key] = false
		x, ok := s.execs[key]
		if !ok || x.Status != StatusPending || x.reserved {
			continue // stale entry, lazily dropped
		}
		s.queues[taskType] = q
		x.reserved = true
		return x
	}
	s.queues[taskType] = q
	return nil
}

// Unreserve returns a reserved execution to the front of its queue, used
// when appending its lease event failed.
func (s *Store) Unreserve(x *Execution) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !x.reserved {
		return
	}
	x.reserved = false
	if x.Status == StatusPending && !s.queued[x.Key()] {
		s.queues[x.TaskType] = append([]string{x.Key()}, s.queues[x.TaskType]...)
		s.queued[x.Key()] = true
	}
}

// WaitChan returns a channel that closes when new work may be available for
// the task type. Callers re-check ReserveNext after it fires.
func (s *Store) WaitChan(taskType string) <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch, ok := s.wake[taskType]
	if !ok {
		ch = make(chan struct{})
		s.wake[taskType] = ch
	}
	return ch
}

// Get returns a copy of one execution's state.
func (s *Store) Get(tenant, project, workflowID string) (Execution, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	x, ok := s.execs[agentstream.SessionKey(tenant, project, workflowID)]
	if !ok {
		return Execution{}, false
	}
	return snapshot(x), true
}

// BeginComplete atomically validates a completion claim and fences the
// execution against concurrent completion calls for the same attempt.
// Returns (ok, duplicate): ok means the caller may append the completion
// event; duplicate means the execution already completed.
func (s *Store) BeginComplete(tenant, project, workflowID string, attempt int) (bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	x, ok := s.execs[agentstream.SessionKey(tenant, project, workflowID)]
	if !ok {
		return false, false
	}
	if x.Status == StatusCompleted {
		return false, true
	}
	if x.Terminal() || x.Attempt != attempt || attempt == 0 || x.completing {
		return false, false
	}
	x.completing = true
	return true, false
}

// AbortComplete releases the completion fence after a failed append.
func (s *Store) AbortComplete(tenant, project, workflowID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if x, ok := s.execs[agentstream.SessionKey(tenant, project, workflowID)]; ok {
		x.completing = false
	}
}

// ExpiredLeases returns copies of running executions whose lease deadline
// passed. The expiry sweep - unlike the fold - is allowed to consult the
// wall clock, because it initiates new events rather than replaying old
// ones. Bounded by the running index, not the store size.
func (s *Store) ExpiredLeases(now time.Time) []Execution {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Execution
	for key := range s.running {
		x, ok := s.execs[key]
		if !ok {
			delete(s.running, key)
			continue
		}
		if x.Status == StatusRunning && !x.reserved && !x.completing && x.LeaseDeadline.Before(now) {
			out = append(out, snapshot(x))
		}
	}
	return out
}

// CountByStatus returns execution counts by status, maintained
// incrementally so it is O(1) regardless of store size.
func (s *Store) CountByStatus() map[Status]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[Status]int, len(s.counts))
	for st, n := range s.counts {
		if n != 0 {
			out[st] = n
		}
	}
	return out
}

// List returns execution snapshots (optionally filtered by status, capped
// at limit) plus counts by status over the whole store. Only up to limit
// executions are copied.
func (s *Store) List(status Status, limit int) ([]Execution, map[Status]int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	counts := make(map[Status]int, len(s.counts))
	for st, n := range s.counts {
		if n != 0 {
			counts[st] = n
		}
	}
	var out []Execution
	for _, x := range s.execs {
		if status != "" && x.Status != status {
			continue
		}
		if limit > 0 && len(out) >= limit {
			break
		}
		out = append(out, snapshot(x))
	}
	return out, counts
}

// Len returns the number of tracked executions.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.execs)
}

// snapshot copies an execution, including its transition history, so
// callers can read it without holding the store lock.
func snapshot(x *Execution) Execution {
	c := *x
	c.Transitions = append([]Transition(nil), x.Transitions...)
	c.Input = append([]byte(nil), x.Input...)
	c.Result = append([]byte(nil), x.Result...)
	return c
}
