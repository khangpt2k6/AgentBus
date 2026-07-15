package workflow

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/khangpt2k6/AgentBus/internal/agentstream"
)

// logPublisher records every appended envelope in order, standing in for
// the durable log.
type logPublisher struct {
	mu  sync.Mutex
	log []agentstream.Event
	err error
}

func (p *logPublisher) PublishEnvelope(_ context.Context, _ string, env agentstream.Event) (int64, int32, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return 0, 0, p.err
	}
	p.log = append(p.log, env)
	return int64(len(p.log) - 1), 0, nil
}

func (p *logPublisher) events() []agentstream.Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]agentstream.Event(nil), p.log...)
}

// manualClock is a settable clock for driving lease expiry.
type manualClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *manualClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *manualClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newTestCoordinator(t *testing.T, clk *manualClock) (*Coordinator, *logPublisher) {
	t.Helper()
	pub := &logPublisher{}
	opts := []Option{WithDefaults(3, 10*time.Second)}
	if clk != nil {
		opts = append(opts, withClock(clk.now))
	}
	return NewCoordinator(pub, opts...), pub
}

func mustSubmit(t *testing.T, c *Coordinator, id string) {
	t.Helper()
	already, err := c.Submit(context.Background(), SubmitSpec{
		Tenant: "t", Project: "p", WorkflowID: id, TaskType: "work",
		Input: []byte(`{"n":1}`),
	})
	if err != nil || already {
		t.Fatalf("submit %s: already=%v err=%v", id, already, err)
	}
}

func TestSubmitLeaseCompleteLifecycle(t *testing.T) {
	c, _ := newTestCoordinator(t, nil)
	mustSubmit(t, c, "wf1")

	lease, err := c.Lease(context.Background(), "work", "worker-a", 0)
	if err != nil || lease == nil {
		t.Fatalf("lease: %v %v", lease, err)
	}
	if lease.Attempt != 1 || lease.WorkflowID != "wf1" {
		t.Fatalf("unexpected lease: %+v", lease)
	}

	accepted, dup, err := c.Complete(context.Background(), "t", "p", "wf1", "worker-a", 1, []byte(`"done"`))
	if err != nil || !accepted || dup {
		t.Fatalf("complete: accepted=%v dup=%v err=%v", accepted, dup, err)
	}

	x, ok := c.Store().Get("t", "p", "wf1")
	if !ok || x.Status != StatusCompleted || string(x.Result) != `"done"` {
		t.Fatalf("unexpected final state: %+v", x)
	}
}

func TestDuplicateSubmitIsIdempotent(t *testing.T) {
	c, pub := newTestCoordinator(t, nil)
	mustSubmit(t, c, "wf1")
	already, err := c.Submit(context.Background(), SubmitSpec{
		Tenant: "t", Project: "p", WorkflowID: "wf1", TaskType: "work",
	})
	if err != nil || !already {
		t.Fatalf("expected already-exists, got already=%v err=%v", already, err)
	}
	if got := len(pub.events()); got != 1 {
		t.Fatalf("duplicate submit appended an event: log has %d entries", got)
	}
}

func TestCompletionIsExactlyOnce(t *testing.T) {
	c, _ := newTestCoordinator(t, nil)
	mustSubmit(t, c, "wf1")
	if _, err := c.Lease(context.Background(), "work", "worker-a", 0); err != nil {
		t.Fatal(err)
	}

	const claimers = 16
	var wg sync.WaitGroup
	acceptedCount := int32(0)
	var mu sync.Mutex
	for i := 0; i < claimers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, _, err := c.Complete(context.Background(), "t", "p", "wf1", "worker-a", 1, []byte(`1`))
			if err != nil {
				t.Errorf("complete: %v", err)
			}
			if ok {
				mu.Lock()
				acceptedCount++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if acceptedCount != 1 {
		t.Fatalf("expected exactly 1 accepted completion, got %d", acceptedCount)
	}
}

func TestZombieWorkerIsFencedAfterRelease(t *testing.T) {
	clk := &manualClock{t: time.Unix(1000, 0)}
	c, _ := newTestCoordinator(t, clk)
	mustSubmit(t, c, "wf1")

	// worker-a takes attempt 1, then goes silent past the lease deadline.
	if _, err := c.Lease(context.Background(), "work", "worker-a", 0); err != nil {
		t.Fatal(err)
	}
	clk.advance(11 * time.Second)
	if n := c.SweepExpired(context.Background()); n != 1 {
		t.Fatalf("expected 1 expired lease, got %d", n)
	}

	// worker-b takes attempt 2.
	lease2, err := c.Lease(context.Background(), "work", "worker-b", 0)
	if err != nil || lease2 == nil || lease2.Attempt != 2 {
		t.Fatalf("second lease: %+v err=%v", lease2, err)
	}

	// The zombie's completion (attempt 1) must be rejected...
	ok, dup, err := c.Complete(context.Background(), "t", "p", "wf1", "worker-a", 1, []byte(`"zombie"`))
	if err != nil || ok || dup {
		t.Fatalf("zombie completion not fenced: ok=%v dup=%v err=%v", ok, dup, err)
	}
	// ...and worker-b's (attempt 2) accepted.
	ok, _, err = c.Complete(context.Background(), "t", "p", "wf1", "worker-b", 2, []byte(`"real"`))
	if err != nil || !ok {
		t.Fatalf("current attempt completion rejected: ok=%v err=%v", ok, err)
	}
	x, _ := c.Store().Get("t", "p", "wf1")
	if string(x.Result) != `"real"` {
		t.Fatalf("result overwritten by zombie: %q", x.Result)
	}
	// A late duplicate of the accepted completion reports duplicate.
	ok, dup, _ = c.Complete(context.Background(), "t", "p", "wf1", "worker-b", 2, []byte(`"again"`))
	if ok || !dup {
		t.Fatalf("expected duplicate report, got ok=%v dup=%v", ok, dup)
	}
}

func TestFailRetryableSchedulesRetryThenDies(t *testing.T) {
	c, _ := newTestCoordinator(t, nil)
	if _, err := c.Submit(context.Background(), SubmitSpec{
		Tenant: "t", Project: "p", WorkflowID: "wf1", TaskType: "work", MaxAttempts: 2,
	}); err != nil {
		t.Fatal(err)
	}

	// Attempt 1 fails retryably: execution re-enqueues.
	lease, _ := c.Lease(context.Background(), "work", "w", 0)
	accepted, willRetry, err := c.Fail(context.Background(), "t", "p", "wf1", "w", lease.Attempt, "boom", true)
	if err != nil || !accepted || !willRetry {
		t.Fatalf("first fail: accepted=%v willRetry=%v err=%v", accepted, willRetry, err)
	}

	// Attempt 2 fails retryably but attempts are exhausted: dead.
	lease2, _ := c.Lease(context.Background(), "work", "w", 0)
	if lease2 == nil || lease2.Attempt != 2 {
		t.Fatalf("expected attempt 2, got %+v", lease2)
	}
	accepted, willRetry, err = c.Fail(context.Background(), "t", "p", "wf1", "w", 2, "boom again", true)
	if err != nil || !accepted || willRetry {
		t.Fatalf("final fail: accepted=%v willRetry=%v err=%v", accepted, willRetry, err)
	}
	x, _ := c.Store().Get("t", "p", "wf1")
	if x.Status != StatusFailed {
		t.Fatalf("expected failed, got %s", x.Status)
	}
}

func TestFailNotRetryableDiesImmediately(t *testing.T) {
	c, _ := newTestCoordinator(t, nil)
	mustSubmit(t, c, "wf1")
	lease, _ := c.Lease(context.Background(), "work", "w", 0)
	accepted, willRetry, err := c.Fail(context.Background(), "t", "p", "wf1", "w", lease.Attempt, "bad input", false)
	if err != nil || !accepted || willRetry {
		t.Fatalf("fail: accepted=%v willRetry=%v err=%v", accepted, willRetry, err)
	}
	x, _ := c.Store().Get("t", "p", "wf1")
	if x.Status != StatusFailed || x.Error != "bad input" {
		t.Fatalf("unexpected state: %+v", x)
	}
}

func TestHeartbeatExtendsLeaseAndStaleHeartbeatRejected(t *testing.T) {
	clk := &manualClock{t: time.Unix(1000, 0)}
	c, _ := newTestCoordinator(t, clk)
	mustSubmit(t, c, "wf1")
	lease, _ := c.Lease(context.Background(), "work", "w", 0)

	clk.advance(8 * time.Second)
	valid, deadline, err := c.Heartbeat(context.Background(), "t", "p", "wf1", "w", lease.Attempt)
	if err != nil || !valid {
		t.Fatalf("heartbeat: valid=%v err=%v", valid, err)
	}
	if !deadline.After(lease.LeaseDeadline) {
		t.Fatalf("deadline not extended: %v <= %v", deadline, lease.LeaseDeadline)
	}
	// Wrong worker or attempt is rejected without touching state.
	valid, _, _ = c.Heartbeat(context.Background(), "t", "p", "wf1", "other", lease.Attempt)
	if valid {
		t.Fatal("heartbeat from wrong worker accepted")
	}
	valid, _, _ = c.Heartbeat(context.Background(), "t", "p", "wf1", "w", lease.Attempt+1)
	if valid {
		t.Fatal("heartbeat with wrong attempt accepted")
	}
}

func TestLeaseLongPollWakesOnSubmit(t *testing.T) {
	c, _ := newTestCoordinator(t, nil)
	type result struct {
		lease *Lease
		err   error
	}
	got := make(chan result, 1)
	go func() {
		l, err := c.Lease(context.Background(), "work", "w", 5*time.Second)
		got <- result{l, err}
	}()
	time.Sleep(50 * time.Millisecond) // let the worker park
	mustSubmit(t, c, "wf1")
	select {
	case r := <-got:
		if r.err != nil || r.lease == nil || r.lease.WorkflowID != "wf1" {
			t.Fatalf("long-poll lease: %+v err=%v", r.lease, r.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("long-poll lease did not wake on submit")
	}
}

func TestLeaseReturnsNilWhenQueueEmpty(t *testing.T) {
	c, _ := newTestCoordinator(t, nil)
	lease, err := c.Lease(context.Background(), "work", "w", 0)
	if err != nil || lease != nil {
		t.Fatalf("expected no work, got %+v err=%v", lease, err)
	}
}

// TestReplayDeterminism drives a full multi-attempt scenario through the
// coordinator, then folds the exact event log it produced into a fresh
// store, and asserts the rebuilt state is identical - the property the
// crash-recovery and failover paths rely on.
func TestReplayDeterminism(t *testing.T) {
	clk := &manualClock{t: time.Unix(1000, 0)}
	c, pub := newTestCoordinator(t, clk)

	// wf1: fails once, retries, completes on attempt 2.
	mustSubmit(t, c, "wf1")
	l, _ := c.Lease(context.Background(), "work", "w1", 0)
	if _, _, err := c.Fail(context.Background(), "t", "p", "wf1", "w1", l.Attempt, "transient", true); err != nil {
		t.Fatal(err)
	}
	l, _ = c.Lease(context.Background(), "work", "w2", 0)
	if valid, _, _ := c.Heartbeat(context.Background(), "t", "p", "wf1", "w2", l.Attempt); !valid {
		t.Fatal("heartbeat rejected")
	}
	if ok, _, _ := c.Complete(context.Background(), "t", "p", "wf1", "w2", l.Attempt, []byte(`42`)); !ok {
		t.Fatal("complete rejected")
	}

	// wf2: lease expires, retries, still running at snapshot time.
	mustSubmit(t, c, "wf2")
	if _, err := c.Lease(context.Background(), "work", "w3", 0); err != nil {
		t.Fatal(err)
	}
	clk.advance(11 * time.Second)
	c.SweepExpired(context.Background())
	if _, err := c.Lease(context.Background(), "work", "w4", 0); err != nil {
		t.Fatal(err)
	}

	// wf3: submitted, never leased.
	mustSubmit(t, c, "wf3")

	// Rebuild from the log alone, twice, and compare against live state.
	for round := 0; round < 2; round++ {
		rebuilt := NewStore()
		for _, ev := range pub.events() {
			rebuilt.Apply(ev)
		}
		for _, id := range []string{"wf1", "wf2", "wf3"} {
			live, ok1 := c.Store().Get("t", "p", id)
			replayed, ok2 := rebuilt.Get("t", "p", id)
			if !ok1 || !ok2 {
				t.Fatalf("round %d: %s missing (live=%v replayed=%v)", round, id, ok1, ok2)
			}
			if !reflect.DeepEqual(live, replayed) {
				t.Fatalf("round %d: %s diverged\nlive:     %+v\nreplayed: %+v", round, id, live, replayed)
			}
		}
	}
}

// TestReplayIdempotentRedelivery re-applies every event twice in place (the
// poller redelivers events the coordinator already folded synchronously)
// and asserts nothing changes.
func TestReplayIdempotentRedelivery(t *testing.T) {
	c, pub := newTestCoordinator(t, nil)
	mustSubmit(t, c, "wf1")
	l, _ := c.Lease(context.Background(), "work", "w", 0)
	if ok, _, _ := c.Complete(context.Background(), "t", "p", "wf1", "w", l.Attempt, []byte(`1`)); !ok {
		t.Fatal("complete rejected")
	}
	before, _ := c.Store().Get("t", "p", "wf1")
	for _, ev := range pub.events() {
		if c.Store().Apply(ev) {
			t.Fatalf("redelivered event mutated state: %s", ev.Type)
		}
	}
	after, _ := c.Store().Get("t", "p", "wf1")
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("state changed under redelivery\nbefore: %+v\nafter:  %+v", before, after)
	}
}

func TestSweepKillsExecutionOutOfAttempts(t *testing.T) {
	clk := &manualClock{t: time.Unix(1000, 0)}
	c, _ := newTestCoordinator(t, clk)
	if _, err := c.Submit(context.Background(), SubmitSpec{
		Tenant: "t", Project: "p", WorkflowID: "wf1", TaskType: "work", MaxAttempts: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Lease(context.Background(), "work", "w", 0); err != nil {
		t.Fatal(err)
	}
	clk.advance(11 * time.Second)
	c.SweepExpired(context.Background())
	x, _ := c.Store().Get("t", "p", "wf1")
	if x.Status != StatusFailed {
		t.Fatalf("expected failed after final lease expiry, got %s", x.Status)
	}
}

func TestFIFOOrderAcrossManyExecutions(t *testing.T) {
	c, _ := newTestCoordinator(t, nil)
	const n = 50
	for i := 0; i < n; i++ {
		mustSubmit(t, c, fmt.Sprintf("wf-%03d", i))
	}
	for i := 0; i < n; i++ {
		l, err := c.Lease(context.Background(), "work", "w", 0)
		if err != nil || l == nil {
			t.Fatalf("lease %d: %v", i, err)
		}
		want := fmt.Sprintf("wf-%03d", i)
		if l.WorkflowID != want {
			t.Fatalf("FIFO violated at %d: got %s want %s", i, l.WorkflowID, want)
		}
	}
}
