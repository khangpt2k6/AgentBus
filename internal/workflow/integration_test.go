package workflow

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/khangpt2k6/AgentBus/agentbus"
	"github.com/khangpt2k6/AgentBus/internal/broker"
	"github.com/khangpt2k6/AgentBus/internal/consumer"
	"github.com/khangpt2k6/AgentBus/internal/grpcapi"
	"github.com/khangpt2k6/AgentBus/internal/wal"
	"google.golang.org/grpc"
)

// testBroker is a real in-process broker + gRPC stack with a real WAL, so
// the SDK worker path and the restart-recovery path run against the exact
// code the binary uses.
type testBroker struct {
	addr    string
	broker  *broker.Broker
	coord   *Coordinator
	poller  *Poller
	walPath string
	stop    func()
}

func startTestBroker(t *testing.T, walPath string) *testBroker {
	t.Helper()

	b := broker.New()
	if err := wal.ReplayWithOptions(walPath, wal.ReplayOptions{AllowPartialTail: true}, func(rec wal.Record) error {
		_, err := b.PublishToPartition(rec.Topic, int(rec.Partition), rec.Payload)
		return err
	}); err != nil {
		t.Fatalf("replay wal: %v", err)
	}
	logFile, err := wal.OpenWithOptions(walPath, wal.Options{SyncMode: wal.SyncNone})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	groups, err := consumer.NewManagerWithPath(filepath.Join(t.TempDir(), "offsets.json"))
	if err != nil {
		t.Fatalf("consumer manager: %v", err)
	}

	gApi := grpcapi.NewServer(b, groups, nil, logFile)
	coord := NewCoordinator(gApi,
		WithDefaults(3, 2*time.Second),
		WithSweepInterval(100*time.Millisecond),
	)
	poller := NewPoller(b, coord, Topic)
	poller.drainOnce() // rebuild state from any WAL-restored events
	coord.Start()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcSrv := grpc.NewServer()
	grpcapi.Register(grpcSrv, gApi)
	RegisterService(grpcSrv, NewService(coord, nil))
	go func() { _ = grpcSrv.Serve(lis) }()

	tb := &testBroker{
		addr:    lis.Addr().String(),
		broker:  b,
		coord:   coord,
		poller:  poller,
		walPath: walPath,
	}
	var stopOnce sync.Once
	tb.stop = func() {
		stopOnce.Do(func() {
			grpcSrv.Stop()
			coord.Stop()
			_ = logFile.Close()
			_ = groups.Close()
		})
	}
	t.Cleanup(tb.stop)
	return tb
}

// TestEndToEndWorkerCrashRetryAndRecovery drives the full loop through the
// real SDK: a worker that fails its first attempt, the coordinator retrying
// it, a second attempt completing - then a broker "restart" (new process
// state, same WAL) that must rebuild identical execution state from the log.
func TestEndToEndWorkerCrashRetryAndRecovery(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")
	tb := startTestBroker(t, walPath)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := agentbus.Connect(ctx, tb.addr)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	already, err := client.SubmitWorkflow(ctx, agentbus.WorkflowSpec{
		Tenant: "acme", Project: "etl", WorkflowID: "job-1",
		TaskType: "transform", Input: []byte(`{"rows":100}`),
		MaxAttempts: 3, LeaseTTL: 5 * time.Second,
	})
	if err != nil || already {
		t.Fatalf("submit: already=%v err=%v", already, err)
	}

	// The handler crashes (returns a retryable error) on attempt 1 and
	// succeeds on attempt 2.
	var attempts atomic.Int32
	workerCtx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		w := agentbus.NewWorker(client, "transform", func(_ context.Context, task agentbus.Task) ([]byte, error) {
			n := attempts.Add(1)
			if n == 1 {
				return nil, errors.New("simulated crash")
			}
			return []byte(fmt.Sprintf(`{"attempt":%d}`, task.Attempt)), nil
		}, agentbus.WithWorkerID("itest-worker"), agentbus.WithLeaseWait(500*time.Millisecond))
		_ = w.Run(workerCtx)
	}()

	waitFor(t, 15*time.Second, func() bool {
		x, ok, err := client.GetExecution(ctx, "acme", "etl", "job-1")
		return err == nil && ok && x.Status == "completed"
	}, "execution did not complete")
	stopWorker()
	<-workerDone

	x, ok, err := client.GetExecution(ctx, "acme", "etl", "job-1")
	if err != nil || !ok {
		t.Fatalf("get execution: ok=%v err=%v", ok, err)
	}
	if x.Attempt != 2 {
		t.Fatalf("expected completion on attempt 2, got %d", x.Attempt)
	}
	if string(x.Result) != `{"attempt":2}` {
		t.Fatalf("unexpected result: %s", x.Result)
	}

	// History rebuilt from the log tells the whole story in order.
	hist, ok, err := client.ExecutionHistory(ctx, "acme", "etl", "job-1")
	if err != nil || !ok {
		t.Fatalf("history: ok=%v err=%v", ok, err)
	}
	wantTypes := []string{EventSubmitted, EventLeased, EventFailed, EventRetry, EventLeased, EventCompleted}
	if len(hist) != len(wantTypes) {
		t.Fatalf("history length = %d, want %d (%+v)", len(hist), len(wantTypes), hist)
	}
	for i, want := range wantTypes {
		if hist[i].EventType != want {
			t.Fatalf("history[%d] = %s, want %s", i, hist[i].EventType, want)
		}
	}

	// "Restart": tear down the whole stack, then bring up a fresh one over
	// the same WAL. The poller must rebuild identical state from offset 0.
	live, _ := tb.coord.Store().Get("acme", "etl", "job-1")
	tb.stop()

	tb2 := startTestBroker(t, walPath)
	rebuilt, ok := tb2.coord.Store().Get("acme", "etl", "job-1")
	if !ok {
		t.Fatal("execution missing after restart")
	}
	if rebuilt.Status != StatusCompleted || rebuilt.Attempt != live.Attempt ||
		string(rebuilt.Result) != string(live.Result) || len(rebuilt.Transitions) != len(live.Transitions) {
		t.Fatalf("state diverged after restart\nlive:    %+v\nrebuilt: %+v", live, rebuilt)
	}
	for i := range live.Transitions {
		if live.Transitions[i] != rebuilt.Transitions[i] {
			t.Fatalf("transition %d diverged: %+v vs %+v", i, live.Transitions[i], rebuilt.Transitions[i])
		}
	}
}

// TestEndToEndLeaseExpiryReassignsToLiveWorker kills a worker mid-task (it
// leases and never acks) and verifies the coordinator reassigns the work to
// a second worker after the lease expires, and that the dead worker's late
// completion is fenced.
func TestEndToEndLeaseExpiryReassignsToLiveWorker(t *testing.T) {
	tb := startTestBroker(t, filepath.Join(t.TempDir(), "test.wal"))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := agentbus.Connect(ctx, tb.addr)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	if _, err := client.SubmitWorkflow(ctx, agentbus.WorkflowSpec{
		Tenant: "acme", Project: "etl", WorkflowID: "job-2",
		TaskType: "transform", MaxAttempts: 2, LeaseTTL: 500 * time.Millisecond,
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}

	// Directly lease through the coordinator, simulating a worker that dies
	// without heartbeating.
	lease, err := tb.coord.Lease(ctx, "transform", "dead-worker", 0)
	if err != nil || lease == nil || lease.Attempt != 1 {
		t.Fatalf("first lease: %+v err=%v", lease, err)
	}

	// The sweep loop reclaims the lease; a live worker picks it up.
	var lease2 *Lease
	waitFor(t, 10*time.Second, func() bool {
		lease2, err = tb.coord.Lease(ctx, "transform", "live-worker", 0)
		return err == nil && lease2 != nil
	}, "lease was never reassigned after expiry")
	if lease2.Attempt != 2 {
		t.Fatalf("reassigned lease attempt = %d, want 2", lease2.Attempt)
	}

	// The dead worker wakes up and tries to complete attempt 1: fenced.
	ok, dup, err := tb.coord.Complete(ctx, "acme", "etl", "job-2", "dead-worker", 1, []byte(`"stale"`))
	if err != nil || ok || dup {
		t.Fatalf("stale completion not fenced: ok=%v dup=%v err=%v", ok, dup, err)
	}
	ok, _, err = tb.coord.Complete(ctx, "acme", "etl", "job-2", "live-worker", 2, []byte(`"fresh"`))
	if err != nil || !ok {
		t.Fatalf("live completion rejected: ok=%v err=%v", ok, err)
	}
	x, _, _ := client.GetExecution(ctx, "acme", "etl", "job-2")
	if x.Status != "completed" || string(x.Result) != `"fresh"` {
		t.Fatalf("unexpected final state: %+v", x)
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal(msg)
}
