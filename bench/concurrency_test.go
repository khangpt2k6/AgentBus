package bench

// Concurrency-focused measurement harness.
//
// These are report-style tests (gated by GOQUEUE_BENCH=1, like the throughput
// reports in bench_test.go) that produce concrete, citable numbers:
//
//   - How many concurrent TCP connections the broker actually sustains, and the
//     aggregate throughput / per-publish p99 at each connection count.
//   - The same sweep over concurrent gRPC clients (the primary API).
//   - An A/B of durable WAL appends: a naive "fsync-per-write" baseline vs the
//     group-commit log, which is what backs any "reduced p99 write latency"
//     claim.
//
// Run:
//   GOQUEUE_BENCH=1 go test ./bench -run TestConcurrentConnectionsReport -count=1 -v
//   GOQUEUE_BENCH=1 go test ./bench -run TestGRPCConcurrentClientsReport  -count=1 -v
//   GOQUEUE_BENCH=1 go test ./bench -run TestWALGroupCommitLatencyReport  -count=1 -v

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/khangpt2k6/EventBus/internal/protocol"
	"github.com/khangpt2k6/EventBus/internal/wal"
	goqueuev1 "github.com/khangpt2k6/EventBus/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestConcurrentConnectionsReport opens a sweep of simultaneous TCP connections,
// each doing sequential publish -> ack round-trips, and reports the aggregate
// throughput and per-publish latency at every connection count. The broker uses
// goroutine-per-connection with no fixed cap, so this measures how throughput
// scales (and where it stops scaling) with concurrent connections.
//
// Note on file descriptors: at N connections this needs ~2*N fds. If a high
// tier fails with "too many open files", raise the ulimit or trim connCounts.
func TestConcurrentConnectionsReport(t *testing.T) {
	if os.Getenv("GOQUEUE_BENCH") == "" {
		t.Skip("set GOQUEUE_BENCH=1 to run concurrent-connections report")
	}

	srv, addr := startTestServer(t)
	defer srv.Shutdown()

	connCounts := []int{1, 8, 16, 32, 64, 128, 256, 512, 1024, 2048}
	const perConn = 1000
	payload := make([]byte, 256)

	t.Logf("TCP concurrent-connection sweep (localhost, 256B, %d publishes/conn):", perConn)
	for _, nConns := range connCounts {
		var wg sync.WaitGroup
		latCh := make(chan []time.Duration, nConns)
		errCh := make(chan error, nConns)

		// Throttle the dial phase only (not the traffic phase). A single-process
		// loopback test otherwise fires every SYN at once, overflowing the OS
		// listen backlog - an artifact of the harness, not the server. Real
		// clients ramp up over time. Once connected, all conns pump concurrently.
		dialSem := make(chan struct{}, 64)

		start := time.Now()
		for c := 0; c < nConns; c++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				dialSem <- struct{}{}
				conn, err := dialWithRetry(addr)
				<-dialSem
				if err != nil {
					errCh <- err
					return
				}
				defer conn.Close()

				lats := make([]time.Duration, 0, perConn)
				for i := 0; i < perConn; i++ {
					s := time.Now()
					if err := protocol.Encode(conn, &protocol.Frame{
						Op:      protocol.OpPublish,
						Topic:   "conn-sweep",
						Payload: payload,
					}); err != nil {
						errCh <- err
						return
					}
					resp, err := protocol.Decode(conn)
					if err != nil {
						errCh <- err
						return
					}
					if resp.Op != protocol.OpAck {
						errCh <- fmt.Errorf("expected ACK, got op=%02x", resp.Op)
						return
					}
					lats = append(lats, time.Since(s))
				}
				latCh <- lats
			}()
		}
		wg.Wait()
		elapsed := time.Since(start)
		close(latCh)
		close(errCh)
		if err := <-errCh; err != nil {
			t.Fatalf("conns=%d: %v", nConns, err)
		}

		all := make([]time.Duration, 0, nConns*perConn)
		for lats := range latCh {
			all = append(all, lats...)
		}
		res := summarize(all, elapsed, nConns*perConn)
		t.Logf("  conns=%4d  total=%9d  %8s  -> %11.0f msgs/sec   p50=%-9s p99=%-9s",
			nConns, nConns*perConn, elapsed.Round(time.Millisecond), res.rate,
			quantile(res.sorted, 0.50).Round(time.Microsecond),
			quantile(res.sorted, 0.99).Round(time.Microsecond))
	}
}

// TestGRPCConcurrentClientsReport opens a sweep of simultaneous gRPC clients
// (each its own HTTP/2 connection) issuing unary Publish calls, and reports
// aggregate throughput and per-call latency. gRPC additionally multiplexes up
// to 100 streams per connection by default, so a single client could carry more
// in-flight work than the one stream used here.
func TestGRPCConcurrentClientsReport(t *testing.T) {
	if os.Getenv("GOQUEUE_BENCH") == "" {
		t.Skip("set GOQUEUE_BENCH=1 to run gRPC concurrent-clients report")
	}

	addr, stop := startTestGRPCServer(t)
	defer stop()

	clientCounts := []int{1, 8, 32, 64, 128}
	const perClient = 2000
	payload := make([]byte, 256)

	t.Logf("gRPC concurrent-client sweep (localhost, 256B, %d publishes/client):", perClient)
	for _, nClients := range clientCounts {
		var wg sync.WaitGroup
		latCh := make(chan []time.Duration, nClients)
		errCh := make(chan error, nClients)

		start := time.Now()
		for c := 0; c < nClients; c++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					errCh <- err
					return
				}
				defer conn.Close()
				client := goqueuev1.NewBrokerServiceClient(conn)

				// Warm up the lazy HTTP/2 connection so the first timed call
				// does not pay the one-time dial/handshake cost.
				if _, err := client.Publish(context.Background(), &goqueuev1.PublishRequest{
					Topic:   "grpc-conn-sweep",
					Payload: payload,
				}); err != nil {
					errCh <- err
					return
				}

				lats := make([]time.Duration, 0, perClient)
				for i := 0; i < perClient; i++ {
					s := time.Now()
					if _, err := client.Publish(context.Background(), &goqueuev1.PublishRequest{
						Topic:   "grpc-conn-sweep",
						Payload: payload,
					}); err != nil {
						errCh <- err
						return
					}
					lats = append(lats, time.Since(s))
				}
				latCh <- lats
			}()
		}
		wg.Wait()
		elapsed := time.Since(start)
		close(latCh)
		close(errCh)
		if err := <-errCh; err != nil {
			t.Fatalf("clients=%d: %v", nClients, err)
		}

		all := make([]time.Duration, 0, nClients*perClient)
		for lats := range latCh {
			all = append(all, lats...)
		}
		res := summarize(all, elapsed, nClients*perClient)
		t.Logf("  clients=%4d  total=%9d  %8s  -> %11.0f msgs/sec   p50=%-9s p99=%-9s",
			nClients, nClients*perClient, elapsed.Round(time.Millisecond), res.rate,
			quantile(res.sorted, 0.50).Round(time.Microsecond),
			quantile(res.sorted, 0.99).Round(time.Microsecond))
	}
}

// TestWALGroupCommitLatencyReport is the A/B that backs any "reduced p99 write
// latency" claim. For each writer concurrency it runs the SAME durable-append
// workload two ways on real disk:
//
//	naive    - one fsync per write, serialized under a mutex (a WAL with no
//	           group commit: the Nth concurrent writer waits behind N fsyncs)
//	gc       - the project's group-commit Log in SyncAlways mode, which
//	           coalesces concurrent writers' fsyncs into one per generation
//
// It prints per-write p99 for both and the reduction. The group-commit win
// shows up at higher concurrency, which is exactly the regime a busy broker
// runs in. Use the measured number for the resume; do not assert a fixed %.
func TestWALGroupCommitLatencyReport(t *testing.T) {
	if os.Getenv("GOQUEUE_BENCH") == "" {
		t.Skip("set GOQUEUE_BENCH=1 to run WAL group-commit latency report")
	}

	concurrency := []int{1, 8, 32, 64}
	const perWriter = 600
	payload := make([]byte, 256)

	t.Logf("WAL durable-append A/B (real disk, 256B, %d appends/writer):", perWriter)
	t.Logf("%-8s | %-32s | %-32s | %s", "writers", "naive fsync-per-write", "group-commit (SyncAlways)", "win")
	for _, c := range concurrency {
		naive := runNaiveFsync(t, c, perWriter, payload)
		gc := runGroupCommit(t, c, perWriter, payload)

		nP99 := quantile(naive.sorted, 0.99)
		gP99 := quantile(gc.sorted, 0.99)
		p99cut := 0.0
		if nP99 > 0 {
			p99cut = (1 - float64(gP99)/float64(nP99)) * 100
		}
		tput := 0.0
		if naive.rate > 0 {
			tput = gc.rate / naive.rate
		}
		t.Logf("%-8d | p99=%-9s %10.0f w/s | p99=%-9s %10.0f w/s | p99 -%4.0f%%  %4.1fx tput",
			c,
			nP99.Round(time.Microsecond), naive.rate,
			gP99.Round(time.Microsecond), gc.rate,
			p99cut, tput)
	}
}

// dialWithRetry tolerates the brief connection refusals that happen when a
// burst of simultaneous dials overruns the OS listen backlog (seen on Windows
// at high connection counts). It retries the dial, not anything stateful.
func dialWithRetry(addr string) (net.Conn, error) {
	var lastErr error
	for attempt := 0; attempt < 400; attempt++ {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		time.Sleep(5 * time.Millisecond)
	}
	return nil, lastErr
}

// latResult holds a sorted latency sample plus the aggregate rate.
type latResult struct {
	sorted []time.Duration
	rate   float64
}

func summarize(all []time.Duration, elapsed time.Duration, total int) latResult {
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	rate := 0.0
	if elapsed > 0 {
		rate = float64(total) / elapsed.Seconds()
	}
	return latResult{sorted: all, rate: rate}
}

// runNaiveFsync models a WAL with no group commit: every append flushes and
// fsyncs while holding a global lock, so concurrent writers serialize behind
// each other's fsync.
func runNaiveFsync(t *testing.T, writers, perWriter int, payload []byte) latResult {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(t.TempDir(), "naive.wal"), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	rec := make([]byte, 8+len(payload)) // 8-byte header + payload, ~ a WAL record
	copy(rec[8:], payload)

	var mu sync.Mutex
	var wg sync.WaitGroup
	latCh := make(chan []time.Duration, writers)

	start := time.Now()
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lats := make([]time.Duration, 0, perWriter)
			for i := 0; i < perWriter; i++ {
				s := time.Now()
				mu.Lock()
				_, werr := f.Write(rec)
				if werr == nil {
					werr = f.Sync()
				}
				mu.Unlock()
				if werr != nil {
					t.Error(werr)
					return
				}
				lats = append(lats, time.Since(s))
			}
			latCh <- lats
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	close(latCh)

	all := make([]time.Duration, 0, writers*perWriter)
	for lats := range latCh {
		all = append(all, lats...)
	}
	return summarize(all, elapsed, writers*perWriter)
}

// runGroupCommit drives the project's group-commit WAL in SyncAlways mode.
func runGroupCommit(t *testing.T, writers, perWriter int, payload []byte) latResult {
	t.Helper()
	w, err := wal.OpenWithOptions(filepath.Join(t.TempDir(), "groupcommit.wal"), wal.Options{SyncMode: wal.SyncAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	var wg sync.WaitGroup
	latCh := make(chan []time.Duration, writers)

	start := time.Now()
	for k := 0; k < writers; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lats := make([]time.Duration, 0, perWriter)
			for i := 0; i < perWriter; i++ {
				s := time.Now()
				if err := w.Append("gc-bench", payload); err != nil {
					t.Error(err)
					return
				}
				lats = append(lats, time.Since(s))
			}
			latCh <- lats
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	close(latCh)

	all := make([]time.Duration, 0, writers*perWriter)
	for lats := range latCh {
		all = append(all, lats...)
	}
	return summarize(all, elapsed, writers*perWriter)
}
