package bench

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/2006t/goqueue/internal/broker"
	"github.com/2006t/goqueue/internal/consumer"
	"github.com/2006t/goqueue/internal/protocol"
	"github.com/2006t/goqueue/internal/wal"
)

// --- In-memory publish throughput ---

func BenchmarkPublishInMemory(b *testing.B) {
	bk := broker.New()
	payload := make([]byte, 256)
	b.SetBytes(256)
	b.ReportAllocs()
	for b.Loop() {
		bk.Publish("bench", payload)
	}
}

func BenchmarkPublishInMemory1K(b *testing.B) {
	bk := broker.New()
	payload := make([]byte, 1024)
	b.SetBytes(1024)
	b.ReportAllocs()
	for b.Loop() {
		bk.Publish("bench", payload)
	}
}

func BenchmarkPublishWithKeyHashing(b *testing.B) {
	bk := broker.New()
	bk.EnsureTopic("bench-keyed", 12)
	payload := make([]byte, 256)
	b.SetBytes(256)
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = bk.PublishWithKey("bench-keyed", "user-42", payload)
	}
}

func BenchmarkPublishRoundRobinPartitions(b *testing.B) {
	bk := broker.New()
	bk.EnsureTopic("bench-rr", 12)
	payload := make([]byte, 256)
	b.SetBytes(256)
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = bk.PublishWithKey("bench-rr", "", payload)
	}
}

// --- Publish with WAL ---

func BenchmarkPublishWithWAL(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.wal")
	w, err := wal.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	bk := broker.New()
	payload := make([]byte, 256)
	b.SetBytes(256)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = w.Append("bench", payload)
		bk.Publish("bench", payload)
	}
}

// --- Protocol encode/decode ---

func BenchmarkProtocolEncodeDecode(b *testing.B) {
	frame := protocol.Frame{Op: protocol.OpPublish, Topic: "bench-topic", Payload: make([]byte, 256)}
	var buf bytes.Buffer
	b.SetBytes(int64(8 + len("bench-topic") + 256))
	b.ReportAllocs()
	for b.Loop() {
		buf.Reset()
		_ = protocol.Encode(&buf, &frame)
		_, _ = protocol.Decode(&buf)
	}
}

// --- Concurrent publish (goroutine contention) ---

func BenchmarkPublishConcurrent8(b *testing.B) {
	benchConcurrentPublish(b, 8)
}

func BenchmarkPublishConcurrent32(b *testing.B) {
	benchConcurrentPublish(b, 32)
}

func benchConcurrentPublish(b *testing.B, goroutines int) {
	bk := broker.New()
	payload := make([]byte, 128)
	b.SetBytes(128)
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bk.Publish("contention", payload)
		}
	})
}

// --- Publish → Subscribe end-to-end (in-process, no TCP) ---

func BenchmarkPubSubInProcess(b *testing.B) {
	bk := broker.New()
	payload := make([]byte, 128)

	sub := bk.Subscribe("flow", "bench-group")
	defer bk.Unsubscribe(sub)

	ctx := context.Background()
	b.SetBytes(128)
	b.ReportAllocs()
	b.ResetTimer()

	done := make(chan struct{})
	go func() {
		defer close(done)
		total := 0
		for total < b.N {
			msgs, err := sub.Next(ctx, 256)
			if err != nil {
				return
			}
			total += len(msgs)
		}
	}()

	for range b.N {
		bk.Publish("flow", payload)
	}
	<-done
}

// --- End-to-end TCP benchmark ---

func BenchmarkTCPPublish(b *testing.B) {
	srv, addr := startTestServer(b)
	defer srv.Shutdown()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	payload := make([]byte, 256)
	b.SetBytes(256)
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		if err := protocol.Encode(conn, &protocol.Frame{
			Op:      protocol.OpPublish,
			Topic:   "tcp-bench",
			Payload: payload,
		}); err != nil {
			b.Fatal(err)
		}
		resp, err := protocol.Decode(conn)
		if err != nil {
			b.Fatal(err)
		}
		if resp.Op != protocol.OpAck {
			b.Fatalf("expected ACK, got op=%02x", resp.Op)
		}
	}
}

func BenchmarkTCPPublishParallel(b *testing.B) {
	srv, addr := startTestServer(b)
	defer srv.Shutdown()

	b.SetBytes(256)
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			b.Fatal(err)
		}
		defer conn.Close()

		payload := make([]byte, 256)
		for pb.Next() {
			if err := protocol.Encode(conn, &protocol.Frame{
				Op:      protocol.OpPublish,
				Topic:   "tcp-par",
				Payload: payload,
			}); err != nil {
				return
			}
			if _, err := protocol.Decode(conn); err != nil {
				return
			}
		}
	})
}

func BenchmarkTCPBatchPublish64(b *testing.B) {
	srv, addr := startTestServer(b)
	defer srv.Shutdown()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	const batchSize = 64
	payload := make([]byte, 256)
	batch := make([][]byte, batchSize)
	for i := range batchSize {
		batch[i] = payload
	}

	b.SetBytes(int64(batchSize * len(payload)))
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		if err := protocol.Encode(conn, &protocol.Frame{
			Op:      protocol.OpBatchPublish,
			Topic:   "tcp-batch",
			Payload: protocol.EncodeBatchPayload(batch),
		}); err != nil {
			b.Fatal(err)
		}
		resp, err := protocol.Decode(conn)
		if err != nil {
			b.Fatal(err)
		}
		if resp.Op == protocol.OpError {
			b.Fatalf("broker error: %s", string(resp.Payload))
		}
		if _, _, _, err := protocol.DecodeBatchAck(resp.Payload); err != nil {
			b.Fatal(err)
		}
	}
}

// --- WAL append raw ---

func BenchmarkWALAppend(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.wal")
	w, err := wal.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	payload := make([]byte, 256)
	b.SetBytes(256)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = w.Append("wal-bench", payload)
	}
}

// --- WAL replay ---

func BenchmarkWALReplay(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "replay.wal")
	w, err := wal.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	payload := make([]byte, 128)
	for range 100_000 {
		_ = w.Append("replay-topic", payload)
	}
	w.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = wal.Replay(path, func(r wal.Record) error {
			return nil
		})
	}
}

func startTestServer(tb testing.TB) (*broker.TCPServer, string) {
	tb.Helper()

	dir := tb.TempDir()
	walPath := filepath.Join(dir, "bench.wal")
	w, err := wal.Open(walPath)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { w.Close() })

	// grab a free port, then release it so the server can bind
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	bk := broker.New()
	srv := broker.NewTCPServer(addr, bk, w, consumer.NewManager(), nil)
	go func() {
		_ = srv.ListenAndServe()
	}()
	time.Sleep(20 * time.Millisecond)
	return srv, addr
}

// --- Throughput report: publish N messages, measure wall clock ---

func TestThroughputReport(t *testing.T) {
	if os.Getenv("GOQUEUE_BENCH") == "" {
		t.Skip("set GOQUEUE_BENCH=1 to run throughput report")
	}

	bk := broker.New()
	counts := []int{100_000, 500_000, 1_000_000}
	payload := make([]byte, 256)

	for _, n := range counts {
		start := time.Now()
		for range n {
			bk.Publish("throughput", payload)
		}
		elapsed := time.Since(start)
		rate := float64(n) / elapsed.Seconds()
		t.Logf("%d msgs in %v → %.0f msgs/sec (256B payload)", n, elapsed, rate)
	}
}

// --- Concurrent throughput report ---

func TestConcurrentThroughputReport(t *testing.T) {
	if os.Getenv("GOQUEUE_BENCH") == "" {
		t.Skip("set GOQUEUE_BENCH=1 to run throughput report")
	}

	bk := broker.New()
	payload := make([]byte, 256)
	workers := []int{1, 4, 8, 16}
	total := 1_000_000

	for _, w := range workers {
		perWorker := total / w
		var wg sync.WaitGroup
		start := time.Now()
		for range w {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range perWorker {
					bk.Publish("concurrent-tp", payload)
				}
			}()
		}
		wg.Wait()
		elapsed := time.Since(start)
		rate := float64(total) / elapsed.Seconds()
		t.Logf("%d workers × %d msgs = %d total in %v → %.0f msgs/sec",
			w, perWorker, total, elapsed, rate)
	}
}

// --- E2E TCP throughput report ---

func TestTCPThroughputReport(t *testing.T) {
	if os.Getenv("GOQUEUE_BENCH") == "" {
		t.Skip("set GOQUEUE_BENCH=1 to run throughput report")
	}

	srv, addr := startTestServer(t)
	defer srv.Shutdown()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	payload := make([]byte, 256)
	n := 100_000
	start := time.Now()
	for range n {
		_ = protocol.Encode(conn, &protocol.Frame{
			Op:      protocol.OpPublish,
			Topic:   "tcp-tp",
			Payload: payload,
		})
		resp, _ := protocol.Decode(conn)
		if resp.Op == protocol.OpError {
			t.Fatalf("broker error: %s", string(resp.Payload))
		}
	}
	elapsed := time.Since(start)
	rate := float64(n) / elapsed.Seconds()

	lastOffset := int64(0)
	// read last ack offset
	_ = protocol.Encode(conn, &protocol.Frame{
		Op:      protocol.OpPublish,
		Topic:   "tcp-tp",
		Payload: []byte("final"),
	})
	resp, _ := protocol.Decode(conn)
	if len(resp.Payload) == 8 {
		lastOffset = int64(binary.BigEndian.Uint64(resp.Payload))
	}

	t.Logf("TCP E2E: %d publishes in %v → %.0f msgs/sec (last offset: %d)", n, elapsed, rate, lastOffset)
}

// placeholder to avoid "unused import" if fmt gets stripped
var _ = fmt.Sprintf
