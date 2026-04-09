package grpcapi

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/2006t/goqueue/internal/broker"
	"github.com/2006t/goqueue/internal/consumer"
	"github.com/2006t/goqueue/internal/metrics"
	"github.com/2006t/goqueue/internal/wal"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	goqueuev1 "github.com/2006t/goqueue/proto"
	"google.golang.org/grpc/metadata"
)

func TestPublishWritesPartitionMetadataToWAL(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "grpc.wal")
	logFile, err := wal.Open(walPath)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	t.Cleanup(func() { _ = logFile.Close() })

	srv := NewServer(broker.New(), consumer.NewManager(), nil, logFile)
	resp, err := srv.Publish(context.Background(), &goqueuev1.PublishRequest{
		Topic:   "orders",
		Key:     "user-42",
		Payload: []byte("hello"),
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	if err := logFile.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	var got []wal.Record
	if err := wal.Replay(walPath, func(r wal.Record) error {
		got = append(got, r)
		return nil
	}); err != nil {
		t.Fatalf("replay wal: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("records = %d, want 1", len(got))
	}
	if got[0].Partition != resp.Partition {
		t.Fatalf("partition in wal = %d, want %d", got[0].Partition, resp.Partition)
	}
	if got[0].Key != "user-42" {
		t.Fatalf("key in wal = %q, want user-42", got[0].Key)
	}
}

func TestConsumeStreamsAndCommitsOffsets(t *testing.T) {
	bk := broker.New()
	groups := consumer.NewManager()
	srv := NewServer(bk, groups, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	stream := newTestConsumeStream(ctx, cancel)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Consume(&goqueuev1.ConsumeRequest{
			Topic:     "orders",
			Group:     "billing",
			Partition: 0,
		}, stream)
	}()

	time.Sleep(100 * time.Millisecond)
	offset, err := bk.PublishToPartition("orders", 0, []byte("first"))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	msg, ok := stream.waitForMessage(2 * time.Second)
	if !ok {
		t.Fatalf("timed out waiting for streamed message")
	}
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("consume returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("consume did not stop after cancel")
	}

	if string(msg.Payload) != "first" {
		t.Fatalf("payload = %q, want first", string(msg.Payload))
	}
	got, ok := groups.GetPartition("orders", "billing", 0)
	if !ok {
		t.Fatalf("expected committed group offset")
	}
	want := offset + 1
	if got != want {
		t.Fatalf("offset = %d, want %d", got, want)
	}
}

func TestPublishAgentEventUpdatesMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	srv := NewServer(broker.New(), consumer.NewManager(), m, nil)

	payload := `{"version":"v1","type":"tool.call","tenant":"acme","project":"support","session_id":"sess-1","agent_id":"planner","attempt":2,"created_at":"2026-04-03T10:00:00Z","payload":{"tool":"search"}}`
	if _, err := srv.Publish(context.Background(), &goqueuev1.PublishRequest{
		Topic:   "agent-events.dlq",
		Payload: []byte(payload),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	assertCounter := func(name string) {
		value, ok := counterValue(families, name, map[string]string{
			"topic":      "agent-events.dlq",
			"event_type": "tool.call",
		})
		if !ok {
			t.Fatalf("counter %s with labels not found", name)
		}
		if value != 1 {
			t.Fatalf("counter %s value=%v want 1", name, value)
		}
	}
	assertCounter("goqueue_agent_events_published_total")
	assertCounter("goqueue_agent_event_retries_total")
	assertCounter("goqueue_agent_event_dlq_total")
}

func counterValue(families []*dto.MetricFamily, name string, labels map[string]string) (float64, bool) {
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		for _, metric := range fam.GetMetric() {
			if matchLabels(metric.GetLabel(), labels) && metric.GetCounter() != nil {
				return metric.GetCounter().GetValue(), true
			}
		}
		return 0, false
	}
	return 0, false
}

func matchLabels(pairs []*dto.LabelPair, want map[string]string) bool {
	if len(pairs) != len(want) {
		// We only accept exact label sets to avoid false positives.
		return false
	}
	for _, p := range pairs {
		if want[p.GetName()] != p.GetValue() {
			return false
		}
	}
	return true
}

type testConsumeStream struct {
	ctx    context.Context
	cancel context.CancelFunc
	ch     chan *goqueuev1.ConsumeMessage
	mu     sync.Mutex
}

func newTestConsumeStream(ctx context.Context, cancel context.CancelFunc) *testConsumeStream {
	return &testConsumeStream{
		ctx:    ctx,
		cancel: cancel,
		ch:     make(chan *goqueuev1.ConsumeMessage, 8),
	}
}

func (s *testConsumeStream) waitForMessage(timeout time.Duration) (*goqueuev1.ConsumeMessage, bool) {
	select {
	case msg := <-s.ch:
		return msg, true
	case <-time.After(timeout):
		return nil, false
	}
}

func (s *testConsumeStream) Context() context.Context { return s.ctx }
func (s *testConsumeStream) SetHeader(metadata.MD) error {
	return nil
}
func (s *testConsumeStream) SendHeader(metadata.MD) error {
	return nil
}
func (s *testConsumeStream) SetTrailer(metadata.MD) {}
func (s *testConsumeStream) SendMsg(any) error      { return nil }
func (s *testConsumeStream) RecvMsg(any) error      { return nil }

func (s *testConsumeStream) Send(msg *goqueuev1.ConsumeMessage) error {
	s.ch <- msg
	return nil
}
