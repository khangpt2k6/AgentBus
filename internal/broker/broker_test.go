package broker

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestBrokerPublishSubscribe(t *testing.T) {
	b := New()

	// subscriber first, then publish — it should block and receive
	sub := b.Subscribe("orders", "payment-svc")
	defer b.Unsubscribe(sub)

	go func() {
		time.Sleep(10 * time.Millisecond)
		b.Publish("orders", []byte("order-1"))
		b.Publish("orders", []byte("order-2"))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	msgs, err := sub.Next(ctx, 10)
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("got %d msgs, want 2", len(msgs))
	}
	if string(msgs[0].Payload) != "order-1" {
		t.Errorf("msg[0]: got %q, want %q", string(msgs[0].Payload), "order-1")
	}
}

func TestBrokerMultipleTopics(t *testing.T) {
	b := New()

	b.Publish("users", []byte("alice"))
	b.Publish("orders", []byte("pizza"))

	topics := b.Topics()
	if len(topics) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(topics))
	}
	if b.TotalPublished() != 2 {
		t.Errorf("total published: got %d, want 2", b.TotalPublished())
	}
}

func TestSubscribeAtOffset(t *testing.T) {
	b := New()
	for i := range 10 {
		b.Publish("events", []byte(fmt.Sprintf("e%d", i)))
	}

	sub := b.SubscribeAt("events", "worker", 5)
	defer b.Unsubscribe(sub)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	msgs, err := sub.Next(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 5 {
		t.Fatalf("got %d msgs, want 5 (offsets 5-9)", len(msgs))
	}
	if string(msgs[0].Payload) != "e5" {
		t.Errorf("first msg: got %q, want %q", string(msgs[0].Payload), "e5")
	}
}

func TestSubscriptionContextCancel(t *testing.T) {
	b := New()
	sub := b.Subscribe("empty", "group")
	defer b.Unsubscribe(sub)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := sub.Next(ctx, 10)
	if err != context.DeadlineExceeded {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

func TestTopicPartitionDetail(t *testing.T) {
	b := New()

	// Missing topic must return an error.
	if _, err := b.TopicPartitionDetail("missing", 0); err == nil {
		t.Fatal("expected error for unknown topic")
	}

	// Publish a few messages, then inspect partition 0.
	for range 10 {
		b.Publish("orders", []byte("payload"))
	}

	d, err := b.TopicPartitionDetail("orders", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.Capacity != defaultCapacity {
		t.Errorf("capacity: got %d, want %d", d.Capacity, defaultCapacity)
	}
	if d.Tail == 0 {
		t.Error("tail should be > 0 after publishing")
	}
	if d.Evictions != 0 {
		t.Errorf("evictions: got %d, want 0 (ring not full)", d.Evictions)
	}

	// Invalid partition must return an error.
	if _, err := b.TopicPartitionDetail("orders", 999); err == nil {
		t.Fatal("expected error for invalid partition")
	}
}
