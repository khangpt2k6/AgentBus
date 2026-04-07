package broker

import (
	"fmt"
	"sync"
	"testing"
)

func TestPublishAndFetch(t *testing.T) {
	topic := newTopic("test", 1024)

	for i := range 10 {
		topic.Publish([]byte(fmt.Sprintf("msg-%d", i)))
	}

	if topic.Tail() != 10 {
		t.Fatalf("tail: got %d, want 10", topic.Tail())
	}

	msgs := topic.Fetch(0, 100)
	if len(msgs) != 10 {
		t.Fatalf("fetch: got %d messages, want 10", len(msgs))
	}
	for i, m := range msgs {
		want := fmt.Sprintf("msg-%d", i)
		if string(m.Payload) != want {
			t.Errorf("msg[%d]: got %q, want %q", i, string(m.Payload), want)
		}
	}
}

func TestRingBufferWrapAround(t *testing.T) {
	cap := 64
	topic := newTopic("wrap", cap)

	for i := range cap + 20 {
		topic.Publish([]byte(fmt.Sprintf("m%d", i)))
	}

	// head should have advanced because we overflowed by 20
	if topic.Head() != 20 {
		t.Fatalf("head: got %d, want 20", topic.Head())
	}
	if topic.Tail() != int64(cap+20) {
		t.Fatalf("tail: got %d, want %d", topic.Tail(), cap+20)
	}

	// fetching from 0 should clamp to head
	msgs := topic.Fetch(0, cap)
	if msgs[0].Offset != 20 {
		t.Fatalf("first offset: got %d, want 20", msgs[0].Offset)
	}
}

func TestConcurrentPublish(t *testing.T) {
	topic := newTopic("concurrent", 1<<16)
	var wg sync.WaitGroup
	writers := 8
	perWriter := 1000

	for w := range writers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := range perWriter {
				topic.Publish([]byte(fmt.Sprintf("w%d-m%d", id, i)))
			}
		}(w)
	}
	wg.Wait()

	total := int64(writers * perWriter)
	if topic.Tail() != total {
		t.Fatalf("tail: got %d, want %d", topic.Tail(), total)
	}
}

func TestTopicCapacity(t *testing.T) {
	cap := 128
	topic := newTopic("cap-test", cap)
	if topic.Capacity() != cap {
		t.Fatalf("capacity: got %d, want %d", topic.Capacity(), cap)
	}
}

func TestTopicEvictions(t *testing.T) {
	cap := 32
	topic := newTopic("evict-test", cap)

	if topic.Evictions() != 0 {
		t.Fatalf("initial evictions: got %d, want 0", topic.Evictions())
	}

	// Fill exactly — no eviction yet.
	for range cap {
		topic.Publish([]byte("x"))
	}
	if topic.Evictions() != 0 {
		t.Fatalf("evictions at full capacity: got %d, want 0", topic.Evictions())
	}

	// Overflow by 7 — each extra message evicts one.
	overflow := 7
	for range overflow {
		topic.Publish([]byte("x"))
	}
	if topic.Evictions() != int64(overflow) {
		t.Fatalf("evictions after overflow: got %d, want %d", topic.Evictions(), overflow)
	}
}
