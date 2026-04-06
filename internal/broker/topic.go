package broker

import (
	"sync"
	"sync/atomic"
	"time"
)

const defaultCapacity = 1 << 20 // 1M messages per topic

// Message is a single record stored in a topic.
type Message struct {
	Offset    int64
	Timestamp time.Time
	Payload   []byte
}

// Topic is a named, ordered log of messages backed by a ring buffer.
// Multiple goroutines may publish and subscribe concurrently.
//
// head and tail are stored as atomic.Int64 so Head()/Tail() are lock-free.
// The RWMutex still guards the messages slice for consistent snapshot reads
// in Fetch(); head/tail are updated under the write lock and also atomically
// so that callers of Head()/Tail() don't need the lock.
type Topic struct {
	name string

	mu       sync.RWMutex
	messages []Message
	head     atomic.Int64 // oldest offset still in ring
	tail     atomic.Int64 // next offset to write (= total published)
	cap      int

	// subscribers waiting for new messages
	subsMu sync.Mutex
	subs   []*Subscription

	published atomic.Int64
}

func newTopic(name string, capacity int) *Topic {
	if capacity <= 0 {
		capacity = defaultCapacity
	}
	return &Topic{
		name:     name,
		messages: make([]Message, capacity),
		cap:      capacity,
	}
}

// Publish appends a message and notifies all subscribers.
func (t *Topic) Publish(payload []byte) int64 {
	t.mu.Lock()
	offset := t.tail.Load()
	idx := int(offset) % t.cap
	t.messages[idx] = Message{
		Offset:    offset,
		Timestamp: time.Now(),
		Payload:   append([]byte(nil), payload...),
	}
	newTail := t.tail.Add(1)
	if newTail-t.head.Load() > int64(t.cap) {
		t.head.Add(1) // evict oldest when full
	}
	t.mu.Unlock()

	t.published.Add(1)
	t.notifySubs()
	return offset
}

// Fetch returns up to maxCount messages starting from offset.
func (t *Topic) Fetch(offset int64, maxCount int) []Message {
	t.mu.RLock()
	defer t.mu.RUnlock()

	head := t.head.Load()
	tail := t.tail.Load()

	if offset < head {
		offset = head
	}
	if offset >= tail {
		return nil
	}

	end := offset + int64(maxCount)
	if end > tail {
		end = tail
	}

	out := make([]Message, 0, end-offset)
	for i := offset; i < end; i++ {
		idx := int(i) % t.cap
		out = append(out, t.messages[idx])
	}
	return out
}

// Head returns the oldest available offset (lock-free).
func (t *Topic) Head() int64 { return t.head.Load() }

// Tail returns the next offset to be written (lock-free).
func (t *Topic) Tail() int64 { return t.tail.Load() }

func (t *Topic) Published() int64 { return t.published.Load() }

func (t *Topic) addSub(s *Subscription) {
	t.subsMu.Lock()
	t.subs = append(t.subs, s)
	t.subsMu.Unlock()
}

func (t *Topic) removeSub(s *Subscription) {
	t.subsMu.Lock()
	defer t.subsMu.Unlock()
	for i, sub := range t.subs {
		if sub == s {
			t.subs[i] = t.subs[len(t.subs)-1]
			t.subs = t.subs[:len(t.subs)-1]
			return
		}
	}
}

func (t *Topic) notifySubs() {
	t.subsMu.Lock()
	defer t.subsMu.Unlock()
	for _, s := range t.subs {
		select {
		case s.notify <- struct{}{}:
		default:
		}
	}
}
