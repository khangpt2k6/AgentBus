package consumer

import (
	"sync"
	"testing"
)

func TestCommitAndGetPartition(t *testing.T) {
	m := NewManager()
	m.CommitPartition("orders", "payments", 2, 42)

	got, ok := m.GetPartition("orders", "payments", 2)
	if !ok {
		t.Fatalf("expected committed offset to exist")
	}
	if got != 42 {
		t.Fatalf("offset = %d, want 42", got)
	}
}

func TestGetMissingPartition(t *testing.T) {
	m := NewManager()
	if _, ok := m.GetPartition("orders", "payments", 0); ok {
		t.Fatalf("expected missing offset")
	}
}

func TestConcurrentCommitLastWriteWins(t *testing.T) {
	m := NewManager()
	const writers = 64

	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(v int64) {
			defer wg.Done()
			m.CommitPartition("events", "analytics", 1, v)
		}(int64(i))
	}
	wg.Wait()

	got, ok := m.GetPartition("events", "analytics", 1)
	if !ok {
		t.Fatalf("expected committed offset")
	}
	if got < 0 || got >= writers {
		t.Fatalf("offset out of range: %d", got)
	}
}
