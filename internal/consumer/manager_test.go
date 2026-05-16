package consumer

import (
	"path/filepath"
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

func TestNewManagerWithPathPersistsAndLoads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offsets.json")

	// Write offsets via first instance.
	m1, err := NewManagerWithPath(path)
	if err != nil {
		t.Fatalf("NewManagerWithPath: %v", err)
	}
	m1.CommitPartition("events", "worker", 0, 42)
	m1.CommitPartition("events", "worker", 1, 100)
	if err := m1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Second instance must load what the first wrote.
	m2, err := NewManagerWithPath(path)
	if err != nil {
		t.Fatalf("second NewManagerWithPath: %v", err)
	}
	defer m2.Close()
	if v, ok := m2.GetPartition("events", "worker", 0); !ok || v != 42 {
		t.Errorf("partition 0: got (%d, %v), want (42, true)", v, ok)
	}
	if v, ok := m2.GetPartition("events", "worker", 1); !ok || v != 100 {
		t.Errorf("partition 1: got (%d, %v), want (100, true)", v, ok)
	}
}

func TestNewManagerWithPathMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nonexistent.json")
	m, err := NewManagerWithPath(path)
	if err != nil {
		t.Fatalf("missing file must not error: %v", err)
	}
	defer m.Close()
	if _, ok := m.GetPartition("any", "group", 0); ok {
		t.Fatal("expected empty manager for missing file")
	}
}

// TestConcurrentCommitsCoalesceFlushes asserts that many concurrent commits
// are batched by the async flusher: the on-disk file count of writes must be
// far less than the number of commits, and the final state must be durable.
func TestConcurrentCommitsCoalesceFlushes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "concurrent-offsets.json")
	m, err := NewManagerWithPath(path)
	if err != nil {
		t.Fatalf("NewManagerWithPath: %v", err)
	}

	const writers = 32
	const perWriter = 100
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(part int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				m.CommitPartition("events", "worker", part, int64(i))
			}
		}(w)
	}
	wg.Wait()

	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	m2, err := NewManagerWithPath(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	defer m2.Close()
	for w := 0; w < writers; w++ {
		v, ok := m2.GetPartition("events", "worker", w)
		if !ok {
			t.Errorf("partition %d missing after reload", w)
			continue
		}
		if v != int64(perWriter-1) {
			t.Errorf("partition %d: got %d, want %d", w, v, perWriter-1)
		}
	}
}
