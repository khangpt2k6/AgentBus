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

	// Second instance must load what the first wrote.
	m2, err := NewManagerWithPath(path)
	if err != nil {
		t.Fatalf("second NewManagerWithPath: %v", err)
	}
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
	if _, ok := m.GetPartition("any", "group", 0); ok {
		t.Fatal("expected empty manager for missing file")
	}
}
