package shardwal

import (
	"testing"
)

func TestManager_SameShardSameHandle(t *testing.T) {
	m, err := NewManager(t.TempDir(), "self")
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer m.Close()

	a, err := m.Shard(7)
	if err != nil {
		t.Fatalf("shard 7: %v", err)
	}
	b, err := m.Shard(7)
	if err != nil {
		t.Fatalf("shard 7 again: %v", err)
	}
	if a != b {
		t.Fatal("expected same handle for shard 7")
	}
}

func TestManager_DifferentShardsDifferentFiles(t *testing.T) {
	m, err := NewManager(t.TempDir(), "self")
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer m.Close()

	s7, _ := m.Shard(7)
	s8, _ := m.Shard(8)
	if _, err := s7.Append([]byte("seven")); err != nil {
		t.Fatalf("append 7: %v", err)
	}
	if _, err := s8.Append([]byte("eight")); err != nil {
		t.Fatalf("append 8: %v", err)
	}
	if s7.Tail() != 1 || s8.Tail() != 1 {
		t.Fatalf("tails: 7=%d 8=%d, want 1/1", s7.Tail(), s8.Tail())
	}
}

func TestManager_HWMIsPerShard(t *testing.T) {
	m, err := NewManager(t.TempDir(), "self")
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer m.Close()

	h7 := m.HWM(7)
	h8 := m.HWM(8)
	if h7 == h8 {
		t.Fatal("HWM(7) and HWM(8) should be different instances")
	}
	if got := m.HWM(7); got != h7 {
		t.Fatal("HWM(7) should be stable across calls")
	}
}
