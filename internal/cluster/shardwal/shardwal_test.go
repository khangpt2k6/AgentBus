package shardwal

import (
	"context"
	"testing"
	"time"
)

func TestShard_AppendAndReplay(t *testing.T) {
	s, err := Open(t.TempDir(), 7)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	off1, err := s.Append([]byte("hello"))
	if err != nil {
		t.Fatalf("append 1: %v", err)
	}
	off2, err := s.Append([]byte("world"))
	if err != nil {
		t.Fatalf("append 2: %v", err)
	}
	if off1 != 0 || off2 != 1 {
		t.Fatalf("offsets = %d,%d want 0,1", off1, off2)
	}

	var got [][]byte
	if err := s.Replay(0, func(off uint64, payload []byte) error {
		got = append(got, append([]byte(nil), payload...))
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(got) != 2 || string(got[0]) != "hello" || string(got[1]) != "world" {
		t.Fatalf("replay got %v", got)
	}
}

func TestShard_ReplayFromOffset(t *testing.T) {
	s, err := Open(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
	for _, p := range []string{"a", "b", "c", "d"} {
		if _, err := s.Append([]byte(p)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	var got []string
	if err := s.Replay(2, func(off uint64, payload []byte) error {
		got = append(got, string(payload))
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(got) != 2 || got[0] != "c" || got[1] != "d" {
		t.Fatalf("replay from offset 2: got %v want [c d]", got)
	}
}

func TestShard_SubscribeReceivesLiveAppends(t *testing.T) {
	s, err := Open(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ch, cancelSub := s.Subscribe(ctx, 0)
	defer cancelSub()

	for _, p := range []string{"a", "b"} {
		if _, err := s.Append([]byte(p)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	got := []string{}
	for len(got) < 2 {
		select {
		case rec, ok := <-ch:
			if !ok {
				t.Fatalf("subscribe channel closed early; got %v", got)
			}
			got = append(got, string(rec.Payload))
		case <-ctx.Done():
			t.Fatalf("timeout waiting for subscribe; got %v", got)
		}
	}
	if got[0] != "a" || got[1] != "b" {
		t.Fatalf("subscribe got %v want [a b]", got)
	}
}

func TestShard_SubscribeBacklogThenLive(t *testing.T) {
	s, err := Open(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	// Pre-populate.
	for _, p := range []string{"old1", "old2"} {
		if _, err := s.Append([]byte(p)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ch, cancelSub := s.Subscribe(ctx, 0)
	defer cancelSub()

	// Live append after subscribe.
	if _, err := s.Append([]byte("new")); err != nil {
		t.Fatalf("append: %v", err)
	}

	got := []string{}
	for len(got) < 3 {
		select {
		case rec, ok := <-ch:
			if !ok {
				t.Fatalf("channel closed early; got %v", got)
			}
			got = append(got, string(rec.Payload))
		case <-ctx.Done():
			t.Fatalf("timeout; got %v", got)
		}
	}
	if got[0] != "old1" || got[1] != "old2" || got[2] != "new" {
		t.Fatalf("got %v want [old1 old2 new]", got)
	}
}

func TestShard_ReopenAfterClose(t *testing.T) {
	dir := t.TempDir()
	s1, err := Open(dir, 0)
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	for _, p := range []string{"a", "b", "c"} {
		if _, err := s1.Append([]byte(p)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	_ = s1.Close()

	s2, err := Open(dir, 0)
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer s2.Close()
	if got := s2.Tail(); got != 3 {
		t.Fatalf("after reopen, Tail() = %d, want 3", got)
	}
	off, err := s2.Append([]byte("d"))
	if err != nil {
		t.Fatalf("append after reopen: %v", err)
	}
	if off != 3 {
		t.Fatalf("offset after reopen = %d, want 3", off)
	}
}
