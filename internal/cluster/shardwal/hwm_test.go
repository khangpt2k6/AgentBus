package shardwal

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestHWM_StartsAtZero(t *testing.T) {
	h := NewHWM("self")
	if got := h.Mark(); got != 0 {
		t.Fatalf("initial Mark = %d, want 0", got)
	}
}

func TestHWM_UpdateRaisesMark(t *testing.T) {
	h := NewHWM("self")
	h.SetReplicas([]string{"self", "n2", "n3"})
	h.Update("self", 5)
	h.Update("n2", 3)
	h.Update("n3", 4)
	if got := h.Mark(); got != 3 {
		t.Fatalf("Mark = %d, want 3 (min)", got)
	}
}

func TestHWM_DropReplicaRecomputesMark(t *testing.T) {
	h := NewHWM("self")
	h.SetReplicas([]string{"self", "slow"})
	h.Update("self", 10)
	h.Update("slow", 2)
	if got := h.Mark(); got != 2 {
		t.Fatalf("Mark with slow replica = %d, want 2", got)
	}
	// Slow replica drops out (lagging too far).
	h.SetReplicas([]string{"self"})
	if got := h.Mark(); got != 10 {
		t.Fatalf("Mark after drop = %d, want 10", got)
	}
}

func TestHWM_WaitForUnblocksOnUpdate(t *testing.T) {
	h := NewHWM("self")
	h.SetReplicas([]string{"self", "n2"})
	h.Update("self", 10)
	h.Update("n2", 0)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- h.WaitFor(ctx, 5)
	}()

	// Mark is currently 0; should be blocked.
	select {
	case <-done:
		t.Fatal("WaitFor returned before HWM caught up")
	case <-time.After(50 * time.Millisecond):
	}

	h.Update("n2", 5)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WaitFor returned err: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("WaitFor did not unblock within 1s after Update")
	}
}

func TestHWM_WaitForRespectsContext(t *testing.T) {
	h := NewHWM("self")
	h.SetReplicas([]string{"self", "n2"})
	h.Update("self", 0)
	h.Update("n2", 0)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := h.WaitFor(ctx, 5)
	if err == nil {
		t.Fatal("WaitFor returned nil; want context error")
	}
	if err != context.DeadlineExceeded {
		t.Fatalf("WaitFor err = %v, want DeadlineExceeded", err)
	}
}

func TestHWM_ConcurrentSafe(t *testing.T) {
	h := NewHWM("self")
	h.SetReplicas([]string{"self", "n2"})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := uint64(0); j < 1000; j++ {
				h.Update("self", j)
				h.Update("n2", j)
				_ = h.Mark()
			}
		}()
	}
	wg.Wait()
}
