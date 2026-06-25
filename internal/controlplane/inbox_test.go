package controlplane

import (
	"sync"
	"testing"
	"time"
)

func TestInboxDeliverUnopened(t *testing.T) {
	in := NewInboxes(4)
	if in.Deliver("nobody", RoutedEvent{Type: "handoff"}) {
		t.Error("Deliver to an unopened inbox should return false")
	}
}

func TestInboxOpenThenDeliver(t *testing.T) {
	in := NewInboxes(4)
	ch := in.Open("escalator")
	if !in.Deliver("escalator", RoutedEvent{Type: "handoff", SessionKey: "acme/bot/s1"}) {
		t.Fatal("Deliver to an open inbox should return true")
	}
	select {
	case got := <-ch:
		if got.SessionKey != "acme/bot/s1" {
			t.Errorf("got %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("expected to receive the routed event")
	}
}

func TestInboxOpenIdempotent(t *testing.T) {
	in := NewInboxes(4)
	ch1 := in.Open("a")
	ch2 := in.Open("a")
	if ch1 != ch2 {
		t.Error("Open should return the same channel for the same agent")
	}
}

func TestInboxFullNonBlocking(t *testing.T) {
	in := NewInboxes(2)
	in.Open("a")
	if !in.Deliver("a", RoutedEvent{}) {
		t.Fatal("first deliver should succeed")
	}
	if !in.Deliver("a", RoutedEvent{}) {
		t.Fatal("second deliver should succeed")
	}

	done := make(chan bool, 1)
	go func() { done <- in.Deliver("a", RoutedEvent{}) }()
	select {
	case ok := <-done:
		if ok {
			t.Error("deliver to a full inbox should return false")
		}
	case <-time.After(time.Second):
		t.Fatal("Deliver blocked on a full inbox; it must be non-blocking")
	}
}

func TestInboxConcurrent(t *testing.T) {
	in := NewInboxes(16)
	in.Open("a")
	var wg sync.WaitGroup
	// drainer
	stop := make(chan struct{})
	go func() {
		ch := in.Open("a")
		for {
			select {
			case <-ch:
			case <-stop:
				return
			}
		}
	}()
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			in.Deliver("a", RoutedEvent{})
			in.Open("b")
			in.Deliver("b", RoutedEvent{})
		}()
	}
	wg.Wait()
	close(stop)
}
