package ratelimit

import (
	"sync"
	"testing"
	"time"
)

func TestDisabledLimiterAllowsEverything(t *testing.T) {
	l := New(0, 0)
	if l.Enabled() {
		t.Fatal("rate <= 0 must be disabled")
	}
	for i := 0; i < 10000; i++ {
		if !l.Allow("tenant-a") {
			t.Fatalf("disabled limiter denied request %d", i)
		}
	}
}

func TestNilLimiterIsDisabled(t *testing.T) {
	var l *Limiter
	if l.Enabled() {
		t.Fatal("nil limiter must report disabled")
	}
	if !l.Allow("anything") {
		t.Fatal("nil limiter must allow")
	}
}

func TestBurstThenThrottleSameInstant(t *testing.T) {
	now := time.Unix(0, 0)
	l := newWithClock(10, 5, func() time.Time { return now })
	for i := 0; i < 5; i++ {
		if !l.Allow("t1") {
			t.Fatalf("burst token %d should be allowed", i)
		}
	}
	if l.Allow("t1") {
		t.Fatal("request beyond burst at the same instant must be throttled")
	}
}

func TestRefillOverTime(t *testing.T) {
	now := time.Unix(0, 0)
	l := newWithClock(10, 5, func() time.Time { return now }) // 10 tokens/sec
	for i := 0; i < 5; i++ {
		l.Allow("t1")
	}
	if l.Allow("t1") {
		t.Fatal("bucket should be empty")
	}
	now = now.Add(100 * time.Millisecond) // 10/s * 0.1s = 1 token
	if !l.Allow("t1") {
		t.Fatal("one token should have refilled after 100ms")
	}
	if l.Allow("t1") {
		t.Fatal("only one token should have refilled")
	}
}

func TestRefillCapsAtBurst(t *testing.T) {
	now := time.Unix(0, 0)
	l := newWithClock(10, 5, func() time.Time { return now })
	now = now.Add(10 * time.Second) // would add 100 tokens, must cap at burst 5
	for i := 0; i < 5; i++ {
		if !l.Allow("t1") {
			t.Fatalf("token %d within burst should be allowed", i)
		}
	}
	if l.Allow("t1") {
		t.Fatal("accumulated tokens must be capped at burst")
	}
}

// The core fault-isolation property: a noisy key cannot consume another key's
// allowance. One flooding tenant is throttled while a quiet tenant is not.
func TestPerKeyIsolation(t *testing.T) {
	now := time.Unix(0, 0)
	l := newWithClock(10, 3, func() time.Time { return now })
	for i := 0; i < 3; i++ {
		l.Allow("noisy")
	}
	if l.Allow("noisy") {
		t.Fatal("noisy tenant should be throttled after draining its bucket")
	}
	for i := 0; i < 3; i++ {
		if !l.Allow("quiet") {
			t.Fatalf("quiet tenant request %d must not be affected by the noisy tenant", i)
		}
	}
}

func TestBurstFloorIsOne(t *testing.T) {
	now := time.Unix(0, 0)
	l := newWithClock(5, 0, func() time.Time { return now }) // burst < 1 is raised to 1
	if !l.Allow("t1") {
		t.Fatal("first request should pass with an effective burst of 1")
	}
	if l.Allow("t1") {
		t.Fatal("second immediate request should be throttled")
	}
}

func TestIdleBucketsAreEvicted(t *testing.T) {
	now := time.Unix(0, 0)
	l := newWithClock(10, 5, func() time.Time { return now })
	l.ttl = time.Second
	l.Allow("a")
	if l.size() != 1 {
		t.Fatalf("expected 1 tracked bucket, got %d", l.size())
	}
	now = now.Add(2 * time.Second)
	l.sweepIdle()
	if l.size() != 0 {
		t.Fatalf("idle bucket should have been evicted, size=%d", l.size())
	}
}

func TestActiveBucketSurvivesSweep(t *testing.T) {
	now := time.Unix(0, 0)
	l := newWithClock(10, 5, func() time.Time { return now })
	l.ttl = time.Second
	l.Allow("busy")
	now = now.Add(500 * time.Millisecond)
	l.Allow("busy") // refresh lastSeen
	l.sweepIdle()
	if l.size() != 1 {
		t.Fatalf("recently active bucket must not be evicted, size=%d", l.size())
	}
}

func TestConcurrentAllowIsRaceFree(t *testing.T) {
	l := New(1000, 1000)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				l.Allow("shared")
			}
		}(g)
	}
	wg.Wait()
}
