// Package ratelimit provides a per-key token-bucket limiter used to isolate
// noisy producers. Each key (for example a tenant) gets its own independent
// bucket, so one agent that floods the broker is throttled on its own quota
// and cannot starve the allowance of any other agent. This is a bulkhead:
// the blast radius of a misbehaving producer stays contained to that producer.
package ratelimit

import (
	"sync"
	"time"
)

// gcEvery bounds memory by sweeping idle buckets once every this many Allow
// calls. Sweeping inline avoids a background goroutine and keeps the limiter
// dependency-free.
const gcEvery = 1024

// defaultTTL is how long a bucket may sit unused before it is evicted. A fresh
// bucket starts full, which matches what an idle bucket would have refilled to,
// so eviction never makes the limiter stricter than leaving the bucket in place.
const defaultTTL = 5 * time.Minute

// Limiter applies an independent token-bucket rate limit per key.
//
// A Limiter constructed with ratePerSec <= 0 is disabled: every request is
// allowed. The zero value and a nil *Limiter are also treated as disabled, so
// callers can hold a nil limiter when rate limiting is turned off.
type Limiter struct {
	rate  float64       // tokens added per second
	burst float64       // bucket capacity
	ttl   time.Duration // idle eviction threshold

	mu         sync.Mutex
	buckets    map[string]*bucket
	now        func() time.Time
	opsSinceGC int
}

type bucket struct {
	tokens   float64
	lastFill time.Time
	lastSeen time.Time
}

// New returns a Limiter that refills ratePerSec tokens per second per key, up
// to a maximum of burst tokens. A rate of 0 or less disables the limiter.
func New(ratePerSec, burst float64) *Limiter {
	return newWithClock(ratePerSec, burst, time.Now)
}

func newWithClock(ratePerSec, burst float64, now func() time.Time) *Limiter {
	if ratePerSec > 0 && burst < 1 {
		burst = 1
	}
	return &Limiter{
		rate:    ratePerSec,
		burst:   burst,
		ttl:     defaultTTL,
		buckets: make(map[string]*bucket),
		now:     now,
	}
}

// Enabled reports whether the limiter enforces a limit.
func (l *Limiter) Enabled() bool {
	return l != nil && l.rate > 0
}

// Allow consumes one token for key and reports whether the request may proceed.
// A disabled limiter always returns true.
func (l *Limiter) Allow(key string) bool {
	if !l.Enabled() {
		return true
	}
	now := l.now()

	l.mu.Lock()
	defer l.mu.Unlock()

	l.opsSinceGC++
	if l.opsSinceGC >= gcEvery {
		l.sweepIdleLocked(now)
		l.opsSinceGC = 0
	}

	b := l.buckets[key]
	if b == nil {
		b = &bucket{tokens: l.burst, lastFill: now}
		l.buckets[key] = b
	} else if elapsed := now.Sub(b.lastFill).Seconds(); elapsed > 0 {
		b.tokens += elapsed * l.rate
		if b.tokens > l.burst {
			b.tokens = l.burst
		}
		b.lastFill = now
	}
	b.lastSeen = now

	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

func (l *Limiter) sweepIdleLocked(now time.Time) {
	for k, b := range l.buckets {
		if now.Sub(b.lastSeen) > l.ttl {
			delete(l.buckets, k)
		}
	}
}

// sweepIdle evicts buckets idle longer than ttl. Exposed for tests.
func (l *Limiter) sweepIdle() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sweepIdleLocked(l.now())
}

// size returns the number of tracked buckets. Exposed for tests.
func (l *Limiter) size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.buckets)
}
