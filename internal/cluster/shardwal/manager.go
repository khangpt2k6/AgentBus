package shardwal

import (
	"sync"
)

// Manager owns the lifecycle of per-shard logs + HWMs. Lazily opens shards
// on first access; closes everything on Manager.Close.
type Manager struct {
	dir    string
	selfID string

	mu     sync.Mutex
	shards map[uint32]*Shard
	hwms   map[uint32]*HighWaterMark
}

// NewManager builds a Manager rooted at dir. selfID is the broker's node
// ID, used to seed HWM trackers as the always-present replica.
func NewManager(dir, selfID string) (*Manager, error) {
	return &Manager{
		dir:    dir,
		selfID: selfID,
		shards: make(map[uint32]*Shard),
		hwms:   make(map[uint32]*HighWaterMark),
	}, nil
}

// Shard returns the cached or newly-opened Shard handle for shardID.
func (m *Manager) Shard(shardID uint32) (*Shard, error) {
	m.mu.Lock()
	if s, ok := m.shards[shardID]; ok {
		m.mu.Unlock()
		return s, nil
	}
	m.mu.Unlock()

	s, err := Open(m.dir, shardID)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	// Re-check after acquiring the lock; another goroutine may have opened it.
	if existing, ok := m.shards[shardID]; ok {
		_ = s.Close()
		return existing, nil
	}
	m.shards[shardID] = s
	// Initialize the HWM's self offset to the existing tail so writes
	// resumed after restart don't roll the mark backwards.
	h := m.hwmLocked(shardID)
	h.Update(m.selfID, s.Tail())
	return s, nil
}

// HWM returns the (cached or newly-created) HighWaterMark for shardID.
func (m *Manager) HWM(shardID uint32) *HighWaterMark {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hwmLocked(shardID)
}

func (m *Manager) hwmLocked(shardID uint32) *HighWaterMark {
	if h, ok := m.hwms[shardID]; ok {
		return h
	}
	h := NewHWM(m.selfID)
	m.hwms[shardID] = h
	return h
}

// SelfID returns the broker's node ID (for diagnostics and to wire HWM elsewhere).
func (m *Manager) SelfID() string { return m.selfID }

// Close closes all open shards.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var firstErr error
	for _, s := range m.shards {
		if err := s.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	m.shards = nil
	m.hwms = nil
	return firstErr
}
