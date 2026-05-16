package consumer

import (
	"encoding/json"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// defaultFlushInterval is the debounce window for the async offset flusher.
// Many concurrent commits within this window collapse into a single disk write.
const defaultFlushInterval = 50 * time.Millisecond

// Manager stores committed offsets for consumer groups.
//
// When a path is set, commits are kept in memory and persisted by a background
// flusher: each commit signals the flusher and the flusher writes a snapshot
// using an atomic temp-file + rename. This keeps Commit off the disk hot path
// while preserving crash-consistent on-disk state (the file is either fully
// written or untouched after rename).
type Manager struct {
	mu      sync.RWMutex
	offsets map[string]int64
	path    string // empty = in-memory only

	// Background flusher state. Only used when path != "".
	flushSignal chan struct{} // buffered, cap=1
	flushDone   chan struct{} // closed when the flusher exits
	closeOnce   sync.Once
	closeCh     chan struct{} // closed by Close to stop the flusher
	flushEvery  time.Duration

	// Observability — incremented when a flush fails. Callers can read via
	// SaveErrors() to wire a Prometheus counter or alert.
	saveErrors atomic.Int64
}

// NewManager creates an in-memory-only manager (no persistence).
func NewManager() *Manager {
	return &Manager{offsets: make(map[string]int64)}
}

// NewManagerWithPath creates a manager that persists offsets to path.
// Existing offsets are loaded on startup; missing file is not an error.
// A background goroutine flushes pending commits to disk; call Close to stop it.
func NewManagerWithPath(path string) (*Manager, error) {
	m := &Manager{
		offsets:     make(map[string]int64),
		path:        path,
		flushSignal: make(chan struct{}, 1),
		flushDone:   make(chan struct{}),
		closeCh:     make(chan struct{}),
		flushEvery:  defaultFlushInterval,
	}
	if err := m.load(); err != nil {
		return nil, err
	}
	go m.flushLoop()
	return m, nil
}

// Close stops the background flusher and writes any final pending state to
// disk. Safe to call multiple times; safe to call on an in-memory manager.
func (m *Manager) Close() error {
	if m.path == "" {
		return nil
	}
	m.closeOnce.Do(func() {
		close(m.closeCh)
	})
	<-m.flushDone
	return m.save()
}

func (m *Manager) flushLoop() {
	defer close(m.flushDone)
	t := time.NewTimer(m.flushEvery)
	defer t.Stop()
	dirty := false
	for {
		select {
		case <-m.closeCh:
			return
		case <-m.flushSignal:
			dirty = true
			if !t.Stop() {
				select {
				case <-t.C:
				default:
				}
			}
			t.Reset(m.flushEvery)
		case <-t.C:
			if dirty {
				if err := m.save(); err != nil {
					// Keep dirty=true so the next tick retries. Losing the
					// flag here would silently leak committed offsets on
					// crash. Surface the error via log + counter so an
					// operator can react.
					m.saveErrors.Add(1)
					log.Printf("consumer: offset flush failed (will retry): %v", err)
				} else {
					dirty = false
				}
			}
			t.Reset(m.flushEvery)
		}
	}
}

func (m *Manager) signalDirty() {
	if m.flushSignal == nil {
		return
	}
	select {
	case m.flushSignal <- struct{}{}:
	default:
		// Another commit already signaled an upcoming flush; nothing to do.
	}
}

func (m *Manager) load() error {
	data, err := os.ReadFile(m.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(data, &m.offsets)
}

// save writes a snapshot atomically: write to .tmp then rename.
func (m *Manager) save() error {
	// Snapshot under read lock — don't hold lock during IO.
	m.mu.RLock()
	snap := make(map[string]int64, len(m.offsets))
	for k, v := range m.offsets {
		snap[k] = v
	}
	m.mu.RUnlock()

	data, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	tmp := m.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, m.path)
}

func key(topic, group string, partition int) string {
	return topic + "::" + group + "::p" + itoa(partition)
}

func (m *Manager) Get(topic, group string) (int64, bool) {
	return m.GetPartition(topic, group, 0)
}

func (m *Manager) GetPartition(topic, group string, partition int) (int64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.offsets[key(topic, group, partition)]
	return v, ok
}

// SaveErrors returns the cumulative count of background flush failures.
// Wire this to a metric/alert in production — a non-zero value means
// committed offsets are not durably persisted and a crash will lose them.
func (m *Manager) SaveErrors() int64 { return m.saveErrors.Load() }

func (m *Manager) Commit(topic, group string, offset int64) {
	m.CommitPartition(topic, group, 0, offset)
}

func (m *Manager) CommitPartition(topic, group string, partition int, offset int64) {
	m.mu.Lock()
	m.offsets[key(topic, group, partition)] = offset
	m.mu.Unlock()
	m.signalDirty()
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	sign := ""
	if v < 0 {
		sign = "-"
		v = -v
	}
	buf := [20]byte{}
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return sign + string(buf[i:])
}
