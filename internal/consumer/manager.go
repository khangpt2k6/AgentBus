package consumer

import (
	"encoding/json"
	"os"
	"sync"
)

// Manager stores committed offsets for consumer groups.
// When a path is set, offsets are persisted to disk on every commit using an
// atomic temp-file + rename so that a crash never leaves a partial file.
type Manager struct {
	mu      sync.RWMutex
	offsets map[string]int64
	path    string // empty = in-memory only
}

// NewManager creates an in-memory-only manager (no persistence).
func NewManager() *Manager {
	return &Manager{offsets: make(map[string]int64)}
}

// NewManagerWithPath creates a manager that persists offsets to path.
// Existing offsets are loaded on startup; missing file is not an error.
func NewManagerWithPath(path string) (*Manager, error) {
	m := &Manager{offsets: make(map[string]int64), path: path}
	if err := m.load(); err != nil {
		return nil, err
	}
	return m, nil
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

func (m *Manager) Commit(topic, group string, offset int64) {
	m.CommitPartition(topic, group, 0, offset)
}

func (m *Manager) CommitPartition(topic, group string, partition int, offset int64) {
	m.mu.Lock()
	m.offsets[key(topic, group, partition)] = offset
	m.mu.Unlock()
	if m.path != "" {
		_ = m.save() // best-effort; crash-safe via atomic rename
	}
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
