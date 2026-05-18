package metadata

import (
	"encoding/json"
	"net"
	"path/filepath"
	"testing"
	"time"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func TestSingleNodeBootstrap_BecomesLeader(t *testing.T) {
	if testing.Short() {
		t.Skip("network-bound; skipped with -short")
	}
	dir := t.TempDir()
	addr := freeAddr(t)
	m, err := Start(Options{
		NodeID:        "n1",
		BindAddr:      addr,
		AdvertiseAddr: addr,
		DataDir:       dir,
		Bootstrap:     true,
		InitialPeers: []Peer{
			{NodeID: "n1", Addr: addr},
		},
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer m.Shutdown()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if m.IsLeader() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !m.IsLeader() {
		t.Fatalf("single-node Raft did not become leader within 5s")
	}

	// Submit one command and verify it lands in the FSM.
	cmd, _ := json.Marshal(Command{Op: OpAddMember, NodeID: "n1", Addr: addr})
	if err := m.Apply(cmd, 2*time.Second); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := m.FSM().Members()["n1"]; got != addr {
		t.Fatalf("FSM Members[n1] = %q, want %q", got, addr)
	}

	// Sanity: data dir exists with a Raft state file.
	if _, err := filepath.Glob(filepath.Join(dir, "raft.db")); err != nil {
		t.Fatalf("glob: %v", err)
	}
}
