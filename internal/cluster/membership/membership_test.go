package membership

import (
	"fmt"
	"net"
	"testing"
	"time"
)

// freePort returns an OS-assigned free TCP/UDP port on 127.0.0.1.
// Memberlist uses both TCP (for full-state sync) and UDP (for ping/gossip)
// on the same port number, so we just need a free integer.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

func TestMembership_TwoNodesFormCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("network-bound; skipped with -short")
	}
	p1, p2 := freePort(t), freePort(t)
	addr1 := fmt.Sprintf("127.0.0.1:%d", p1)
	addr2 := fmt.Sprintf("127.0.0.1:%d", p2)

	m1, err := Start(Config{NodeID: "n1", GossipBind: addr1})
	if err != nil {
		t.Fatalf("start m1: %v", err)
	}
	defer m1.Shutdown()

	m2, err := Start(Config{NodeID: "n2", GossipBind: addr2, JoinAddrs: []string{addr1}})
	if err != nil {
		t.Fatalf("start m2: %v", err)
	}
	defer m2.Shutdown()

	// Wait up to 3s for both nodes to see each other.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(m1.Alive()) == 2 && len(m2.Alive()) == 2 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("nodes did not converge: m1.Alive()=%v m2.Alive()=%v", m1.Alive(), m2.Alive())
}
