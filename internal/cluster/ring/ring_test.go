package ring

import (
	"fmt"
	"testing"
)

func TestRing_LeaderForIsDeterministic(t *testing.T) {
	r := New(128)
	r.AddNode("n1")
	r.AddNode("n2")
	r.AddNode("n3")

	first := r.LeaderFor(7)
	for i := 0; i < 100; i++ {
		if got := r.LeaderFor(7); got != first {
			t.Fatalf("LeaderFor(7) inconsistent: %q vs %q", got, first)
		}
	}
	if first == "" {
		t.Fatalf("LeaderFor(7) returned empty")
	}
}

func TestRing_EmptyReturnsEmpty(t *testing.T) {
	r := New(128)
	if got := r.LeaderFor(0); got != "" {
		t.Fatalf("empty ring should return \"\", got %q", got)
	}
}

func TestRing_DistributionIsRoughlyEven(t *testing.T) {
	r := New(128)
	r.AddNode("n1")
	r.AddNode("n2")
	r.AddNode("n3")

	counts := map[string]int{}
	const N = 30000
	for i := uint32(0); i < N; i++ {
		counts[r.LeaderFor(i)]++
	}
	want := N / 3
	for node, n := range counts {
		if n < want*3/4 || n > want*5/4 {
			t.Errorf("node %s got %d keys, want ~%d (±25%%)", node, n, want)
		}
	}
}

func TestRing_RemoveNodeRedistributes(t *testing.T) {
	r := New(64)
	r.AddNode("n1")
	r.AddNode("n2")
	r.AddNode("n3")

	before := map[uint32]string{}
	for i := uint32(0); i < 100; i++ {
		before[i] = r.LeaderFor(i)
	}

	r.RemoveNode("n2")

	moved := 0
	for i := uint32(0); i < 100; i++ {
		after := r.LeaderFor(i)
		if after == "n2" {
			t.Fatalf("shard %d still mapped to removed node n2", i)
		}
		if after != before[i] {
			moved++
		}
	}
	if moved < 15 || moved > 60 {
		t.Errorf("removing one of three nodes moved %d/100 keys; want roughly 33", moved)
	}
}

func TestRing_AddNodeIdempotent(t *testing.T) {
	r := New(32)
	r.AddNode("n1")
	r.AddNode("n1")
	r.AddNode("n2")

	got := r.Members()
	if len(got) != 2 {
		t.Fatalf("Members() = %v, want 2 distinct nodes", got)
	}
}

func TestRing_MembersIsStable(t *testing.T) {
	r := New(8)
	r.AddNode("n3")
	r.AddNode("n1")
	r.AddNode("n2")
	got := r.Members()
	want := []string{"n1", "n2", "n3"}
	if fmt.Sprintf("%v", got) != fmt.Sprintf("%v", want) {
		t.Fatalf("Members() = %v, want %v (sorted)", got, want)
	}
}
