package cluster

import (
	"reflect"
	"testing"
)

func TestParsePeers_Valid(t *testing.T) {
	got, err := ParsePeers("n1@127.0.0.1:7001,n2@127.0.0.1:7002,n3@127.0.0.1:7003")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []Peer{
		{NodeID: "n1", RaftAddr: "127.0.0.1:7001"},
		{NodeID: "n2", RaftAddr: "127.0.0.1:7002"},
		{NodeID: "n3", RaftAddr: "127.0.0.1:7003"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ParsePeers mismatch:\ngot:  %#v\nwant: %#v", got, want)
	}
}

func TestParsePeers_EmptyStringIsEmptySlice(t *testing.T) {
	got, err := ParsePeers("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("want empty slice, got %v", got)
	}
}

func TestParsePeers_RejectsMalformed(t *testing.T) {
	cases := []string{
		"missing-at-sign:7001",
		"n1@",
		"@127.0.0.1:7001",
		"n1@127.0.0.1",       // no port
		"n1@127.0.0.1:notnum", // bad port
	}
	for _, c := range cases {
		if _, err := ParsePeers(c); err == nil {
			t.Errorf("ParsePeers(%q) want error, got nil", c)
		}
	}
}

func TestConfig_ValidateRequiresNodeID(t *testing.T) {
	c := Config{
		Peers:       []Peer{{NodeID: "n1", RaftAddr: "127.0.0.1:7001"}},
		RaftBind:    "127.0.0.1:7001",
		GossipBind:  "127.0.0.1:8001",
		RaftDir:     "/tmp/raft",
	}
	if err := c.Validate(); err == nil {
		t.Fatal("want error for empty NodeID")
	}
}
