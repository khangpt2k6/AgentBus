package router

import (
	"encoding/json"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/khangpt2k6/AgentBus/internal/cluster/metadata"
)

type applyHelper struct{ t *testing.T }

func newApplyHelper(t *testing.T) *applyHelper { return &applyHelper{t: t} }

func (h *applyHelper) apply(f *metadata.FSM, c metadata.Command) {
	h.t.Helper()
	b, err := json.Marshal(c)
	if err != nil {
		h.t.Fatalf("marshal cmd: %v", err)
	}
	if resp := f.Apply(&raft.Log{Data: b}); resp != nil {
		if err, ok := resp.(error); ok {
			h.t.Fatalf("apply: %v", err)
		}
	}
}
