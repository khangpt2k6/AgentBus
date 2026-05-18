package assigner

import (
	"encoding/json"
	"fmt"

	"github.com/hashicorp/raft"
	"github.com/khangpt2k6/AgentBus/internal/cluster/metadata"
)

type jsonApplyHelper struct{}

func (h jsonApplyHelper) do(f *metadata.FSM, c metadata.Command) error {
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	if resp := f.Apply(&raft.Log{Data: b}); resp != nil {
		if err, ok := resp.(error); ok {
			return fmt.Errorf("fsm: %w", err)
		}
	}
	return nil
}
