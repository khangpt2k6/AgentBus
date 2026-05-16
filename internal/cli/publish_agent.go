package cli

import (
	"encoding/json"
	"fmt"

	"github.com/khangpt2k6/AgentBus/internal/agentstream"
	"github.com/spf13/cobra"
)

func newPublishAgentCmd(opts *options) *cobra.Command {
	var (
		topic     string
		tenant    string
		project   string
		sessionID string
		agentID   string
		eventType string
		step      string
		attempt   int
		payload   string
		partition int
	)

	cmd := &cobra.Command{
		Use:   "publish-agent",
		Short: "Publish a standardized multi-agent event envelope",
		RunE: func(cmd *cobra.Command, args []string) error {
			eventPayload := json.RawMessage(payload)
			if len(eventPayload) == 0 {
				eventPayload = json.RawMessage(`{}`)
			}
			if !json.Valid(eventPayload) {
				return fmt.Errorf("--payload must be valid JSON")
			}

			event := agentstream.Event{
				Type:      eventType,
				Tenant:    tenant,
				Project:   project,
				SessionID: sessionID,
				AgentID:   agentID,
				Step:      step,
				Attempt:   attempt,
				Payload:   eventPayload,
			}
			encoded, err := event.Marshal()
			if err != nil {
				return err
			}
			key := agentstream.SessionKey(tenant, project, sessionID)
			if opts.grpc {
				return publishGRPC(opts.addr, topic, key, partition, string(encoded))
			}
			return publishTCP(opts.addr, topic, key, string(encoded))
		},
	}

	cmd.Flags().StringVar(&topic, "topic", "agent-events", "topic name")
	cmd.Flags().StringVar(&tenant, "tenant", "", "tenant id")
	cmd.Flags().StringVar(&project, "project", "", "project id")
	cmd.Flags().StringVar(&sessionID, "session", "", "agent session id")
	cmd.Flags().StringVar(&agentID, "agent", "", "agent id")
	cmd.Flags().StringVar(&eventType, "type", "", "event type (e.g. token.chunk, tool.call, tool.result)")
	cmd.Flags().StringVar(&step, "step", "", "pipeline step name")
	cmd.Flags().IntVar(&attempt, "attempt", 1, "retry attempt number")
	cmd.Flags().StringVar(&payload, "payload", "{}", "event payload as JSON object/string")
	cmd.Flags().IntVar(&partition, "partition", -1, "target partition (gRPC only)")

	_ = cmd.MarkFlagRequired("tenant")
	_ = cmd.MarkFlagRequired("project")
	_ = cmd.MarkFlagRequired("session")
	_ = cmd.MarkFlagRequired("agent")
	_ = cmd.MarkFlagRequired("type")

	return cmd
}
