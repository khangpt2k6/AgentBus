package cli

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/khangpt2k6/AgentBus/internal/agentstream"
	"github.com/spf13/cobra"
)

func newRetryAgentCmd(opts *options) *cobra.Command {
	var (
		topic       string
		dlqTopic    string
		eventRaw    string
		maxAttempts int
		delay       time.Duration
	)

	cmd := &cobra.Command{
		Use:   "retry-agent",
		Short: "Route failed agent event to retry or DLQ",
		Long:  "Takes a serialized agent event JSON, increments attempt, and republishes either to source topic (retry) or DLQ.",
		RunE: func(cmd *cobra.Command, args []string) error {
			if !json.Valid([]byte(eventRaw)) {
				return fmt.Errorf("--event must be valid JSON")
			}
			var ev agentstream.Event
			if err := json.Unmarshal([]byte(eventRaw), &ev); err != nil {
				return fmt.Errorf("decode --event: %w", err)
			}
			if err := ev.Validate(); err != nil {
				return err
			}

			toDLQ, nextAttempt := agentstream.DecideRetryRoute(ev.Attempt, maxAttempts)
			ev.Attempt = nextAttempt
			encoded, err := ev.Marshal()
			if err != nil {
				return err
			}
			key := agentstream.SessionKey(ev.Tenant, ev.Project, ev.SessionID)

			targetTopic := topic
			action := "retry"
			if toDLQ {
				targetTopic = agentstream.ResolveDLQTopic(topic, dlqTopic)
				action = "dlq"
			} else if delay > 0 {
				time.Sleep(delay)
			}

			if opts.grpc {
				if err := publishGRPC(opts.addr, targetTopic, key, -1, string(encoded)); err != nil {
					return err
				}
			} else {
				if err := publishTCP(opts.addr, targetTopic, key, string(encoded)); err != nil {
					return err
				}
			}

			fmt.Printf("agent event routed action=%s topic=%s attempt=%d session=%s\n", action, targetTopic, ev.Attempt, ev.SessionID)
			return nil
		},
	}

	cmd.Flags().StringVar(&topic, "topic", "agent-events", "source topic for retries")
	cmd.Flags().StringVar(&dlqTopic, "dlq-topic", "", "dead-letter topic override (default: <topic>.dlq)")
	cmd.Flags().StringVar(&eventRaw, "event", "", "failed event JSON payload")
	cmd.Flags().IntVar(&maxAttempts, "max-attempts", 3, "maximum attempts before routing to DLQ")
	cmd.Flags().DurationVar(&delay, "delay", 0, "delay before retry publish (ignored for DLQ routing)")

	_ = cmd.MarkFlagRequired("event")

	return cmd
}
