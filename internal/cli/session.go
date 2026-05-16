package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/khangpt2k6/AgentBus/agentbus"
	"github.com/spf13/cobra"
)

// newSessionCmd builds the `goqueue session` subcommand group: the workflow
// for debugging a multi-agent run by session id.
func newSessionCmd(opts *options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "session",
		Short: "Replay or tail an agent session for debugging",
		Long: `Tools for inspecting a single agent session by its (tenant, project, session_id) triple.

Use 'session replay' for a full historical dump (e.g. post-mortem after a failure)
and 'session tail' to watch a session live.`,
	}
	cmd.AddCommand(newSessionReplayCmd(opts))
	cmd.AddCommand(newSessionTailCmd(opts))
	return cmd
}

type sessionFlags struct {
	tenant, project, session string
	topic                    string
	partitions               int
	pretty                   bool
	format                   string // "pretty" | "jsonl"
	from                     int64
	max                      int
}

func bindSessionFlags(cmd *cobra.Command, sf *sessionFlags) {
	cmd.Flags().StringVar(&sf.tenant, "tenant", "", "session tenant (required)")
	cmd.Flags().StringVar(&sf.project, "project", "", "session project (required)")
	cmd.Flags().StringVar(&sf.session, "session", "", "session id (required)")
	cmd.Flags().StringVar(&sf.topic, "topic", "agent-events", "topic to scan")
	cmd.Flags().IntVar(&sf.partitions, "partitions", 3, "topic partition count (must match broker)")
	cmd.Flags().StringVar(&sf.format, "format", "pretty", "output format: pretty | jsonl")
	_ = cmd.MarkFlagRequired("tenant")
	_ = cmd.MarkFlagRequired("project")
	_ = cmd.MarkFlagRequired("session")
}

func sessionRef(sf *sessionFlags) agentbus.SessionRef {
	return agentbus.SessionRef{
		Tenant:    sf.tenant,
		Project:   sf.project,
		SessionID: sf.session,
	}
}

func newSessionReplayCmd(opts *options) *cobra.Command {
	sf := &sessionFlags{}
	cmd := &cobra.Command{
		Use:   "replay",
		Short: "Dump every persisted event for a session in order",
		RunE: func(cmd *cobra.Command, args []string) error {
			if !opts.grpc {
				return errors.New("session commands require --grpc")
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
			defer cancel()

			client, err := agentbus.Connect(ctx, opts.addr)
			if err != nil {
				return err
			}
			defer client.Close()

			events, err := client.ReplaySession(ctx, sessionRef(sf), agentbus.ReplayOptions{
				Topic:          sf.topic,
				PartitionCount: sf.partitions,
				FromOffset:     sf.from,
				MaxEvents:      sf.max,
			})
			if err != nil {
				return err
			}
			renderEvents(os.Stdout, events, sf.format)
			fmt.Fprintf(os.Stderr, "\n%d event(s) for session %s\n", len(events), sf.session)
			return nil
		},
	}
	bindSessionFlags(cmd, sf)
	cmd.Flags().Int64Var(&sf.from, "from", 0, "start offset on the session's partition")
	cmd.Flags().IntVar(&sf.max, "max", 0, "max events to return (0 = unlimited)")
	return cmd
}

func newSessionTailCmd(opts *options) *cobra.Command {
	sf := &sessionFlags{}
	cmd := &cobra.Command{
		Use:   "tail",
		Short: "Stream events for a session as they arrive (live)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if !opts.grpc {
				return errors.New("session commands require --grpc")
			}
			// No outer timeout — tail runs until SIGINT or stream close.
			ctx := cmd.Context()

			client, err := agentbus.Connect(ctx, opts.addr)
			if err != nil {
				return err
			}
			defer client.Close()

			sub, err := client.TailSession(ctx, sessionRef(sf), agentbus.ReplayOptions{
				Topic:          sf.topic,
				PartitionCount: sf.partitions,
			})
			if err != nil {
				return err
			}
			defer sub.Close()

			fmt.Fprintf(os.Stderr, "tailing session %s on %s (Ctrl-C to stop)\n", sf.session, sf.topic)
			for {
				ev, err := sub.Next(ctx)
				if err != nil {
					if errors.Is(err, agentbus.ErrSubscriptionClosed) || errors.Is(err, context.Canceled) {
						return nil
					}
					return err
				}
				renderEvents(os.Stdout, []agentbus.DecodedEvent{ev}, sf.format)
			}
		},
	}
	bindSessionFlags(cmd, sf)
	return cmd
}

func renderEvents(w io.Writer, events []agentbus.DecodedEvent, format string) {
	switch format {
	case "jsonl":
		enc := json.NewEncoder(w)
		for _, ev := range events {
			_ = enc.Encode(ev)
		}
	default: // pretty
		for _, ev := range events {
			renderPretty(w, ev)
		}
	}
}

func renderPretty(w io.Writer, ev agentbus.DecodedEvent) {
	ts := ev.Timestamp
	if ts.IsZero() {
		ts = ev.CreatedAt
	}
	tsStr := ts.UTC().Format("15:04:05.000")

	step := ev.Step
	if step != "" {
		step = " " + step
	}
	attempt := ""
	if ev.Attempt > 1 {
		attempt = fmt.Sprintf(" (attempt %d)", ev.Attempt)
	}
	fmt.Fprintf(w, "[%s] off=%-6d %-14s%s%s  agent=%s\n",
		tsStr, ev.Offset, ev.Type, step, attempt, ev.AgentID)
	// Indent payload one level for readability. Skip if too large.
	if len(ev.Payload) > 0 && len(ev.Payload) < 4096 {
		var pretty bytes.Buffer
		if err := json.Indent(&pretty, ev.Payload, "    ", "  "); err == nil {
			fmt.Fprintf(w, "    %s\n", pretty.String())
		} else {
			fmt.Fprintf(w, "    %s\n", ev.Payload)
		}
	}
}
