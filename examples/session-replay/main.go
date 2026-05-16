// Demonstrates the agent session replay workflow.
//
// Story: an agent run for session "sess-42" produced 4 events (tool call,
// retry, retry, handoff). Some time later, ops wants to debug it. They
// call ReplaySession with the session id and get the entire run back, in
// the order it happened, ready to print or pipe to a debugger.
//
// Run a broker first:
//
//	broker --grpc-addr=:9095
//
// Then:
//
//	go run ./examples/session-replay
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/khangpt2k6/AgentBus/agentbus"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client, err := agentbus.Connect(ctx, "localhost:9095")
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer client.Close()

	sess := agentbus.SessionRef{
		Tenant:    "acme",
		Project:   "support-bot",
		SessionID: "sess-42",
		AgentID:   "planner",
	}

	// 1) Simulate an agent run that produces a handful of events.
	produceAgentRun(ctx, client, sess)

	// 2) Now replay it — this is what ops would do post-mortem.
	fmt.Println("---- session replay ----")
	events, err := client.ReplaySession(ctx, sess, agentbus.ReplayOptions{})
	if err != nil {
		log.Fatalf("replay: %v", err)
	}
	for _, ev := range events {
		fmt.Printf("[+%d ms] %-14s step=%-20s payload=%s\n",
			ev.CreatedAt.Sub(events[0].CreatedAt).Milliseconds(),
			ev.Type, ev.Step, ev.Payload)
	}
	fmt.Printf("\nTotal: %d events replayed for session %s\n", len(events), sess.SessionID)
}

func produceAgentRun(ctx context.Context, client *agentbus.Client, sess agentbus.SessionRef) {
	// First attempt — succeeds returning empty.
	must(client.PublishToolCall(ctx, sess, agentbus.ToolCall{
		Tool: "search", CallID: "c1",
		Arguments: json.RawMessage(`{"query":"latest order"}`),
	}))
	must(client.PublishToolResult(ctx, sess, agentbus.ToolResult{
		CallID: "c1", Tool: "search",
		Output:  json.RawMessage(`{"results":[]}`),
		Latency: 417 * time.Millisecond,
	}))

	// Second attempt — different query, timeout.
	must(client.PublishToolCall(ctx, sess, agentbus.ToolCall{
		Tool: "search", CallID: "c2",
		Arguments: json.RawMessage(`{"query":"order acme-1042"}`),
	}))
	must(client.PublishToolResult(ctx, sess, agentbus.ToolResult{
		CallID: "c2", Tool: "search",
		Error: "timeout",
	}))

	// Handoff to a different agent.
	must(client.PublishHandoff(ctx, sess, agentbus.Handoff{
		FromAgent: "planner", ToAgent: "escalator",
		Reason: "2 failed search attempts",
	}))

	// Brief sleep so server-side ordering is settled before replay.
	time.Sleep(100 * time.Millisecond)
}

func must(_ agentbus.PublishResult, err error) {
	if err != nil {
		log.Fatalf("publish: %v", err)
	}
}
