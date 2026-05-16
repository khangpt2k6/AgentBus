// Demonstrates the agent-event flow: per-session ordering, structured
// envelope, and consumer ack.
//
// Run a broker first:
//
//	broker --grpc-addr=:9095
//
// Then:
//
//	go run ./examples/agent-events
package main

import (
	"context"
	"encoding/json"
	"errors"
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

	// Subscribe to the canonical agent-events topic.
	sub, err := client.Subscribe(ctx, agentbus.DefaultAgentTopic, "billing-service")
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	// Consume in the background, decoding the envelope.
	go func() {
		for {
			msg, err := sub.Next(ctx)
			if err != nil {
				if errors.Is(err, agentbus.ErrSubscriptionClosed) {
					return
				}
				log.Printf("recv: %v", err)
				return
			}
			var env map[string]any
			if err := json.Unmarshal(msg.Payload, &env); err != nil {
				log.Printf("decode: %v (raw=%s)", err, msg.Payload)
				continue
			}
			fmt.Printf("recv  session=%v type=%v step=%v attempt=%v offset=%d\n",
				env["session_id"], env["type"], env["step"], env["attempt"], msg.Offset)
		}
	}()

	// Publish a few events for the same session — they will arrive in order.
	steps := []struct {
		Type string
		Step string
		Body string
	}{
		{"tool.call", "retrieve-context", `{"tool":"search","query":"last order"}`},
		{"tool.result", "retrieve-context", `{"results":[{"order_id":42}]}`},
		{"tool.call", "send-email", `{"to":"user@acme.com","subject":"order found"}`},
		{"tool.result", "send-email", `{"sent":true}`},
	}
	for i, s := range steps {
		res, err := client.PublishAgent(ctx, agentbus.AgentEvent{
			Tenant:    "acme",
			Project:   "support-bot",
			SessionID: "sess-42",
			AgentID:   "planner",
			Type:      s.Type,
			Step:      s.Step,
			Attempt:   1,
			Payload:   []byte(s.Body),
		})
		if err != nil {
			log.Fatalf("publish %d: %v", i, err)
		}
		fmt.Printf("sent  partition=%d offset=%d type=%s step=%s\n",
			res.Partition, res.Offset, s.Type, s.Step)
	}

	time.Sleep(800 * time.Millisecond)
}
