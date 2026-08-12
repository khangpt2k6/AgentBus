// Package eventbus is the official Go client for EventBus, a session-ordered
// event bus for AI agents.
//
// Quick start:
//
//	client, err := eventbus.Connect(ctx, "localhost:9095")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer client.Close()
//
//	// Publish a plain message.
//	_, err = client.Publish(ctx, "orders", []byte("hello"))
//
//	// Publish an agent event (per-session ordering).
//	_, err = client.PublishAgent(ctx, eventbus.AgentEvent{
//		Tenant:    "acme",
//		Project:   "support-bot",
//		SessionID: "sess-42",
//		AgentID:   "planner",
//		Type:      "tool.call",
//		Payload:   []byte(`{"tool":"search"}`),
//	})
//
//	// Subscribe.
//	sub, err := client.Subscribe(ctx, "agent-events", "billing")
//	for {
//		msg, err := sub.Next(ctx)
//		if errors.Is(err, eventbus.ErrSubscriptionClosed) {
//			break
//		}
//		fmt.Printf("offset=%d payload=%s\n", msg.Offset, msg.Payload)
//	}
package eventbus
