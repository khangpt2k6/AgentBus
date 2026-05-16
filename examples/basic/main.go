// Basic publish + subscribe round-trip against a running AgentBus broker.
//
// Run a broker first:
//
//	broker --grpc-addr=:9095
//
// Then:
//
//	go run ./examples/basic
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/khangpt2k6/AgentBus/agentbus"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := agentbus.Connect(ctx, "localhost:9095")
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer client.Close()

	// Subscribe before publishing so we don't race the broker.
	sub, err := client.Subscribe(ctx, "demo-orders", "demo-consumer")
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	// Consume in the background.
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
			fmt.Printf("recv  partition=%d offset=%d payload=%s\n",
				msg.Partition, msg.Offset, msg.Payload)
		}
	}()

	// Publish a few messages.
	for i := 0; i < 3; i++ {
		res, err := client.Publish(ctx, "demo-orders", []byte(fmt.Sprintf("order-%d", i)))
		if err != nil {
			log.Fatalf("publish: %v", err)
		}
		fmt.Printf("sent  partition=%d offset=%d\n", res.Partition, res.Offset)
	}

	// Give the consumer a moment to drain. In real code, use proper sync.
	time.Sleep(500 * time.Millisecond)
}
