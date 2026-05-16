package agentbus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	pb "github.com/khangpt2k6/AgentBus/proto"
)

// Message is one delivered message.
type Message struct {
	Partition int32
	Offset    int64
	Payload   []byte
	Timestamp time.Time
}

// SubscribeOptions configures a Subscribe call.
type SubscribeOptions struct {
	// Partition selects a single partition. -1 (default) reads across all
	// partitions, using a hash of the consumer group to pick one. For the
	// guaranteed-ordering case where you need to consume ALL partitions, run
	// one Subscribe per partition.
	Partition int32
}

// Subscribe opens a server-streaming consumer on topic for the given group.
// The broker delivers messages starting from the group's last committed
// offset (or the latest message if no offset is committed yet).
//
// Call Next() in a loop; it blocks until a message arrives or the context
// is canceled. Always defer sub.Close() to release the underlying stream.
func (c *Client) Subscribe(ctx context.Context, topic, group string) (*Subscription, error) {
	return c.SubscribeWithOptions(ctx, topic, group, SubscribeOptions{Partition: -1})
}

// SubscribeWithOptions is like Subscribe but lets the caller pin a specific
// partition.
func (c *Client) SubscribeWithOptions(ctx context.Context, topic, group string, opts SubscribeOptions) (*Subscription, error) {
	if topic == "" {
		return nil, errors.New("agentbus: topic is required")
	}
	if group == "" {
		group = "default"
	}
	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := c.api.Consume(streamCtx, &pb.ConsumeRequest{
		Topic:     topic,
		Group:     group,
		Partition: opts.Partition,
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("agentbus: open consume stream: %w", err)
	}
	return &Subscription{stream: stream, cancel: cancel}, nil
}

// Subscription is an iterator over a stream of messages.
//
// Not safe for concurrent use — drive it from a single goroutine. To fan
// out, copy messages onto your own channel after Next() returns.
type Subscription struct {
	stream pb.BrokerService_ConsumeClient
	cancel context.CancelFunc
}

// Next blocks until the next message is available, the context is canceled,
// or the stream ends. Returns ErrSubscriptionClosed when the stream is
// closed normally.
func (s *Subscription) Next(ctx context.Context) (Message, error) {
	// We could honor ctx by spawning a goroutine, but gRPC propagates the
	// stream's context, which was set at Subscribe time. Callers wanting
	// to abort should cancel the context they passed to Subscribe.
	_ = ctx
	msg, err := s.stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
			return Message{}, ErrSubscriptionClosed
		}
		return Message{}, err
	}
	return Message{
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Payload:   msg.Payload,
		Timestamp: time.Unix(0, msg.TimestampUnixNano),
	}, nil
}

// Close terminates the subscription. Safe to call multiple times.
func (s *Subscription) Close() error {
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}
	return nil
}
