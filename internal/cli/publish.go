package cli

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"time"

	"github.com/2006t/goqueue/internal/protocol"
	goqueuev1 "github.com/2006t/goqueue/proto"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func newPublishCmd(opts *options) *cobra.Command {
	var (
		topic     string
		key       string
		partition int
	)
	cmd := &cobra.Command{
		Use:   "publish [message]",
		Short: "Publish a single message",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.grpc {
				return publishGRPC(opts.addr, topic, key, partition, args[0])
			}
			return publishTCP(opts.addr, topic, args[0])
		},
	}
	cmd.Flags().StringVar(&topic, "topic", "", "topic name")
	cmd.Flags().StringVar(&key, "key", "", "partition key (gRPC only)")
	cmd.Flags().IntVar(&partition, "partition", -1, "target partition (gRPC only)")
	_ = cmd.MarkFlagRequired("topic")
	return cmd
}

func newPublishBatchCmd(opts *options) *cobra.Command {
	var (
		topic   string
		count   int
		payload string
	)
	cmd := &cobra.Command{
		Use:   "publish-batch [messages...]",
		Short: "Publish a batch of messages (TCP only)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.grpc {
				return fmt.Errorf("publish-batch currently supports TCP only")
			}
			if count <= 0 {
				return fmt.Errorf("--count must be > 0")
			}
			var batch [][]byte
			if len(args) > 0 {
				batch = make([][]byte, 0, len(args))
				for _, a := range args {
					batch = append(batch, []byte(a))
				}
			} else {
				msg := payload
				if msg == "" {
					msg = "hello"
				}
				batch = make([][]byte, count)
				for i := range count {
					batch[i] = []byte(msg)
				}
			}
			return publishBatchTCP(opts.addr, topic, batch)
		},
	}
	cmd.Flags().StringVar(&topic, "topic", "", "topic name")
	cmd.Flags().IntVar(&count, "count", 100, "number of generated messages when no positional args are passed")
	cmd.Flags().StringVar(&payload, "payload", "", "payload text used with --count")
	_ = cmd.MarkFlagRequired("topic")
	return cmd
}

func publishTCP(addr, topic, msg string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("dial broker: %w", err)
	}
	defer conn.Close()

	if err := protocol.Encode(conn, &protocol.Frame{
		Op:      protocol.OpPublish,
		Topic:   topic,
		Payload: []byte(msg),
	}); err != nil {
		return fmt.Errorf("send publish: %w", err)
	}
	resp, err := protocol.Decode(conn)
	if err != nil {
		return fmt.Errorf("read ack: %w", err)
	}
	if resp.Op == protocol.OpError {
		return fmt.Errorf("broker error: %s", string(resp.Payload))
	}
	if len(resp.Payload) == 8 {
		offset := int64(binary.BigEndian.Uint64(resp.Payload))
		fmt.Printf("published topic=%s offset=%d\n", topic, offset)
		return nil
	}
	fmt.Printf("published topic=%s\n", topic)
	return nil
}

func publishBatchTCP(addr, topic string, batch [][]byte) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("dial broker: %w", err)
	}
	defer conn.Close()

	if err := protocol.Encode(conn, &protocol.Frame{
		Op:      protocol.OpBatchPublish,
		Topic:   topic,
		Payload: protocol.EncodeBatchPayload(batch),
	}); err != nil {
		return fmt.Errorf("send batch publish: %w", err)
	}
	resp, err := protocol.Decode(conn)
	if err != nil {
		return fmt.Errorf("read batch ack: %w", err)
	}
	if resp.Op == protocol.OpError {
		return fmt.Errorf("broker error: %s", string(resp.Payload))
	}
	first, last, n, err := protocol.DecodeBatchAck(resp.Payload)
	if err != nil {
		return fmt.Errorf("decode batch ack: %w", err)
	}
	fmt.Printf("batch published topic=%s count=%d first_offset=%d last_offset=%d\n", topic, n, first, last)
	return nil
}

func publishGRPC(addr, topic, key string, partition int, msg string) error {
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("dial grpc broker: %w", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := goqueuev1.NewBrokerServiceClient(conn)
	resp, err := client.Publish(ctx, &goqueuev1.PublishRequest{
		Topic:     topic,
		Payload:   []byte(msg),
		Key:       key,
		Partition: int32(partition),
	})
	if err != nil {
		return fmt.Errorf("grpc publish failed: %w", err)
	}
	fmt.Printf("published topic=%s partition=%d offset=%d\n", topic, resp.Partition, resp.Offset)
	return nil
}
