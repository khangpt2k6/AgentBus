package cli

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/2006t/goqueue/internal/protocol"
	goqueuev1 "github.com/2006t/goqueue/proto"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type options struct {
	addr string
	grpc bool
}

func Execute() error {
	return newRootCmd().Execute()
}

func newRootCmd() *cobra.Command {
	opts := &options{}

	root := &cobra.Command{
		Use:   "goqueue",
		Short: "GoQueue command line client",
		Long:  "GoQueue CLI publishes, consumes, and fetches messages over TCP or gRPC.",
	}

	root.PersistentFlags().StringVar(&opts.addr, "addr", "localhost:9090", "broker address")
	root.PersistentFlags().BoolVar(&opts.grpc, "grpc", false, "use gRPC transport")

	root.AddCommand(newPublishCmd(opts))
	root.AddCommand(newPublishBatchCmd(opts))
	root.AddCommand(newConsumeCmd(opts))
	root.AddCommand(newFetchCmd(opts))
	root.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print CLI version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("goqueue dev")
		},
	})

	return root
}

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

func newConsumeCmd(opts *options) *cobra.Command {
	var (
		topic     string
		group     string
		partition int
	)
	cmd := &cobra.Command{
		Use:   "consume",
		Short: "Consume messages from a topic",
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.grpc {
				return consumeGRPC(opts.addr, topic, group, partition)
			}
			return consumeTCP(opts.addr, topic, group)
		},
	}
	cmd.Flags().StringVar(&topic, "topic", "", "topic name")
	cmd.Flags().StringVar(&group, "group", "default", "consumer group")
	cmd.Flags().IntVar(&partition, "partition", -1, "partition id (gRPC only, -1 auto)")
	_ = cmd.MarkFlagRequired("topic")
	return cmd
}

func consumeTCP(addr, topic, group string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("dial broker: %w", err)
	}
	defer conn.Close()

	if err := protocol.Encode(conn, &protocol.Frame{
		Op:      protocol.OpSubscribe,
		Topic:   topic,
		Payload: []byte(group),
	}); err != nil {
		return fmt.Errorf("send subscribe: %w", err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		_ = conn.Close()
		os.Exit(0)
	}()

	for {
		frame, err := protocol.Decode(conn)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("read message: %w", err)
		}
		if frame.Op == protocol.OpError {
			return fmt.Errorf("broker error: %s", string(frame.Payload))
		}
		if frame.Op != protocol.OpMessage {
			continue
		}
		fmt.Printf("[%s] %s\n", frame.Topic, string(frame.Payload))
	}
}

func consumeGRPC(addr, topic, group string, partition int) error {
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("dial grpc broker: %w", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		cancel()
	}()

	client := goqueuev1.NewBrokerServiceClient(conn)
	stream, err := client.Consume(ctx, &goqueuev1.ConsumeRequest{
		Topic:     topic,
		Group:     group,
		Partition: int32(partition),
	})
	if err != nil {
		return fmt.Errorf("grpc consume stream failed: %w", err)
	}

	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF || ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("grpc read message failed: %w", err)
		}
		fmt.Printf("[%s p=%d] %s\n", topic, msg.Partition, string(msg.Payload))
	}
}

func newFetchCmd(opts *options) *cobra.Command {
	var (
		topic    string
		offset   int64
		maxCount int
	)
	cmd := &cobra.Command{
		Use:   "fetch",
		Short: "Fetch messages from a topic by offset (TCP only)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.grpc {
				return fmt.Errorf("fetch currently supports TCP only")
			}
			return fetchTCP(opts.addr, topic, offset, maxCount)
		},
	}
	cmd.Flags().StringVar(&topic, "topic", "", "topic name")
	cmd.Flags().Int64Var(&offset, "offset", 0, "starting offset")
	cmd.Flags().IntVar(&maxCount, "max", 100, "maximum number of messages to fetch")
	_ = cmd.MarkFlagRequired("topic")
	return cmd
}

func fetchTCP(addr, topic string, offset int64, maxCount int) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("dial broker: %w", err)
	}
	defer conn.Close()

	req := protocol.EncodeFetchRequest(offset, maxCount)
	if err := protocol.Encode(conn, &protocol.Frame{
		Op:      protocol.OpFetch,
		Topic:   topic,
		Payload: req,
	}); err != nil {
		return fmt.Errorf("send fetch: %w", err)
	}
	resp, err := protocol.Decode(conn)
	if err != nil {
		return fmt.Errorf("read fetch response: %w", err)
	}
	if resp.Op == protocol.OpError {
		return fmt.Errorf("broker error: %s", string(resp.Payload))
	}
	msgs, err := protocol.DecodeFetchResponse(resp.Payload)
	if err != nil {
		return fmt.Errorf("decode fetch response: %w", err)
	}
	for _, m := range msgs {
		fmt.Printf("[%s off=%d] %s\n", topic, m.Offset, string(m.Payload))
	}
	return nil
}
