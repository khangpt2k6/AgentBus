package cli

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/2006t/goqueue/internal/protocol"
	goqueuev1 "github.com/2006t/goqueue/proto"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

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
