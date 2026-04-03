package cli

import (
	"fmt"
	"net"

	"github.com/2006t/goqueue/internal/protocol"
	"github.com/spf13/cobra"
)

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
