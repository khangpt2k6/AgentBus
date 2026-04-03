package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/2006t/goqueue/internal/protocol"
	goqueuev1 "github.com/2006t/goqueue/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "publish":
		publishCmd(os.Args[2:])
	case "publish-batch":
		publishBatchCmd(os.Args[2:])
	case "consume":
		consumeCmd(os.Args[2:])
	case "fetch":
		fetchCmd(os.Args[2:])
	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Println("goqueue publish --topic orders --addr localhost:9090 \"hello\"")
	fmt.Println("goqueue publish --grpc --addr localhost:9095 --topic orders --key user-42 \"hello\"")
	fmt.Println("goqueue publish --grpc --addr localhost:9095 --topic orders --partition 1 \"hello\"")
	fmt.Println("goqueue publish-batch --addr localhost:9090 --topic orders --count 100 --payload \"hello\"")
	fmt.Println("goqueue consume --topic orders --group payment-service --addr localhost:9090")
	fmt.Println("goqueue consume --grpc --addr localhost:9095 --topic orders --group payment-service --partition -1")
	fmt.Println("goqueue fetch --addr localhost:9090 --topic orders --offset 0 --max 100")
}

func publishCmd(args []string) {
	fs := flag.NewFlagSet("publish", flag.ExitOnError)
	addr := fs.String("addr", "localhost:9090", "broker tcp address")
	topic := fs.String("topic", "", "topic name")
	useGRPC := fs.Bool("grpc", false, "use gRPC transport")
	key := fs.String("key", "", "partition key (gRPC only)")
	partition := fs.Int("partition", -1, "target partition (gRPC only)")
	_ = fs.Parse(args)

	if *topic == "" || fs.NArg() < 1 {
		log.Fatal("publish requires --topic and message payload")
	}
	msg := fs.Arg(0)
	if *useGRPC {
		publishGRPC(*addr, *topic, *key, *partition, msg)
		return
	}
	publishTCP(*addr, *topic, msg)
}

func publishTCP(addr, topic, msg string) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatalf("dial broker: %v", err)
	}
	defer conn.Close()

	if err := protocol.Encode(conn, &protocol.Frame{
		Op:      protocol.OpPublish,
		Topic:   topic,
		Payload: []byte(msg),
	}); err != nil {
		log.Fatalf("send publish: %v", err)
	}
	resp, err := protocol.Decode(conn)
	if err != nil {
		log.Fatalf("read ack: %v", err)
	}
	if resp.Op == protocol.OpError {
		log.Fatalf("broker error: %s", string(resp.Payload))
	}
	if len(resp.Payload) == 8 {
		offset := int64(binary.BigEndian.Uint64(resp.Payload))
		fmt.Printf("published topic=%s offset=%d\n", topic, offset)
		return
	}
	fmt.Printf("published topic=%s\n", topic)
}

func publishGRPC(addr, topic, key string, partition int, msg string) {
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("dial grpc broker: %v", err)
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
		log.Fatalf("grpc publish failed: %v", err)
	}
	fmt.Printf("published topic=%s partition=%d offset=%d\n", topic, resp.Partition, resp.Offset)
}

func publishBatchCmd(args []string) {
	fs := flag.NewFlagSet("publish-batch", flag.ExitOnError)
	addr := fs.String("addr", "localhost:9090", "broker tcp address")
	topic := fs.String("topic", "", "topic name")
	count := fs.Int("count", 100, "number of messages in batch")
	payload := fs.String("payload", "", "payload text (used when no positional args)")
	_ = fs.Parse(args)

	if *topic == "" {
		log.Fatal("publish-batch requires --topic")
	}
	if *count <= 0 {
		log.Fatal("publish-batch requires --count > 0")
	}

	var batch [][]byte
	if fs.NArg() > 0 {
		batch = make([][]byte, 0, fs.NArg())
		for _, a := range fs.Args() {
			batch = append(batch, []byte(a))
		}
	} else {
		msg := *payload
		if msg == "" {
			msg = "hello"
		}
		batch = make([][]byte, *count)
		for i := range *count {
			batch[i] = []byte(msg)
		}
	}

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		log.Fatalf("dial broker: %v", err)
	}
	defer conn.Close()

	if err := protocol.Encode(conn, &protocol.Frame{
		Op:      protocol.OpBatchPublish,
		Topic:   *topic,
		Payload: protocol.EncodeBatchPayload(batch),
	}); err != nil {
		log.Fatalf("send batch publish: %v", err)
	}
	resp, err := protocol.Decode(conn)
	if err != nil {
		log.Fatalf("read batch ack: %v", err)
	}
	if resp.Op == protocol.OpError {
		log.Fatalf("broker error: %s", string(resp.Payload))
	}
	first, last, n, err := protocol.DecodeBatchAck(resp.Payload)
	if err != nil {
		log.Fatalf("decode batch ack: %v", err)
	}
	fmt.Printf("batch published topic=%s count=%d first_offset=%d last_offset=%d\n", *topic, n, first, last)
}

func consumeCmd(args []string) {
	fs := flag.NewFlagSet("consume", flag.ExitOnError)
	addr := fs.String("addr", "localhost:9090", "broker tcp address")
	topic := fs.String("topic", "", "topic name")
	group := fs.String("group", "default", "consumer group")
	useGRPC := fs.Bool("grpc", false, "use gRPC transport")
	partition := fs.Int("partition", -1, "partition id (gRPC only, -1 auto)")
	_ = fs.Parse(args)

	if *topic == "" {
		log.Fatal("consume requires --topic")
	}
	if *useGRPC {
		consumeGRPC(*addr, *topic, *group, *partition)
		return
	}
	consumeTCP(*addr, *topic, *group)
}

func consumeTCP(addr, topic, group string) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatalf("dial broker: %v", err)
	}
	defer conn.Close()

	if err := protocol.Encode(conn, &protocol.Frame{
		Op:      protocol.OpSubscribe,
		Topic:   topic,
		Payload: []byte(group),
	}); err != nil {
		log.Fatalf("send subscribe: %v", err)
	}

	// Simple signal handling to make ctrl+c exit cleanly.
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
				return
			}
			log.Fatalf("read message: %v", err)
		}
		if frame.Op == protocol.OpError {
			log.Fatalf("broker error: %s", string(frame.Payload))
		}
		if frame.Op != protocol.OpMessage {
			continue
		}
		fmt.Printf("[%s] %s\n", frame.Topic, string(frame.Payload))
	}
}

func consumeGRPC(addr, topic, group string, partition int) {
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("dial grpc broker: %v", err)
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
		log.Fatalf("grpc consume stream failed: %v", err)
	}

	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF || ctx.Err() != nil {
				return
			}
			log.Fatalf("grpc read message failed: %v", err)
		}
		fmt.Printf("[%s p=%d] %s\n", topic, msg.Partition, string(msg.Payload))
	}
}

func fetchCmd(args []string) {
	fs := flag.NewFlagSet("fetch", flag.ExitOnError)
	addr := fs.String("addr", "localhost:9090", "broker tcp address")
	topic := fs.String("topic", "", "topic name")
	offset := fs.Int64("offset", 0, "starting offset")
	maxCount := fs.Int("max", 100, "maximum number of messages to fetch")
	_ = fs.Parse(args)

	if *topic == "" {
		log.Fatal("fetch requires --topic")
	}

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		log.Fatalf("dial broker: %v", err)
	}
	defer conn.Close()

	req := protocol.EncodeFetchRequest(*offset, *maxCount)
	if err := protocol.Encode(conn, &protocol.Frame{
		Op:      protocol.OpFetch,
		Topic:   *topic,
		Payload: req,
	}); err != nil {
		log.Fatalf("send fetch: %v", err)
	}
	resp, err := protocol.Decode(conn)
	if err != nil {
		log.Fatalf("read fetch response: %v", err)
	}
	if resp.Op == protocol.OpError {
		log.Fatalf("broker error: %s", string(resp.Payload))
	}
	msgs, err := protocol.DecodeFetchResponse(resp.Payload)
	if err != nil {
		log.Fatalf("decode fetch response: %v", err)
	}
	for _, m := range msgs {
		fmt.Printf("[%s off=%d] %s\n", *topic, m.Offset, string(m.Payload))
	}
}
