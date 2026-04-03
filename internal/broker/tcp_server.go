package broker

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/2006t/goqueue/internal/consumer"
	"github.com/2006t/goqueue/internal/metrics"
	"github.com/2006t/goqueue/internal/protocol"
	"github.com/2006t/goqueue/internal/wal"
)

type TCPServer struct {
	addr     string
	broker   *Broker
	wal      *wal.Log
	groups   *consumer.Manager
	metrics  *metrics.Metrics
	listener net.Listener

	activeConns sync.WaitGroup
	connCount   atomic.Int64
}

func NewTCPServer(addr string, b *Broker, l *wal.Log, g *consumer.Manager, m *metrics.Metrics) *TCPServer {
	return &TCPServer{
		addr:    addr,
		broker:  b,
		wal:     l,
		groups:  g,
		metrics: m,
	}
}

func (s *TCPServer) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.listener = ln
	log.Printf("tcp broker listening on %s", s.addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		s.activeConns.Add(1)
		s.connCount.Add(1)
		go func() {
			defer s.activeConns.Done()
			defer s.connCount.Add(-1)
			s.handleConn(conn)
		}()
	}
}

// Shutdown stops accepting new connections and waits for existing ones to finish.
func (s *TCPServer) Shutdown() {
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.activeConns.Wait()
}

func (s *TCPServer) ConnCount() int64 { return s.connCount.Load() }

func (s *TCPServer) handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		frame, err := protocol.Decode(conn)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Printf("decode frame error: %v", err)
			}
			return
		}
		if frame.Topic == "" {
			_ = protocol.Encode(conn, &protocol.Frame{Op: protocol.OpError, Topic: "error", Payload: []byte("topic required")})
			continue
		}

		switch frame.Op {
		case protocol.OpPublish:
			if err := s.handlePublish(conn, frame); err != nil {
				log.Printf("publish error: %v", err)
				return
			}
		case protocol.OpBatchPublish:
			if err := s.handleBatchPublish(conn, frame); err != nil {
				log.Printf("batch publish error: %v", err)
				return
			}
		case protocol.OpFetch:
			if err := s.handleFetch(conn, frame); err != nil {
				log.Printf("fetch error: %v", err)
				return
			}
		case protocol.OpSubscribe:
			if err := s.handleSubscribe(conn, frame); err != nil && !errors.Is(err, io.EOF) {
				log.Printf("subscribe error: %v", err)
			}
			return
		default:
			_ = protocol.Encode(conn, &protocol.Frame{Op: protocol.OpError, Topic: frame.Topic, Payload: []byte("unsupported operation")})
		}
	}
}

func (s *TCPServer) handleBatchPublish(conn net.Conn, frame *protocol.Frame) error {
	start := time.Now()
	payloads, err := protocol.DecodeBatchPayload(frame.Payload)
	if err != nil {
		return protocol.Encode(conn, &protocol.Frame{
			Op:      protocol.OpError,
			Topic:   frame.Topic,
			Payload: []byte("invalid batch payload"),
		})
	}
	if len(payloads) == 0 {
		return protocol.Encode(conn, &protocol.Frame{
			Op:      protocol.OpError,
			Topic:   frame.Topic,
			Payload: []byte("empty batch"),
		})
	}

	var (
		firstOffset int64 = -1
		lastOffset  int64
	)
	for _, p := range payloads {
		if s.wal != nil {
			if err := s.wal.Append(frame.Topic, p); err != nil {
				return err
			}
		}
		offset := s.broker.Publish(frame.Topic, p)
		if firstOffset < 0 {
			firstOffset = offset
		}
		lastOffset = offset
	}

	if s.metrics != nil {
		s.metrics.PublishedTotal.Add(float64(len(payloads)))
		s.metrics.ObservePublishLatency(start)
	}

	return protocol.Encode(conn, &protocol.Frame{
		Op:      protocol.OpBatchAck,
		Topic:   frame.Topic,
		Payload: protocol.EncodeBatchAck(firstOffset, lastOffset, len(payloads)),
	})
}

func (s *TCPServer) handleFetch(conn net.Conn, frame *protocol.Frame) error {
	offset, maxCount, err := protocol.DecodeFetchRequest(frame.Payload)
	if err != nil {
		return protocol.Encode(conn, &protocol.Frame{
			Op:      protocol.OpError,
			Topic:   frame.Topic,
			Payload: []byte("invalid fetch payload"),
		})
	}
	if maxCount <= 0 {
		maxCount = 1
	}
	if maxCount > 4096 {
		maxCount = 4096
	}

	msgs := s.broker.Fetch(frame.Topic, offset, maxCount)
	out := make([]protocol.BatchMessage, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, protocol.BatchMessage{
			Offset:  m.Offset,
			Payload: m.Payload,
		})
	}
	return protocol.Encode(conn, &protocol.Frame{
		Op:      protocol.OpFetch,
		Topic:   frame.Topic,
		Payload: protocol.EncodeFetchResponse(out),
	})
}

func (s *TCPServer) handlePublish(conn net.Conn, frame *protocol.Frame) error {
	start := time.Now()
	if s.wal != nil {
		if err := s.wal.Append(frame.Topic, frame.Payload); err != nil {
			return err
		}
	}
	offset := s.broker.Publish(frame.Topic, frame.Payload)
	if s.metrics != nil {
		s.metrics.PublishedTotal.Inc()
		s.metrics.ObservePublishLatency(start)
	}
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, uint64(offset))
	return protocol.Encode(conn, &protocol.Frame{
		Op:      protocol.OpAck,
		Topic:   frame.Topic,
		Payload: out,
	})
}

func (s *TCPServer) handleSubscribe(conn net.Conn, frame *protocol.Frame) error {
	group := string(frame.Payload)
	if group == "" {
		group = "default"
	}

	startOffset := int64(-1)
	if committed, ok := s.groups.Get(frame.Topic, group); ok {
		startOffset = committed
	}
	sub := s.broker.SubscribeAt(frame.Topic, group, startOffset)
	defer s.broker.Unsubscribe(sub)

	ctx := context.Background()
	for {
		msgs, err := sub.Next(ctx, 128)
		if err != nil {
			return err
		}
		for _, msg := range msgs {
			if err := protocol.Encode(conn, &protocol.Frame{
				Op:      protocol.OpMessage,
				Topic:   frame.Topic,
				Payload: msg.Payload,
			}); err != nil {
				return err
			}
		}

		latestOffset := msgs[len(msgs)-1].Offset + 1
		s.groups.Commit(frame.Topic, group, latestOffset)
		s.broker.AddConsumed(int64(len(msgs)))
		if s.metrics != nil {
			s.metrics.ConsumedTotal.Add(float64(len(msgs)))
			head, tail, err := s.broker.TopicInfo(frame.Topic)
			if err == nil {
				lag := tail - latestOffset
				if latestOffset < head {
					lag = tail - head
				}
				s.metrics.ConsumerLag.WithLabelValues(frame.Topic, group).Set(float64(lag))
			}
		}
	}
}
