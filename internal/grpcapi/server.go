package grpcapi

import (
	"context"
	"io"
	"time"

	"github.com/khangpt2k6/AgentBus/internal/broker"
	"github.com/khangpt2k6/AgentBus/internal/consumer"
	"github.com/khangpt2k6/AgentBus/internal/metrics"
	"github.com/khangpt2k6/AgentBus/internal/wal"
	goqueuev1 "github.com/khangpt2k6/AgentBus/proto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	goqueuev1.UnimplementedBrokerServiceServer

	broker  *broker.Broker
	groups  *consumer.Manager
	metrics *metrics.Metrics
	wal     *wal.Log
}

func NewServer(b *broker.Broker, g *consumer.Manager, m *metrics.Metrics, l *wal.Log) *Server {
	return &Server{broker: b, groups: g, metrics: m, wal: l}
}

func (s *Server) Publish(ctx context.Context, req *goqueuev1.PublishRequest) (*goqueuev1.PublishResponse, error) {
	ctx, span := otel.Tracer("goqueue.grpcapi").Start(ctx, "BrokerService.Publish")
	defer span.End()
	span.SetAttributes(
		attribute.String("topic", req.Topic),
		attribute.String("key", req.Key),
		attribute.Int("payload_bytes", len(req.Payload)),
		attribute.Int64("requested_partition", int64(req.Partition)),
	)

	if req.Topic == "" {
		span.SetStatus(otelcodes.Error, "topic required")
		return nil, status.Error(codes.InvalidArgument, "topic is required")
	}
	start := time.Now()
	// Resolve partition upfront (validates explicit, derives from key otherwise)
	// so we can write the WAL with the correct partition BEFORE mutating
	// in-memory broker state. WAL-first ordering prevents the case where a
	// failed fsync leaves consumers having seen a message the producer thinks
	// failed.
	var partition int
	if req.Partition >= 0 {
		// Ensure topic exists with default config, then validate partition.
		s.broker.EnsureTopic(req.Topic, 0)
		if int(req.Partition) >= s.broker.PartitionCount(req.Topic) {
			err := status.Errorf(codes.InvalidArgument, "invalid partition %d", req.Partition)
			span.RecordError(err)
			span.SetStatus(otelcodes.Error, "invalid partition")
			return nil, err
		}
		partition = int(req.Partition)
	} else {
		partition = s.broker.RouteKey(req.Topic, req.Key)
	}
	if s.wal != nil {
		if err := s.wal.AppendRecord(wal.Record{
			Timestamp: time.Now().UnixNano(),
			Topic:     req.Topic,
			Key:       req.Key,
			Partition: int32(partition),
			Payload:   req.Payload,
		}); err != nil {
			span.RecordError(err)
			span.SetStatus(otelcodes.Error, "wal append failed")
			return nil, status.Errorf(codes.Internal, "wal append failed: %v", err)
		}
	}
	offset, err := s.broker.PublishToPartition(req.Topic, partition, req.Payload)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "publish failed")
		return nil, status.Errorf(codes.Internal, "publish failed: %v", err)
	}
	if s.metrics != nil {
		s.metrics.PublishedTotal.Inc()
		s.metrics.ObservePublishLatency(start)
		s.metrics.ObserveAgentPayload(req.Topic, req.Payload)
	}
	span.SetAttributes(
		attribute.Int("partition", partition),
		attribute.Int64("offset", offset),
	)
	span.SetStatus(otelcodes.Ok, "ok")
	return &goqueuev1.PublishResponse{Offset: offset, Partition: int32(partition)}, nil
}

func (s *Server) Consume(req *goqueuev1.ConsumeRequest, stream grpc.ServerStreamingServer[goqueuev1.ConsumeMessage]) error {
	ctx := stream.Context()
	ctx, span := otel.Tracer("goqueue.grpcapi").Start(ctx, "BrokerService.Consume")
	defer span.End()
	span.SetAttributes(
		attribute.String("topic", req.Topic),
		attribute.String("group", req.Group),
		attribute.Int64("requested_partition", int64(req.Partition)),
	)

	if req.Topic == "" {
		span.SetStatus(otelcodes.Error, "topic required")
		return status.Error(codes.InvalidArgument, "topic is required")
	}
	group := req.Group
	if group == "" {
		group = "default"
	}

	partition := int(req.Partition)
	var sub *broker.Subscription
	if partition < 0 {
		sub = s.broker.SubscribeGroupAt(req.Topic, group, -1)
		if committed, ok := s.groups.GetPartition(req.Topic, group, sub.Partition()); ok {
			sub.Commit(committed)
		}
	} else {
		startOffset := int64(-1)
		if committed, ok := s.groups.GetPartition(req.Topic, group, partition); ok {
			startOffset = committed
		}
		sub = s.broker.SubscribePartitionAt(req.Topic, group, partition, startOffset)
	}
	defer s.broker.Unsubscribe(sub)

	for {
		msgs, err := sub.Next(stream.Context(), 128)
		if err != nil {
			if err == context.Canceled || err == io.EOF {
				span.SetStatus(otelcodes.Ok, "client closed stream")
				return nil
			}
			span.RecordError(err)
			span.SetStatus(otelcodes.Error, "consume loop failed")
			return err
		}
		span.AddEvent("consume.batch", trace.WithAttributes(attribute.Int("batch_size", len(msgs))))
		for _, msg := range msgs {
			out := &goqueuev1.ConsumeMessage{
				Offset:            msg.Offset,
				Payload:           msg.Payload,
				TimestampUnixNano: msg.Timestamp.UnixNano(),
				Partition:         int32(sub.Partition()),
			}
			if err := stream.Send(out); err != nil {
				span.RecordError(err)
				span.SetStatus(otelcodes.Error, "stream send failed")
				return err
			}
		}
		latestOffset := msgs[len(msgs)-1].Offset + 1
		s.groups.CommitPartition(req.Topic, group, sub.Partition(), latestOffset)
		s.broker.AddConsumed(int64(len(msgs)))
		if s.metrics != nil {
			s.metrics.ConsumedTotal.Add(float64(len(msgs)))
			head, tail, err := s.broker.TopicPartitionInfo(req.Topic, sub.Partition())
			if err == nil {
				lag := tail - latestOffset
				if latestOffset < head {
					lag = tail - head
				}
				s.metrics.ConsumerLag.WithLabelValues(req.Topic, group).Set(float64(lag))
			}
		}
		span.SetAttributes(attribute.Int("partition", sub.Partition()))
	}
}

// Fetch returns a single page of historical messages from (topic, partition)
// starting at from_offset. Unlike Consume, it doesn't touch consumer-group
// state and doesn't stream — callers paginate by passing the response's
// next_offset on the next call. Powers session replay and time-travel reads.
func (s *Server) Fetch(ctx context.Context, req *goqueuev1.FetchRequest) (*goqueuev1.FetchResponse, error) {
	_, span := otel.Tracer("goqueue.grpcapi").Start(ctx, "BrokerService.Fetch")
	defer span.End()
	span.SetAttributes(
		attribute.String("topic", req.Topic),
		attribute.Int64("partition", int64(req.Partition)),
		attribute.Int64("from_offset", req.FromOffset),
		attribute.Int64("max_count", int64(req.MaxCount)),
	)

	if req.Topic == "" {
		span.SetStatus(otelcodes.Error, "topic required")
		return nil, status.Error(codes.InvalidArgument, "topic is required")
	}
	if req.Partition < 0 {
		span.SetStatus(otelcodes.Error, "partition required")
		return nil, status.Error(codes.InvalidArgument, "partition must be >= 0")
	}
	if int(req.Partition) >= s.broker.PartitionCount(req.Topic) {
		// Topic may not exist yet — return empty rather than error so callers
		// can poll safely.
		head, tail, err := s.broker.TopicPartitionInfo(req.Topic, int(req.Partition))
		if err != nil {
			return &goqueuev1.FetchResponse{NextOffset: req.FromOffset}, nil
		}
		return &goqueuev1.FetchResponse{NextOffset: req.FromOffset, Head: head, Tail: tail}, nil
	}

	maxCount := int(req.MaxCount)
	if maxCount <= 0 {
		maxCount = 256
	}
	if maxCount > 4096 {
		maxCount = 4096
	}

	msgs := s.broker.FetchPartition(req.Topic, int(req.Partition), req.FromOffset, maxCount)
	head, tail, _ := s.broker.TopicPartitionInfo(req.Topic, int(req.Partition))

	out := make([]*goqueuev1.ConsumeMessage, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, &goqueuev1.ConsumeMessage{
			Offset:            m.Offset,
			Payload:           m.Payload,
			TimestampUnixNano: m.Timestamp.UnixNano(),
			Partition:         req.Partition,
		})
	}

	nextOffset := req.FromOffset + int64(len(msgs))
	span.SetAttributes(attribute.Int("returned", len(msgs)))
	span.SetStatus(otelcodes.Ok, "ok")
	return &goqueuev1.FetchResponse{
		Messages:   out,
		NextOffset: nextOffset,
		Head:       head,
		Tail:       tail,
	}, nil
}

func Register(grpcServer *grpc.Server, srv *Server) {
	goqueuev1.RegisterBrokerServiceServer(grpcServer, srv)
}
