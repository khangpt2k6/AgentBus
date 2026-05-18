package grpcapi

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"time"

	"github.com/khangpt2k6/AgentBus/internal/agentstream"
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

// envelopePeek is the minimal subset of the agent envelope used for
// server-side session filtering. Keeping it tiny avoids the cost of fully
// decoding the payload on every scanned message.
type envelopePeek struct {
	Tenant    string `json:"tenant"`
	Project   string `json:"project"`
	SessionID string `json:"session_id"`
}

// newSpanID generates a fresh 8-byte OTEL SpanID. Used when we synthesize a
// parent SpanContext (session-derived trace) so child spans get a unique id.
func newSpanID() trace.SpanID {
	var id trace.SpanID
	_, _ = rand.Read(id[:])
	if !id.IsValid() {
		id[7] = 1
	}
	return id
}

func sessionFilterMatches(payload []byte, filter *goqueuev1.SessionFilter) bool {
	if filter == nil {
		return true
	}
	if filter.Tenant == "" && filter.Project == "" && filter.SessionId == "" {
		return true
	}
	var p envelopePeek
	if err := json.Unmarshal(payload, &p); err != nil {
		return false
	}
	if filter.Tenant != "" && filter.Tenant != p.Tenant {
		return false
	}
	if filter.Project != "" && filter.Project != p.Project {
		return false
	}
	if filter.SessionId != "" && filter.SessionId != p.SessionID {
		return false
	}
	return true
}

// RouteChecker is the minimum surface PublishAgent needs from the cluster
// router. Defined as an interface so single-node mode and tests don't
// import the cluster package.
type RouteChecker interface {
	RouteSession(tenant, project, sessionID string) (isLocal bool, shardID uint32, leaderClientAddr string)
}

// ShardWALHook is the minimum surface PublishAgent needs from shardwal:
// append a payload to a shard and (optionally) wait for quorum durability.
type ShardWALHook interface {
	Append(shardID uint32, payload []byte) (offset uint64, err error)
	WaitQuorum(ctx context.Context, shardID uint32, offset uint64) error
}

type Server struct {
	goqueuev1.UnimplementedBrokerServiceServer

	broker     *broker.Broker
	groups     *consumer.Manager
	metrics    *metrics.Metrics
	wal        *wal.Log
	routeCheck RouteChecker
	shardWAL   ShardWALHook
}

func NewServer(b *broker.Broker, g *consumer.Manager, m *metrics.Metrics, l *wal.Log) *Server {
	return &Server{broker: b, groups: g, metrics: m, wal: l}
}

// SetRouteChecker enables cluster-mode routing checks. Pass nil to disable.
func (s *Server) SetRouteChecker(rc RouteChecker) { s.routeCheck = rc }

// SetShardWALHook enables cluster-mode shard-WAL writes. Pass nil to disable.
func (s *Server) SetShardWALHook(h ShardWALHook) { s.shardWAL = h }

func (s *Server) Publish(ctx context.Context, req *goqueuev1.PublishRequest) (*goqueuev1.PublishResponse, error) {
	// Peek the envelope BEFORE starting the span so we can anchor the span
	// to the session's derived trace_id when no upstream trace is propagated.
	// Producers that already propagate OTEL context keep their own trace —
	// we only synthesize one when the caller has none, so observers can
	// still group events by session in Jaeger/Tempo.
	if ev, ok := agentstream.PeekEnvelope(req.Payload); ok {
		if !trace.SpanContextFromContext(ctx).IsValid() {
			traceID := agentstream.SessionTraceID(ev.Tenant, ev.Project, ev.SessionID)
			if traceID.IsValid() {
				ctx = trace.ContextWithSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{
					TraceID:    traceID,
					SpanID:     newSpanID(),
					TraceFlags: trace.FlagsSampled,
					Remote:     false,
				}))
			}
		}
	}
	ctx, span := otel.Tracer("goqueue.grpcapi").Start(ctx, "BrokerService.Publish")
	defer span.End()
	span.SetAttributes(
		attribute.String("topic", req.Topic),
		attribute.String("key", req.Key),
		attribute.Int("payload_bytes", len(req.Payload)),
		attribute.Int64("requested_partition", int64(req.Partition)),
	)
	// Attach agent attributes for searchability in trace backends. Done
	// after Start so the producer's request span carries them too.
	if ev, ok := agentstream.PeekEnvelope(req.Payload); ok {
		span.SetAttributes(agentstream.AttributesFor(ev)...)
	}

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

	head, tail, _ := s.broker.TopicPartitionInfo(req.Topic, int(req.Partition))

	// Fast path: no filter, return up to maxCount raw messages.
	if req.SessionFilter == nil {
		msgs := s.broker.FetchPartition(req.Topic, int(req.Partition), req.FromOffset, maxCount)
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
			Messages: out, NextOffset: nextOffset, Head: head, Tail: tail,
		}, nil
	}

	// Filtered path: scan in larger pages until we collect maxCount matches
	// or hit the scan budget. nextOffset is the offset to resume from on
	// the next page, NOT FromOffset + len(matches) — callers must use it
	// instead of trying to derive their own.
	scanLimit := int(req.ScanLimit)
	if scanLimit <= 0 {
		scanLimit = 16384
	}
	if scanLimit > 65536 {
		scanLimit = 65536
	}
	pageSize := 1024
	if pageSize > scanLimit {
		pageSize = scanLimit
	}

	out := make([]*goqueuev1.ConsumeMessage, 0, maxCount)
	offset := req.FromOffset
	scanned := 0
	for len(out) < maxCount && scanned < scanLimit && offset < tail {
		msgs := s.broker.FetchPartition(req.Topic, int(req.Partition), offset, pageSize)
		if len(msgs) == 0 {
			break
		}
		for _, m := range msgs {
			scanned++
			if !sessionFilterMatches(m.Payload, req.SessionFilter) {
				continue
			}
			out = append(out, &goqueuev1.ConsumeMessage{
				Offset:            m.Offset,
				Payload:           m.Payload,
				TimestampUnixNano: m.Timestamp.UnixNano(),
				Partition:         req.Partition,
			})
			if len(out) >= maxCount {
				break
			}
		}
		offset += int64(len(msgs))
	}
	nextOffset := offset
	span.SetAttributes(
		attribute.Int("returned", len(out)),
		attribute.Int("scanned", scanned),
		attribute.Bool("filtered", true),
	)
	span.SetStatus(otelcodes.Ok, "ok")
	return &goqueuev1.FetchResponse{
		Messages:   out,
		NextOffset: nextOffset,
		Head:       head,
		Tail:       tail,
	}, nil
}

// PublishAgent publishes a structured agent event. In cluster mode, if this
// node does not own the target session's shard, it returns
// codes.FailedPrecondition with a NotLeaderError status detail so the client
// can redirect to the current shard leader.
func (s *Server) PublishAgent(ctx context.Context, req *goqueuev1.PublishAgentRequest) (*goqueuev1.PublishAgentResponse, error) {
	var shardID uint32
	if s.routeCheck != nil {
		tenant := req.GetEvent().GetTenant()
		project := req.GetEvent().GetProject()
		session := req.GetEvent().GetSessionId()
		if tenant != "" && project != "" && session != "" {
			isLocal, sid, hint := s.routeCheck.RouteSession(tenant, project, session)
			if !isLocal {
				st := status.New(codes.FailedPrecondition, "not the leader of this session's shard")
				withDetails, derr := st.WithDetails(&goqueuev1.NotLeaderError{LeaderAddr: hint})
				if derr == nil {
					st = withDetails
				}
				return nil, st.Err()
			}
			shardID = sid
		}
	}

	ev := req.GetEvent()
	if ev == nil {
		return nil, status.Error(codes.InvalidArgument, "event is required")
	}
	env := agentstream.Event{
		Type:      ev.GetType(),
		Tenant:    ev.GetTenant(),
		Project:   ev.GetProject(),
		SessionID: ev.GetSessionId(),
		AgentID:   ev.GetAgentId(),
		Payload:   ev.GetPayload(),
	}
	encoded, err := env.Marshal()
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid agent event: %v", err)
	}
	key := agentstream.SessionKey(ev.GetTenant(), ev.GetProject(), ev.GetSessionId())
	topic := "agent-events"
	partition := s.broker.RouteKey(topic, key)
	// Cluster-mode local path: append to shardwal first, then wait for
	// quorum. The broker.Broker write below stays as-is so subscribers still
	// see the event in-memory.
	if s.shardWAL != nil {
		walOffset, err := s.shardWAL.Append(shardID, encoded)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "shardwal append: %v", err)
		}
		waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := s.shardWAL.WaitQuorum(waitCtx, shardID, walOffset); err != nil {
			return nil, status.Errorf(codes.DeadlineExceeded, "quorum not reached: %v", err)
		}
	}
	if s.wal != nil {
		if err := s.wal.AppendRecord(wal.Record{
			Timestamp: time.Now().UnixNano(),
			Topic:     topic,
			Key:       key,
			Partition: int32(partition),
			Payload:   encoded,
		}); err != nil {
			return nil, status.Errorf(codes.Internal, "wal append failed: %v", err)
		}
	}
	offset, err := s.broker.PublishToPartition(topic, partition, encoded)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "publish failed: %v", err)
	}
	if s.metrics != nil {
		s.metrics.PublishedTotal.Inc()
		s.metrics.ObserveAgentPayload(topic, encoded)
	}
	return &goqueuev1.PublishAgentResponse{Offset: offset, Partition: int32(partition)}, nil
}

func Register(grpcServer *grpc.Server, srv *Server) {
	goqueuev1.RegisterBrokerServiceServer(grpcServer, srv)
}
