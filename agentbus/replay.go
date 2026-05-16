package agentbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"time"

	pb "github.com/khangpt2k6/AgentBus/proto"
)

// DecodedEvent is one envelope read back from the bus, with the AgentBus
// metadata parsed out of the JSON wrapper. Payload remains as raw JSON so
// the caller can decode into their own typed struct.
type DecodedEvent struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time

	// Envelope fields, populated from the JSON wrapper.
	Version   string
	Type      string
	Tenant    string
	Project   string
	SessionID string
	AgentID   string
	Step      string
	Attempt   int
	CreatedAt time.Time

	// Payload is the raw bytes inside the envelope (the user-provided body
	// of the original PublishAgent call). Usually JSON.
	Payload json.RawMessage
}

// DecodeEvent parses an envelope-style payload (as produced by PublishAgent)
// from raw bytes. Use when consuming via Subscribe and you want the struct
// form. Returns false if the bytes are not a valid envelope.
func DecodeEvent(raw []byte) (DecodedEvent, bool) {
	if !json.Valid(raw) {
		return DecodedEvent{}, false
	}
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return DecodedEvent{}, false
	}
	if env.Type == "" || env.Tenant == "" || env.SessionID == "" {
		return DecodedEvent{}, false
	}
	ts, _ := time.Parse(time.RFC3339Nano, env.CreatedAt)
	return DecodedEvent{
		Version:   env.Version,
		Type:      env.Type,
		Tenant:    env.Tenant,
		Project:   env.Project,
		SessionID: env.SessionID,
		AgentID:   env.AgentID,
		Step:      env.Step,
		Attempt:   env.Attempt,
		CreatedAt: ts,
		Payload:   env.Payload,
	}, true
}

// ReplayOptions tunes a session-replay scan.
type ReplayOptions struct {
	// Topic to scan. Defaults to DefaultAgentTopic ("agent-events").
	Topic string
	// PartitionCount is the topic's partition count. Must match the broker's
	// configuration for session routing to land on the right partition.
	// Defaults to 3 (broker's defaultTopicPartitions).
	PartitionCount int
	// FromOffset starts the scan at this offset on the session's partition.
	// Default 0 reads from the oldest retained message.
	FromOffset int64
	// PageSize is the per-RPC batch size. Default 256, capped at 4096 by the server.
	PageSize int32
	// MaxEvents caps the total events returned. 0 = unlimited (until tail).
	MaxEvents int
}

// ReplaySession returns every persisted event for the given session, in the
// order they were originally written. It uses Fetch under the hood (so it
// doesn't disturb consumer-group state) and filters envelope-by-envelope.
//
// Cost: O(messages on the session's partition since FromOffset). For busy
// topics, prefer running this against a quiet replica or with a tighter
// FromOffset.
func (c *Client) ReplaySession(ctx context.Context, sess SessionRef, opts ReplayOptions) ([]DecodedEvent, error) {
	topic, partition, err := c.resolveSessionRouting(sess, opts.Topic, opts.PartitionCount)
	if err != nil {
		return nil, err
	}
	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = 256
	}

	var out []DecodedEvent
	offset := opts.FromOffset
	if offset < 0 {
		offset = 0
	}
	for {
		page, err := c.api.Fetch(ctx, &pb.FetchRequest{
			Topic:      topic,
			Partition:  partition,
			FromOffset: offset,
			MaxCount:   pageSize,
		})
		if err != nil {
			return out, fmt.Errorf("agentbus: replay fetch: %w", err)
		}
		if len(page.Messages) == 0 {
			// Caught up to tail.
			return out, nil
		}
		for _, m := range page.Messages {
			ev, ok := DecodeEvent(m.Payload)
			if !ok {
				continue
			}
			if !sessionMatches(ev, sess) {
				continue
			}
			ev.Topic = topic
			ev.Partition = m.Partition
			ev.Offset = m.Offset
			ev.Timestamp = time.Unix(0, m.TimestampUnixNano)
			out = append(out, ev)
			if opts.MaxEvents > 0 && len(out) >= opts.MaxEvents {
				return out, nil
			}
		}
		offset = page.NextOffset
		if offset >= page.Tail {
			return out, nil
		}
	}
}

// TailSession is the live counterpart to ReplaySession: it opens a Subscribe
// stream on the session's partition and yields only events matching the
// session triple. Use it to watch an agent run as it happens.
//
// The returned Subscription must be Close()d.
func (c *Client) TailSession(ctx context.Context, sess SessionRef, opts ReplayOptions) (*FilteredSubscription, error) {
	topic, partition, err := c.resolveSessionRouting(sess, opts.Topic, opts.PartitionCount)
	if err != nil {
		return nil, err
	}
	// Use a unique group name so we don't disturb other consumers and we
	// start at the latest offset (default behaviour for fresh groups).
	group := "tail-" + strings.ReplaceAll(sess.SessionID, "/", "_")
	inner, err := c.SubscribeWithOptions(ctx, topic, group, SubscribeOptions{Partition: partition})
	if err != nil {
		return nil, err
	}
	return &FilteredSubscription{inner: inner, sess: sess}, nil
}

// FilteredSubscription wraps a Subscription and drops messages that don't
// match a session triple. Returned by TailSession.
type FilteredSubscription struct {
	inner *Subscription
	sess  SessionRef
}

// Next blocks until the next matching event arrives.
func (f *FilteredSubscription) Next(ctx context.Context) (DecodedEvent, error) {
	for {
		msg, err := f.inner.Next(ctx)
		if err != nil {
			return DecodedEvent{}, err
		}
		ev, ok := DecodeEvent(msg.Payload)
		if !ok {
			continue
		}
		if !sessionMatches(ev, f.sess) {
			continue
		}
		ev.Partition = msg.Partition
		ev.Offset = msg.Offset
		ev.Timestamp = msg.Timestamp
		return ev, nil
	}
}

func (f *FilteredSubscription) Close() error { return f.inner.Close() }

// resolveSessionRouting picks the topic and partition the session's events
// land on. Partition selection mirrors the broker's internal logic so we
// don't have to query for it.
func (c *Client) resolveSessionRouting(sess SessionRef, topic string, partitionCount int) (string, int32, error) {
	if sess.Tenant == "" || sess.Project == "" || sess.SessionID == "" {
		return "", 0, errors.New("agentbus: SessionRef.Tenant/Project/SessionID are required")
	}
	if topic == "" {
		topic = DefaultAgentTopic
	}
	if partitionCount <= 0 {
		partitionCount = 3 // mirrors broker.defaultTopicPartitions
	}
	key := sessionKey(sess.Tenant, sess.Project, sess.SessionID)
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return topic, int32(h.Sum32() % uint32(partitionCount)), nil
}

func sessionMatches(ev DecodedEvent, sess SessionRef) bool {
	return ev.Tenant == sess.Tenant && ev.Project == sess.Project && ev.SessionID == sess.SessionID
}
