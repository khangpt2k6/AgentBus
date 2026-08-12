package workflow

import (
	"time"

	"github.com/khangpt2k6/EventBus/internal/agentstream"
	"github.com/khangpt2k6/EventBus/internal/broker"
)

const (
	defaultPollBatch    = 512
	defaultPollInterval = 50 * time.Millisecond
)

// Poller drives runtime state ingestion: it tails every partition of the
// workflow-events topic and folds each recognized envelope into the
// coordinator's store. On startup it reads from offset 0, so replaying a
// WAL-restored topic rebuilds execution state deterministically. It is
// read-only against the broker and off the publish hot path.
type Poller struct {
	broker   *broker.Broker
	coord    *Coordinator
	topic    string
	batch    int
	interval time.Duration
	cursors  []int64
	stop     chan struct{}
	done     chan struct{}
}

// NewPoller constructs a poller for the given topic, ensuring the topic
// exists so its partition count is stable.
func NewPoller(b *broker.Broker, c *Coordinator, topic string) *Poller {
	b.EnsureTopic(topic, 0)
	n := b.PartitionCount(topic)
	return &Poller{
		broker:   b,
		coord:    c,
		topic:    topic,
		batch:    defaultPollBatch,
		interval: defaultPollInterval,
		cursors:  make([]int64, n),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// drainOnce makes a single pass over all partitions, folding any new
// events, and returns how many envelopes were ingested.
func (p *Poller) drainOnce() int {
	ingested := 0
	for part := 0; part < len(p.cursors); part++ {
		msgs := p.broker.FetchPartition(p.topic, part, p.cursors[part], p.batch)
		for _, m := range msgs {
			if ev, ok := agentstream.ParseEvent(m.Payload); ok {
				p.coord.Ingest(ev)
				ingested++
			}
			p.cursors[part] = m.Offset + 1
		}
	}
	return ingested
}

// Start runs the poll loop in a background goroutine until Stop is called.
func (p *Poller) Start() {
	go func() {
		defer close(p.done)
		t := time.NewTicker(p.interval)
		defer t.Stop()
		p.drainOnce() // rebuild state immediately on startup
		for {
			select {
			case <-p.stop:
				return
			case <-t.C:
				p.drainOnce()
			}
		}
	}()
}

// Stop halts the poll loop and waits for the goroutine to exit.
func (p *Poller) Stop() {
	close(p.stop)
	<-p.done
}
