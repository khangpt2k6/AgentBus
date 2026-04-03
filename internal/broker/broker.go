package broker

import (
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
)

const defaultTopicPartitions = 3

// Broker manages all topics and routes messages between producers and consumers.
type Broker struct {
	mu     sync.RWMutex
	topics map[string]*topicSet

	totalPublished atomic.Int64
	totalConsumed  atomic.Int64
}

type topicSet struct {
	partitions []*Topic
	next       atomic.Uint64
}

func New() *Broker {
	return &Broker{
		topics: make(map[string]*topicSet),
	}
}

// Publish writes a message to the named topic, creating it if needed.
func (b *Broker) Publish(topicName string, payload []byte) int64 {
	set := b.getOrCreate(topicName)
	offset := set.partitions[0].Publish(payload)
	b.totalPublished.Add(1)
	return offset
}

// PublishWithKey publishes to a deterministic partition when key is provided.
// Empty key uses round-robin partitioning.
func (b *Broker) PublishWithKey(topicName, key string, payload []byte) (partition int, offset int64, err error) {
	set := b.getOrCreate(topicName)
	partition = choosePartition(set, key)
	offset = set.partitions[partition].Publish(payload)
	b.totalPublished.Add(1)
	return partition, offset, nil
}

func (b *Broker) PublishToPartition(topicName string, partition int, payload []byte) (int64, error) {
	set := b.getOrCreate(topicName)
	if partition < 0 || partition >= len(set.partitions) {
		return 0, fmt.Errorf("invalid partition %d", partition)
	}
	offset := set.partitions[partition].Publish(payload)
	b.totalPublished.Add(1)
	return offset, nil
}

func (b *Broker) EnsureTopic(topicName string, partitions int) {
	if partitions <= 0 {
		partitions = defaultTopicPartitions
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.topics[topicName]; ok {
		return
	}
	b.topics[topicName] = newTopicSet(topicName, partitions)
}

func (b *Broker) PartitionCount(topicName string) int {
	b.mu.RLock()
	set, ok := b.topics[topicName]
	b.mu.RUnlock()
	if !ok {
		return 0
	}
	return len(set.partitions)
}

// Subscribe returns a Subscription starting at the latest offset.
func (b *Broker) Subscribe(topicName, group string) *Subscription {
	return b.SubscribeAt(topicName, group, -1)
}

// SubscribeAt returns a Subscription starting at a specific offset.
// If startOffset < 0, it starts from the current tail (latest).
func (b *Broker) SubscribeAt(topicName, group string, startOffset int64) *Subscription {
	return b.SubscribePartitionAt(topicName, group, 0, startOffset)
}

// SubscribeGroupAt selects partition by group hash and subscribes from offset.
func (b *Broker) SubscribeGroupAt(topicName, group string, startOffset int64) *Subscription {
	set := b.getOrCreate(topicName)
	partition := choosePartition(set, group)
	return b.SubscribePartitionAt(topicName, group, partition, startOffset)
}

func (b *Broker) SubscribePartitionAt(topicName, group string, partition int, startOffset int64) *Subscription {
	set := b.getOrCreate(topicName)
	if partition < 0 || partition >= len(set.partitions) {
		partition = 0
	}
	t := set.partitions[partition]
	if startOffset < 0 {
		startOffset = t.Tail()
	}
	s := newSubscription(t, group, partition, startOffset)
	t.addSub(s)
	return s
}

// Unsubscribe removes a subscription from its topic.
func (b *Broker) Unsubscribe(s *Subscription) {
	s.topic.removeSub(s)
}

// TopicInfo returns metadata for a topic.
func (b *Broker) TopicInfo(name string) (head, tail int64, err error) {
	b.mu.RLock()
	set, ok := b.topics[name]
	b.mu.RUnlock()
	if !ok {
		return 0, 0, fmt.Errorf("topic %q not found", name)
	}
	t := set.partitions[0]
	return t.Head(), t.Tail(), nil
}

func (b *Broker) TopicPartitionInfo(name string, partition int) (head, tail int64, err error) {
	b.mu.RLock()
	set, ok := b.topics[name]
	b.mu.RUnlock()
	if !ok {
		return 0, 0, fmt.Errorf("topic %q not found", name)
	}
	if partition < 0 || partition >= len(set.partitions) {
		return 0, 0, fmt.Errorf("invalid partition %d", partition)
	}
	t := set.partitions[partition]
	return t.Head(), t.Tail(), nil
}

func (b *Broker) Fetch(topicName string, offset int64, maxCount int) []Message {
	return b.FetchPartition(topicName, 0, offset, maxCount)
}

func (b *Broker) FetchPartition(topicName string, partition int, offset int64, maxCount int) []Message {
	set := b.getOrCreate(topicName)
	if partition < 0 || partition >= len(set.partitions) {
		return nil
	}
	return set.partitions[partition].Fetch(offset, maxCount)
}

// Topics returns a snapshot of all topic names.
func (b *Broker) Topics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	names := make([]string, 0, len(b.topics))
	for k := range b.topics {
		names = append(names, k)
	}
	return names
}

func (b *Broker) TotalPublished() int64 { return b.totalPublished.Load() }
func (b *Broker) TotalConsumed() int64  { return b.totalConsumed.Load() }
func (b *Broker) AddConsumed(n int64)   { b.totalConsumed.Add(n) }

func (b *Broker) getOrCreate(name string) *topicSet {
	b.mu.RLock()
	t, ok := b.topics[name]
	b.mu.RUnlock()
	if ok {
		return t
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if t, ok = b.topics[name]; ok {
		return t
	}
	t = newTopicSet(name, defaultTopicPartitions)
	b.topics[name] = t
	return t
}

func newTopicSet(name string, partitions int) *topicSet {
	if partitions <= 0 {
		partitions = defaultTopicPartitions
	}
	out := &topicSet{
		partitions: make([]*Topic, partitions),
	}
	for i := range partitions {
		out.partitions[i] = newTopic(fmt.Sprintf("%s-%d", name, i), defaultCapacity)
	}
	return out
}

func choosePartition(set *topicSet, key string) int {
	if len(set.partitions) == 1 {
		return 0
	}
	if key == "" {
		return int((set.next.Add(1) - 1) % uint64(len(set.partitions)))
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(len(set.partitions)))
}
