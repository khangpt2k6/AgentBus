package controlplane

import "sync"

// RoutedEvent is an agent event the control plane delivers to a target agent's
// inbox (currently handoffs).
type RoutedEvent struct {
	SessionKey string
	Type       string
	Payload    []byte
}

// Inboxes holds a bounded, non-blocking delivery queue per agent. An agent opens
// its inbox (via OpenInbox on the service) and the handoff router delivers events
// addressed to it. Delivery never blocks the ingestion loop: if an inbox is full
// or unopened, Deliver returns false and the caller records it as unrouted.
type Inboxes struct {
	mu    sync.Mutex
	boxes map[string]chan RoutedEvent
	size  int
}

// NewInboxes returns an inbox set where each agent's queue buffers size events.
func NewInboxes(size int) *Inboxes {
	if size <= 0 {
		size = 1
	}
	return &Inboxes{boxes: make(map[string]chan RoutedEvent), size: size}
}

// Open returns the receive channel for an agent's inbox, creating it on first
// call. Repeated calls for the same agent return the same channel.
func (i *Inboxes) Open(agentID string) <-chan RoutedEvent {
	i.mu.Lock()
	defer i.mu.Unlock()
	ch := i.boxes[agentID]
	if ch == nil {
		ch = make(chan RoutedEvent, i.size)
		i.boxes[agentID] = ch
	}
	return ch
}

// Deliver enqueues an event onto an agent's inbox without blocking. It returns
// false if the agent has no open inbox or its queue is full.
func (i *Inboxes) Deliver(agentID string, ev RoutedEvent) bool {
	i.mu.Lock()
	ch := i.boxes[agentID]
	i.mu.Unlock()
	if ch == nil {
		return false
	}
	select {
	case ch <- ev:
		return true
	default:
		return false
	}
}

// Close removes and closes an agent's inbox.
func (i *Inboxes) Close(agentID string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if ch := i.boxes[agentID]; ch != nil {
		delete(i.boxes, agentID)
		close(ch)
	}
}
