// Package api defines the JSON types shared between the broker HTTP API
// and the WASM frontend. Both import this package directly — no type drift.
package api

// BrokerStats is the response shape for GET /api/stats.
type BrokerStats struct {
	NodeID         string      `json:"node_id"`
	Role           string      `json:"role"`
	LeaderID       string      `json:"leader_id"`
	Term           int64       `json:"term"`
	Uptime         string      `json:"uptime"`
	TotalPublished int64       `json:"total_published"`
	TotalConsumed  int64       `json:"total_consumed"`
	TCPConnections int64       `json:"tcp_connections"`
	Topics         []TopicStat `json:"topics"`
	WAL            WALInfo     `json:"wal"`
}

type TopicStat struct {
	Name       string          `json:"name"`
	Partitions []PartitionStat `json:"partitions"`
	Total      int64           `json:"total"`
}

type PartitionStat struct {
	Index     int     `json:"index"`
	Head      int64   `json:"head"`
	Tail      int64   `json:"tail"`
	Size      int64   `json:"size"`
	Capacity  int64   `json:"capacity"`
	FillPct   float64 `json:"fill_pct"`
	Evictions int64   `json:"evictions"`
}

type WALInfo struct {
	Path     string `json:"path"`
	SyncMode string `json:"sync_mode"`
}

// PublishRequest is the body for POST /api/publish.
type PublishRequest struct {
	Topic     string `json:"topic"`
	Key       string `json:"key,omitempty"`
	Partition int    `json:"partition,omitempty"` // -1 or 0 = auto
	Payload   string `json:"payload"`
}

// PublishResponse is returned after a successful publish.
type PublishResponse struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

// FetchedMessage is one message returned by GET /api/fetch.
type FetchedMessage struct {
	Offset    int64  `json:"offset"`
	Timestamp string `json:"timestamp"` // RFC3339Nano
	Payload   string `json:"payload"`
}
