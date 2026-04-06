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
	Total      int64           `json:"total"` // sum of all partition sizes
}

type PartitionStat struct {
	Index int   `json:"index"`
	Head  int64 `json:"head"`
	Tail  int64 `json:"tail"`
	Size  int64 `json:"size"` // tail - head = messages in ring
}

type WALInfo struct {
	Path     string `json:"path"`
	SyncMode string `json:"sync_mode"`
}
