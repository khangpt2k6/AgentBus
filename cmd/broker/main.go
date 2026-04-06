package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/2006t/goqueue/internal/api"
	"github.com/2006t/goqueue/internal/broker"
	"github.com/2006t/goqueue/internal/consumer"
	"github.com/2006t/goqueue/internal/grpcapi"
	"github.com/2006t/goqueue/internal/metrics"
	"github.com/2006t/goqueue/internal/telemetry"
	"github.com/2006t/goqueue/internal/wal"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

func main() {
	tcpAddr := flag.String("tcp-addr", ":9090", "TCP broker listen address")
	grpcAddr := flag.String("grpc-addr", ":9095", "gRPC listen address")
	metricsAddr := flag.String("metrics-addr", ":2112", "Prometheus metrics listen address")
	walPath := flag.String("wal-path", "data/goqueue.wal", "WAL file path")
	walSyncMode := flag.String("wal-sync-mode", "none", "WAL fsync mode: none|always|interval")
	walSyncInterval := flag.Duration("wal-sync-interval", 250*time.Millisecond, "WAL fsync interval when wal-sync-mode=interval")
	walAllowPartialTail := flag.Bool("wal-allow-partial-tail", true, "allow replay to skip truncated tail records")
	nodeID := flag.String("node-id", "node-1", "node identifier for raft/dashboard labels")
	raftRole := flag.String("raft-role", "standalone", "raft role label: leader|follower|candidate|standalone")
	raftLeader := flag.String("raft-leader-id", "", "current raft leader id label")
	raftTerm := flag.Int64("raft-term", 1, "raft term value")
	adminToken := flag.String("raft-admin-token", "", "optional admin token for raft state updates")
	flag.Parse()

	if err := os.MkdirAll("data", 0o755); err != nil {
		log.Fatalf("create data dir: %v", err)
	}

	b := broker.New()
	if err := wal.ReplayWithOptions(*walPath, wal.ReplayOptions{AllowPartialTail: *walAllowPartialTail}, func(rec wal.Record) error {
		switch {
		case rec.Partition >= 0:
			_, err := b.PublishToPartition(rec.Topic, int(rec.Partition), rec.Payload)
			return err
		case rec.Key != "":
			_, _, err := b.PublishWithKey(rec.Topic, rec.Key, rec.Payload)
			return err
		default:
			b.Publish(rec.Topic, rec.Payload)
			return nil
		}
	}); err != nil {
		log.Fatalf("replay wal: %v", err)
	}

	syncMode, err := wal.ParseSyncMode(*walSyncMode)
	if err != nil {
		log.Fatalf("invalid wal-sync-mode: %v", err)
	}

	logFile, err := wal.OpenWithOptions(*walPath, wal.Options{
		SyncMode:     syncMode,
		SyncInterval: *walSyncInterval,
	})
	if err != nil {
		log.Fatalf("open wal: %v", err)
	}

	startTime := time.Now()
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	groups := consumer.NewManager()
	var tcpSrv *broker.TCPServer // declared early so /api/stats closure can capture it
	leader := *raftLeader
	if strings.TrimSpace(leader) == "" {
		leader = *nodeID
	}
	state := &raftRuntimeState{
		NodeID:   *nodeID,
		Role:     *raftRole,
		LeaderID: leader,
		Term:     *raftTerm,
	}
	m.SetRaftState(state.NodeID, state.Role, state.LeaderID, state.Term)

	traceShutdown, err := telemetry.SetupTracing(context.Background(), "goqueue-broker")
	if err != nil {
		log.Fatalf("setup tracing: %v", err)
	}
	defer func() {
		_ = traceShutdown(context.Background())
	}()

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ready := &atomic.Bool{}
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler(reg))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})
	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		cur := state.Get()
		uptime := time.Since(startTime).Round(time.Second)

		topicNames := b.Topics()
		topics := make([]api.TopicStat, 0, len(topicNames))
		for _, name := range topicNames {
			partCount := b.PartitionCount(name)
			ts := api.TopicStat{Name: name}
			for i := 0; i < partCount; i++ {
				head, tail, err := b.TopicPartitionInfo(name, i)
				if err != nil {
					continue
				}
				size := tail - head
				ts.Total += size
				ts.Partitions = append(ts.Partitions, api.PartitionStat{
					Index: i, Head: head, Tail: tail, Size: size,
				})
			}
			topics = append(topics, ts)
		}

		var connCount int64
		if tcpSrv != nil {
			connCount = tcpSrv.ConnCount()
		}

		stats := api.BrokerStats{
			NodeID:         cur.NodeID,
			Role:           cur.Role,
			LeaderID:       cur.LeaderID,
			Term:           cur.Term,
			Uptime:         formatUptime(uptime),
			TotalPublished: b.TotalPublished(),
			TotalConsumed:  b.TotalConsumed(),
			TCPConnections: connCount,
			Topics:         topics,
			WAL: api.WALInfo{
				Path:     *walPath,
				SyncMode: string(syncMode),
			},
		}
		_ = json.NewEncoder(w).Encode(stats)
	})
	mux.HandleFunc("/raft/state", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			if *adminToken != "" && r.Header.Get("X-GoQueue-Admin-Token") != *adminToken {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			var in raftStateUpdateRequest
			if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
				http.Error(w, "invalid json body", http.StatusBadRequest)
				return
			}
			prevLeader := state.Get().LeaderID
			state.Update(in)
			cur := state.Get()
			if prevLeader != cur.LeaderID {
				m.IncRaftLeaderChange(cur.NodeID)
			}
			m.SetRaftState(cur.NodeID, cur.Role, cur.LeaderID, cur.Term)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		cur := state.Get()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"node_id":   cur.NodeID,
			"role":      cur.Role,
			"leader_id": cur.LeaderID,
			"term":      cur.Term,
		})
	})
	mux.HandleFunc("/api/publish", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req api.PublishRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
			return
		}
		if req.Topic == "" {
			http.Error(w, "topic is required", http.StatusBadRequest)
			return
		}
		var (
			partition int
			offset    int64
			pubErr    error
		)
		payload := []byte(req.Payload)
		switch {
		case req.Partition > 0:
			offset, pubErr = b.PublishToPartition(req.Topic, req.Partition, payload)
			partition = req.Partition
		case req.Key != "":
			var p int
			p, offset, pubErr = b.PublishWithKey(req.Topic, req.Key, payload)
			partition = p
		default:
			offset = b.Publish(req.Topic, payload)
		}
		if pubErr != nil {
			http.Error(w, pubErr.Error(), http.StatusInternalServerError)
			return
		}
		if logFile != nil {
			_ = logFile.AppendRecord(wal.Record{
				Timestamp: time.Now().UnixNano(),
				Topic:     req.Topic,
				Key:       req.Key,
				Partition: int32(partition),
				Payload:   payload,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(api.PublishResponse{
			Topic:     req.Topic,
			Partition: partition,
			Offset:    offset,
		})
	})

	mux.HandleFunc("/api/fetch", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		q := r.URL.Query()
		topic := q.Get("topic")
		if topic == "" {
			http.Error(w, "topic is required", http.StatusBadRequest)
			return
		}
		partition := 0
		if p := q.Get("partition"); p != "" {
			fmt.Sscanf(p, "%d", &partition)
		}
		var offset int64
		if o := q.Get("offset"); o != "" {
			fmt.Sscanf(o, "%d", &offset)
		}
		limit := 20
		if l := q.Get("limit"); l != "" {
			fmt.Sscanf(l, "%d", &limit)
		}
		if limit > 100 {
			limit = 100
		}
		msgs := b.FetchPartition(topic, partition, offset, limit)
		out := make([]api.FetchedMessage, 0, len(msgs))
		for _, m := range msgs {
			out = append(out, api.FetchedMessage{
				Offset:    m.Offset,
				Timestamp: m.Timestamp.Format(time.RFC3339),
				Payload:   string(m.Payload),
			})
		}
		_ = json.NewEncoder(w).Encode(out)
	})

	metricsSrv := &http.Server{
		Addr:    *metricsAddr,
		Handler: mux,
	}

	grpcLis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("grpc listen: %v", err)
	}
	grpcSrv := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
	grpcapi.Register(grpcSrv, grpcapi.NewServer(b, groups, m, logFile))

	tcpSrv = broker.NewTCPServer(*tcpAddr, b, logFile, groups, m)
	ready.Store(true)

	errCh := make(chan error, 3)
	go func() {
		log.Printf("metrics listening on %s", *metricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	go func() {
		log.Printf("grpc broker listening on %s", *grpcAddr)
		if err := grpcSrv.Serve(grpcLis); err != nil {
			errCh <- err
		}
	}()
	go func() {
		if err := tcpSrv.ListenAndServe(); err != nil {
			errCh <- err
		}
	}()

	select {
	case <-rootCtx.Done():
		log.Printf("shutdown signal received")
	case err := <-errCh:
		log.Printf("server error: %v", err)
	}

	ready.Store(false)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_ = metricsSrv.Shutdown(shutdownCtx)
	grpcStopCh := make(chan struct{})
	go func() {
		grpcSrv.GracefulStop()
		close(grpcStopCh)
	}()
	select {
	case <-grpcStopCh:
	case <-time.After(5 * time.Second):
		grpcSrv.Stop()
	}
	tcpSrv.Shutdown()
	if err := logFile.Close(); err != nil {
		log.Printf("wal close error: %v", err)
	}
}

func formatUptime(d time.Duration) string {
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60
	if h > 0 {
		return fmt.Sprintf("%dh %dm %ds", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm %ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}

type raftRuntimeState struct {
	mu sync.RWMutex

	NodeID   string
	Role     string
	LeaderID string
	Term     int64
}

type raftStateUpdateRequest struct {
	Role     string `json:"role"`
	LeaderID string `json:"leader_id"`
	Term     int64  `json:"term"`
}

func (s *raftRuntimeState) Get() raftRuntimeState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return raftRuntimeState{
		NodeID:   s.NodeID,
		Role:     s.Role,
		LeaderID: s.LeaderID,
		Term:     s.Term,
	}
}

func (s *raftRuntimeState) Update(in raftStateUpdateRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if strings.TrimSpace(in.Role) != "" {
		s.Role = in.Role
	}
	if strings.TrimSpace(in.LeaderID) != "" {
		s.LeaderID = in.LeaderID
	}
	if in.Term > 0 {
		s.Term = in.Term
	}
}
