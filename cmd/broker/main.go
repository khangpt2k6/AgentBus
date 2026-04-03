package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"

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
	if err := wal.Replay(*walPath, func(rec wal.Record) error {
		b.Publish(rec.Topic, rec.Payload)
		return nil
	}); err != nil {
		log.Fatalf("replay wal: %v", err)
	}

	logFile, err := wal.Open(*walPath)
	if err != nil {
		log.Fatalf("open wal: %v", err)
	}
	defer logFile.Close()

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	groups := consumer.NewManager()
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

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.Handler(reg))
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
		log.Printf("metrics listening on %s", *metricsAddr)
		if err := http.ListenAndServe(*metricsAddr, mux); err != nil {
			log.Fatalf("metrics server stopped: %v", err)
		}
	}()

	go func() {
		lis, err := net.Listen("tcp", *grpcAddr)
		if err != nil {
			log.Fatalf("grpc listen: %v", err)
		}
		grpcSrv := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
		grpcapi.Register(grpcSrv, grpcapi.NewServer(b, groups, m, logFile))
		log.Printf("grpc broker listening on %s", *grpcAddr)
		if err := grpcSrv.Serve(lis); err != nil {
			log.Fatalf("grpc server stopped: %v", err)
		}
	}()

	srv := broker.NewTCPServer(*tcpAddr, b, logFile, groups, m)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("tcp server stopped: %v", err)
	}
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
