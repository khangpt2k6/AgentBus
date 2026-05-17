package metadata

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// Peer is the bootstrap-time description of a cluster member.
type Peer struct {
	NodeID string
	Addr   string
}

// Options bundles every knob the Raft wrapper needs.
type Options struct {
	NodeID        string
	BindAddr      string    // listen address for the Raft transport
	AdvertiseAddr string    // address peers should dial to reach this node
	DataDir       string    // where bolt logs + snapshots live
	Bootstrap     bool      // call BootstrapCluster on first start
	InitialPeers  []Peer    // server list when Bootstrap is true
	LogOutput     io.Writer // optional; defaults to os.Stderr
}

// Metadata is the running handle to a Raft node.
type Metadata struct {
	raft        *raft.Raft
	fsm         *FSM
	tx          raft.Transport
	logStore    *raftboltdb.BoltStore
	stableStore *raftboltdb.BoltStore
}

// Start brings up a Raft node. If opts.Bootstrap is true and the on-disk
// state is fresh, BootstrapCluster is called with opts.InitialPeers.
func Start(opts Options) (*Metadata, error) {
	if opts.NodeID == "" {
		return nil, fmt.Errorf("NodeID required")
	}
	if opts.BindAddr == "" {
		return nil, fmt.Errorf("BindAddr required")
	}
	if opts.AdvertiseAddr == "" {
		opts.AdvertiseAddr = opts.BindAddr
	}
	if opts.DataDir == "" {
		return nil, fmt.Errorf("DataDir required")
	}
	if opts.LogOutput == nil {
		opts.LogOutput = os.Stderr
	}
	if err := os.MkdirAll(opts.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir DataDir: %w", err)
	}

	fsm := NewFSM()

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(opts.DataDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("bolt log store: %w", err)
	}
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(opts.DataDir, "raft-stable.db"))
	if err != nil {
		return nil, fmt.Errorf("bolt stable store: %w", err)
	}
	snapStore, err := raft.NewFileSnapshotStore(opts.DataDir, 2, opts.LogOutput)
	if err != nil {
		return nil, fmt.Errorf("file snapshot store: %w", err)
	}

	advAddr, err := net.ResolveTCPAddr("tcp", opts.AdvertiseAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve advertise: %w", err)
	}

	logger := hclog.New(&hclog.LoggerOptions{
		Output: opts.LogOutput,
		Level:  hclog.Warn, // keep test output tidy
	})

	tx, err := raft.NewTCPTransportWithLogger(
		opts.BindAddr,
		advAddr,
		3,
		10*time.Second,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("tcp transport: %w", err)
	}

	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(opts.NodeID)
	cfg.Logger = logger
	// Snappier election timings for local-cluster demos; production users
	// can override via env later.
	cfg.HeartbeatTimeout = 500 * time.Millisecond
	cfg.ElectionTimeout = 500 * time.Millisecond
	cfg.LeaderLeaseTimeout = 250 * time.Millisecond
	cfg.CommitTimeout = 50 * time.Millisecond

	r, err := raft.NewRaft(cfg, fsm, logStore, stableStore, snapStore, tx)
	if err != nil {
		return nil, fmt.Errorf("raft new: %w", err)
	}

	if opts.Bootstrap {
		servers := make([]raft.Server, 0, len(opts.InitialPeers))
		for _, p := range opts.InitialPeers {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(p.NodeID),
				Address: raft.ServerAddress(p.Addr),
			})
		}
		fut := r.BootstrapCluster(raft.Configuration{Servers: servers})
		if err := fut.Error(); err != nil && err != raft.ErrCantBootstrap {
			return nil, fmt.Errorf("bootstrap: %w", err)
		}
	}

	return &Metadata{raft: r, fsm: fsm, tx: tx, logStore: logStore, stableStore: stableStore}, nil
}

// FSM returns the underlying state machine for read-only inspection.
func (m *Metadata) FSM() *FSM { return m.fsm }

// IsLeader reports whether this node is currently the Raft leader.
func (m *Metadata) IsLeader() bool {
	return m.raft.State() == raft.Leader
}

// Leader returns the current leader's transport address, or "" if unknown.
func (m *Metadata) Leader() string {
	addr, _ := m.raft.LeaderWithID()
	return string(addr)
}

// LeaderCh forwards Raft's leadership notification channel.
// Receivers get `true` when this node becomes leader, `false` when it loses.
func (m *Metadata) LeaderCh() <-chan bool { return m.raft.LeaderCh() }

// Apply submits a serialized Command to the Raft log. Returns once it has
// been committed and applied by the FSM, or an error.
func (m *Metadata) Apply(cmd []byte, timeout time.Duration) error {
	fut := m.raft.Apply(cmd, timeout)
	if err := fut.Error(); err != nil {
		return err
	}
	if resp := fut.Response(); resp != nil {
		if e, ok := resp.(error); ok {
			return e
		}
	}
	return nil
}

// Shutdown stops Raft and closes BoltDB stores. Safe to call once.
func (m *Metadata) Shutdown() error {
	fut := m.raft.Shutdown()
	if err := fut.Error(); err != nil {
		return err
	}
	if err := m.logStore.Close(); err != nil {
		return fmt.Errorf("close log store: %w", err)
	}
	if err := m.stableStore.Close(); err != nil {
		return fmt.Errorf("close stable store: %w", err)
	}
	return nil
}
