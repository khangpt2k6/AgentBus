package agentbus

import (
	"context"
	"errors"
	"fmt"
	"time"

	pb "github.com/khangpt2k6/AgentBus/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Client is a gRPC client to an AgentBus broker. Safe for concurrent use.
//
// A single Client multiplexes all requests over one HTTP/2 connection; you
// rarely need to create more than one per process.
type Client struct {
	conn *grpc.ClientConn
	api  pb.BrokerServiceClient
}

type config struct {
	creds       credentials.TransportCredentials
	dialTimeout time.Duration
	dialOpts    []grpc.DialOption
}

// Option configures a Client at Connect time.
type Option func(*config)

// WithTLS enables TLS using the given transport credentials. Use
// google.golang.org/grpc/credentials.NewTLS / NewClientTLSFromFile to build
// one. Without this option, the client connects in plaintext (suitable for
// loopback / private networks; do NOT use over the public internet).
func WithTLS(creds credentials.TransportCredentials) Option {
	return func(c *config) { c.creds = creds }
}

// WithDialTimeout sets the maximum time spent establishing the initial
// connection. Default: 5 seconds.
func WithDialTimeout(d time.Duration) Option {
	return func(c *config) { c.dialTimeout = d }
}

// WithDialOption forwards a raw grpc.DialOption (for advanced users who need
// to set keepalive, retry policy, interceptors, etc.).
func WithDialOption(o grpc.DialOption) Option {
	return func(c *config) { c.dialOpts = append(c.dialOpts, o) }
}

// Connect dials an AgentBus broker at addr (host:port) and returns a ready
// Client. Defaults to plaintext gRPC with a 5-second dial timeout; pass
// WithTLS for production.
func Connect(ctx context.Context, addr string, opts ...Option) (*Client, error) {
	cfg := &config{
		creds:       insecure.NewCredentials(),
		dialTimeout: 5 * time.Second,
	}
	for _, o := range opts {
		o(cfg)
	}

	dialCtx, cancel := context.WithTimeout(ctx, cfg.dialTimeout)
	defer cancel()

	dialOpts := append(
		[]grpc.DialOption{grpc.WithTransportCredentials(cfg.creds)},
		cfg.dialOpts...,
	)
	conn, err := grpc.DialContext(dialCtx, addr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("agentbus: dial %s: %w", addr, err)
	}
	return &Client{
		conn: conn,
		api:  pb.NewBrokerServiceClient(conn),
	}, nil
}

// Close releases the underlying gRPC connection. Subsequent calls on the
// Client will fail. Safe to call multiple times.
func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

// ErrSubscriptionClosed is returned by Subscription.Next when the stream
// has been canceled or closed cleanly by either side.
var ErrSubscriptionClosed = errors.New("agentbus: subscription closed")
