package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/khangpt2k6/AgentBus/agentbus"
	"github.com/spf13/cobra"
)

// newWebhookCmd lets non-Go (or any) consumer integrate via HTTP: subscribe
// to a topic and POST every event to a URL.
//
// Each POST body is the event payload (the JSON envelope, when the event
// was published via PublishAgent). Headers carry routing metadata.
//
// 2xx responses are treated as success and offsets advance. On non-2xx,
// the subscriber retries with exponential backoff up to --max-attempts;
// if still failing, the event is logged and the loop continues (the
// consumer offset still advances to avoid wedging).
func newWebhookCmd(opts *options) *cobra.Command {
	var (
		topic       string
		group       string
		url         string
		partition   int
		timeout     time.Duration
		maxAttempts int
		baseBackoff time.Duration
		header      []string
	)
	cmd := &cobra.Command{
		Use:   "webhook",
		Short: "Subscribe to a topic and POST every event to a URL",
		Long: `Run AgentBus as the producer side of an HTTP webhook pipeline. Useful for
integrating non-Go consumers (Slack notifiers, serverless functions, third-party
SaaS), without forcing them to speak gRPC.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if !opts.grpc {
				return errors.New("webhook requires --grpc")
			}
			if url == "" {
				return errors.New("--url is required")
			}
			extraHeaders, err := parseHeaders(header)
			if err != nil {
				return err
			}

			ctx := cmd.Context()
			client, err := agentbus.Connect(ctx, opts.addr)
			if err != nil {
				return err
			}
			defer client.Close()

			sub, err := client.SubscribeWithOptions(ctx, topic, group, agentbus.SubscribeOptions{
				Partition: int32(partition),
			})
			if err != nil {
				return err
			}
			defer sub.Close()

			httpClient := &http.Client{Timeout: timeout}
			fmt.Fprintf(os.Stderr, "webhook: %s [group=%s] -> %s\n", topic, group, url)

			for {
				msg, err := sub.Next(ctx)
				if err != nil {
					if errors.Is(err, agentbus.ErrSubscriptionClosed) || errors.Is(err, context.Canceled) {
						return nil
					}
					return err
				}
				if err := deliver(ctx, httpClient, url, msg, extraHeaders, maxAttempts, baseBackoff); err != nil {
					// Already exhausted retries — log + continue (don't wedge the
					// consumer group on a permanently-failing endpoint).
					fmt.Fprintf(os.Stderr, "webhook: dropped offset=%d after %d attempts: %v\n",
						msg.Offset, maxAttempts, err)
				}
			}
		},
	}
	cmd.Flags().StringVar(&topic, "topic", "agent-events", "topic to subscribe to")
	cmd.Flags().StringVar(&group, "group", "webhook", "consumer group (resumes from last commit)")
	cmd.Flags().StringVar(&url, "url", "", "HTTP endpoint to POST each event to (required)")
	cmd.Flags().IntVar(&partition, "partition", -1, "partition to subscribe to (-1 = group hash)")
	cmd.Flags().DurationVar(&timeout, "timeout", 10*time.Second, "per-request HTTP timeout")
	cmd.Flags().IntVar(&maxAttempts, "max-attempts", 5, "max retries per event (0 = drop on first failure)")
	cmd.Flags().DurationVar(&baseBackoff, "backoff", 500*time.Millisecond, "initial backoff between retries (exponential)")
	cmd.Flags().StringArrayVar(&header, "header", nil, "extra header on every request, e.g. 'Authorization: Bearer xyz' (repeatable)")
	_ = cmd.MarkFlagRequired("url")
	return cmd
}

func parseHeaders(raw []string) (http.Header, error) {
	h := http.Header{}
	for _, item := range raw {
		i := -1
		for k, c := range item {
			if c == ':' {
				i = k
				break
			}
		}
		if i <= 0 || i == len(item)-1 {
			return nil, fmt.Errorf("invalid --header %q (expected 'Key: Value')", item)
		}
		key := item[:i]
		val := item[i+1:]
		for len(val) > 0 && (val[0] == ' ' || val[0] == '\t') {
			val = val[1:]
		}
		h.Add(key, val)
	}
	return h, nil
}

func deliver(ctx context.Context, hc *http.Client, url string, msg agentbus.Message, extra http.Header, maxAttempts int, base time.Duration) error {
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	var lastErr error
	backoff := base
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(msg.Payload))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Agentbus-Offset", strconv.FormatInt(msg.Offset, 10))
		req.Header.Set("X-Agentbus-Partition", strconv.FormatInt(int64(msg.Partition), 10))
		req.Header.Set("X-Agentbus-Timestamp", msg.Timestamp.UTC().Format(time.RFC3339Nano))
		req.Header.Set("X-Agentbus-Attempt", strconv.Itoa(attempt))
		// Tag with envelope fields if the payload is an agent event — lets
		// the consumer route without re-parsing.
		if ev, ok := agentbus.DecodeEvent(msg.Payload); ok {
			req.Header.Set("X-Agentbus-Tenant", ev.Tenant)
			req.Header.Set("X-Agentbus-Project", ev.Project)
			req.Header.Set("X-Agentbus-Session", ev.SessionID)
			req.Header.Set("X-Agentbus-Type", ev.Type)
		}
		for k, vals := range extra {
			for _, v := range vals {
				req.Header.Add(k, v)
			}
		}
		resp, err := hc.Do(req)
		if err == nil {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, bytes.TrimSpace(body))
			// 4xx (except 408/429) is a permanent error — no retry.
			if resp.StatusCode >= 400 && resp.StatusCode < 500 &&
				resp.StatusCode != http.StatusRequestTimeout &&
				resp.StatusCode != http.StatusTooManyRequests {
				return lastErr
			}
		} else {
			lastErr = err
		}
		if attempt < maxAttempts {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
	return lastErr
}

