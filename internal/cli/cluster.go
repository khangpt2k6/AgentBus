package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/spf13/cobra"
)

type clusterStatusResponse struct {
	NodeID   string `json:"node_id"`
	Role     string `json:"role"`
	LeaderID string `json:"leader_id"`
	Term     int64  `json:"term"`
	Uptime   string `json:"uptime"`
}

func newClusterCmd(_ *options) *cobra.Command {
	c := &cobra.Command{
		Use:   "cluster",
		Short: "Inspect AgentBus cluster state",
	}
	c.AddCommand(newClusterStatusCmd())
	c.AddCommand(newClusterRouteCmd())
	return c
}

func newClusterStatusCmd() *cobra.Command {
	var metricsURL string
	c := &cobra.Command{
		Use:   "status",
		Short: "Print the current node's view of cluster state",
		RunE: func(_ *cobra.Command, _ []string) error {
			cli := &http.Client{Timeout: 3 * time.Second}
			resp, err := cli.Get(metricsURL + "/api/stats")
			if err != nil {
				return fmt.Errorf("fetch stats: %w", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
			}
			var out clusterStatusResponse
			if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
				return fmt.Errorf("decode: %w", err)
			}
			fmt.Printf("Node:    %s\n", out.NodeID)
			fmt.Printf("Role:    %s\n", out.Role)
			fmt.Printf("Leader:  %s\n", out.LeaderID)
			fmt.Printf("Term:    %d\n", out.Term)
			fmt.Printf("Uptime:  %s\n", out.Uptime)
			return nil
		},
	}
	c.Flags().StringVar(&metricsURL, "metrics-url", "http://localhost:2112", "broker metrics/admin URL")
	return c
}
