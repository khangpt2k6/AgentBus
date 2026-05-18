package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/spf13/cobra"
)

type clusterRouteResponse struct {
	ClusterEnabled bool   `json:"cluster_enabled"`
	Reason         string `json:"reason,omitempty"`
	Tenant         string `json:"tenant,omitempty"`
	Project        string `json:"project,omitempty"`
	Session        string `json:"session,omitempty"`
	ShardID        uint32 `json:"shard_id"`
	LeaderNodeID   string `json:"leader_node_id"`
	LeaderClient   string `json:"leader_client"`
	IsLocal        bool   `json:"is_local"`
}

func newClusterRouteCmd() *cobra.Command {
	var metricsURL, tenant, project, session string
	c := &cobra.Command{
		Use:   "route",
		Short: "Show which shard + leader a session would route to",
		RunE: func(_ *cobra.Command, _ []string) error {
			if tenant == "" || project == "" || session == "" {
				return fmt.Errorf("--tenant, --project, and --session are all required")
			}
			cli := &http.Client{Timeout: 3 * time.Second}
			q := url.Values{}
			q.Set("tenant", tenant)
			q.Set("project", project)
			q.Set("session", session)
			resp, err := cli.Get(metricsURL + "/api/route?" + q.Encode())
			if err != nil {
				return fmt.Errorf("fetch route: %w", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
			}
			var out clusterRouteResponse
			if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
				return fmt.Errorf("decode: %w", err)
			}
			if !out.ClusterEnabled {
				fmt.Printf("Cluster: not enabled (%s)\n", out.Reason)
				return nil
			}
			fmt.Printf("Session: %s/%s/%s\n", out.Tenant, out.Project, out.Session)
			fmt.Printf("Shard:   %d\n", out.ShardID)
			if out.LeaderNodeID == "" {
				fmt.Println("Leader:  (none — shard unassigned or current leader is dead)")
			} else {
				fmt.Printf("Leader:  %s (client addr: %s)\n", out.LeaderNodeID, out.LeaderClient)
			}
			fmt.Printf("Local:   %v\n", out.IsLocal)
			return nil
		},
	}
	c.Flags().StringVar(&metricsURL, "metrics-url", "http://localhost:2112", "broker metrics/admin URL")
	c.Flags().StringVar(&tenant, "tenant", "", "tenant (required)")
	c.Flags().StringVar(&project, "project", "", "project (required)")
	c.Flags().StringVar(&session, "session", "", "session id (required)")
	return c
}
