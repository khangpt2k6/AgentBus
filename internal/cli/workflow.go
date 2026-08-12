package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/khangpt2k6/EventBus/eventbus"
	"github.com/spf13/cobra"
)

// newWorkflowCmd groups the durable workflow runtime commands. These talk
// gRPC only, so they take their own --grpc-addr instead of the root --addr
// (which defaults to the TCP broker port).
func newWorkflowCmd(_ *options) *cobra.Command {
	var grpcAddr string

	cmd := &cobra.Command{
		Use:   "workflow",
		Short: "Submit and inspect durable workflow executions",
	}
	cmd.PersistentFlags().StringVar(&grpcAddr, "grpc-addr", "localhost:9095", "broker gRPC address")

	connect := func(ctx context.Context) (*eventbus.Client, error) {
		return eventbus.Connect(ctx, grpcAddr)
	}

	var (
		tenant      string
		project     string
		workflowID  string
		taskType    string
		input       string
		maxAttempts int
		leaseTTL    time.Duration
	)

	submit := &cobra.Command{
		Use:   "submit",
		Short: "Durably enqueue a workflow execution",
		RunE: func(cmd *cobra.Command, args []string) error {
			if input != "" && !json.Valid([]byte(input)) {
				return fmt.Errorf("--input must be valid JSON")
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second)
			defer cancel()
			client, err := connect(ctx)
			if err != nil {
				return err
			}
			defer client.Close()
			already, err := client.SubmitWorkflow(ctx, eventbus.WorkflowSpec{
				Tenant:      tenant,
				Project:     project,
				WorkflowID:  workflowID,
				TaskType:    taskType,
				Input:       []byte(input),
				MaxAttempts: maxAttempts,
				LeaseTTL:    leaseTTL,
			})
			if err != nil {
				return err
			}
			if already {
				fmt.Printf("workflow %s already exists (idempotent no-op)\n", workflowID)
				return nil
			}
			fmt.Printf("submitted workflow %s (task type %s)\n", workflowID, taskType)
			return nil
		},
	}
	submit.Flags().StringVar(&tenant, "tenant", "", "tenant id")
	submit.Flags().StringVar(&project, "project", "", "project id")
	submit.Flags().StringVar(&workflowID, "id", "", "workflow execution id")
	submit.Flags().StringVar(&taskType, "task-type", "", "worker queue that executes this workflow")
	submit.Flags().StringVar(&input, "input", "", "JSON input payload")
	submit.Flags().IntVar(&maxAttempts, "max-attempts", 0, "maximum lease attempts (0 = broker default)")
	submit.Flags().DurationVar(&leaseTTL, "lease-ttl", 0, "lease duration (0 = broker default)")
	_ = submit.MarkFlagRequired("tenant")
	_ = submit.MarkFlagRequired("project")
	_ = submit.MarkFlagRequired("id")
	_ = submit.MarkFlagRequired("task-type")

	var (
		stTenant  string
		stProject string
		stID      string
	)
	status := &cobra.Command{
		Use:   "status",
		Short: "Show the current state of one execution",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second)
			defer cancel()
			client, err := connect(ctx)
			if err != nil {
				return err
			}
			defer client.Close()
			x, found, err := client.GetExecution(ctx, stTenant, stProject, stID)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("execution %s/%s/%s not found", stTenant, stProject, stID)
			}
			fmt.Printf("id:           %s\n", x.WorkflowID)
			fmt.Printf("task type:    %s\n", x.TaskType)
			fmt.Printf("status:       %s\n", x.Status)
			fmt.Printf("attempt:      %d/%d\n", x.Attempt, x.MaxAttempts)
			if x.WorkerID != "" {
				fmt.Printf("worker:       %s\n", x.WorkerID)
			}
			if !x.LeaseDeadline.IsZero() && x.Status == "running" {
				fmt.Printf("lease until:  %s\n", x.LeaseDeadline.Format(time.RFC3339))
			}
			if len(x.Result) > 0 {
				fmt.Printf("result:       %s\n", x.Result)
			}
			if x.Error != "" {
				fmt.Printf("error:        %s\n", x.Error)
			}
			fmt.Printf("submitted:    %s\n", x.SubmittedAt.Format(time.RFC3339))
			fmt.Printf("updated:      %s\n", x.UpdatedAt.Format(time.RFC3339))
			return nil
		},
	}
	status.Flags().StringVar(&stTenant, "tenant", "", "tenant id")
	status.Flags().StringVar(&stProject, "project", "", "project id")
	status.Flags().StringVar(&stID, "id", "", "workflow execution id")
	_ = status.MarkFlagRequired("tenant")
	_ = status.MarkFlagRequired("project")
	_ = status.MarkFlagRequired("id")

	var (
		hTenant  string
		hProject string
		hID      string
	)
	history := &cobra.Command{
		Use:   "history",
		Short: "Show an execution's state transitions rebuilt from the log",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second)
			defer cancel()
			client, err := connect(ctx)
			if err != nil {
				return err
			}
			defer client.Close()
			transitions, found, err := client.ExecutionHistory(ctx, hTenant, hProject, hID)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("execution %s/%s/%s not found", hTenant, hProject, hID)
			}
			for i, tr := range transitions {
				line := fmt.Sprintf("%2d  %-13s -> %-9s attempt=%d", i, tr.EventType, tr.Status, tr.Attempt)
				if tr.WorkerID != "" {
					line += "  worker=" + tr.WorkerID
				}
				if tr.Detail != "" {
					line += "  " + tr.Detail
				}
				fmt.Printf("%s  (%s)\n", line, tr.At.Format(time.RFC3339Nano))
			}
			return nil
		},
	}
	history.Flags().StringVar(&hTenant, "tenant", "", "tenant id")
	history.Flags().StringVar(&hProject, "project", "", "project id")
	history.Flags().StringVar(&hID, "id", "", "workflow execution id")
	_ = history.MarkFlagRequired("tenant")
	_ = history.MarkFlagRequired("project")
	_ = history.MarkFlagRequired("id")

	var (
		lStatus string
		lLimit  int
	)
	list := &cobra.Command{
		Use:   "list",
		Short: "List executions and counts by status",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second)
			defer cancel()
			client, err := connect(ctx)
			if err != nil {
				return err
			}
			defer client.Close()
			execs, counts, err := client.ListExecutions(ctx, lStatus, lLimit)
			if err != nil {
				return err
			}
			fmt.Printf("counts: pending=%d running=%d retrying=%d completed=%d failed=%d\n",
				counts["pending"], counts["running"], counts["retrying"], counts["completed"], counts["failed"])
			for _, x := range execs {
				fmt.Printf("%-30s %-12s %-10s attempt=%d updated=%s\n",
					x.Tenant+"/"+x.Project+"/"+x.WorkflowID, x.TaskType, x.Status, x.Attempt,
					x.UpdatedAt.Format(time.RFC3339))
			}
			return nil
		},
	}
	list.Flags().StringVar(&lStatus, "status", "", "filter by status (pending|running|retrying|completed|failed)")
	list.Flags().IntVar(&lLimit, "limit", 50, "maximum rows to print")

	cmd.AddCommand(submit, status, history, list)
	return cmd
}
