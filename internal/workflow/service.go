package workflow

import (
	"context"
	"time"

	goqueuev1 "github.com/khangpt2k6/EventBus/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RouteChecker is the minimum surface Submit needs from the cluster router
// to redirect callers to the shard leader. Mirrors grpcapi.RouteChecker so
// single-node mode and tests don't import the cluster package.
type RouteChecker interface {
	RouteSession(tenant, project, sessionID string) (isLocal bool, shardID uint32, leaderClientAddr string)
}

// Service exposes the coordinator over the WorkflowService gRPC API.
type Service struct {
	goqueuev1.UnimplementedWorkflowServiceServer
	coord      *Coordinator
	routeCheck RouteChecker
}

// NewService wraps a coordinator. routeCheck may be nil in single-node mode.
func NewService(c *Coordinator, routeCheck RouteChecker) *Service {
	return &Service{coord: c, routeCheck: routeCheck}
}

// RegisterService registers the workflow service on a gRPC server.
func RegisterService(s *grpc.Server, svc *Service) {
	goqueuev1.RegisterWorkflowServiceServer(s, svc)
}

func requireIDs(tenant, project, workflowID string) error {
	if tenant == "" || project == "" || workflowID == "" {
		return status.Error(codes.InvalidArgument, "tenant, project, and workflow_id are required")
	}
	return nil
}

func (s *Service) SubmitWorkflow(ctx context.Context, req *goqueuev1.SubmitWorkflowRequest) (*goqueuev1.SubmitWorkflowResponse, error) {
	if err := requireIDs(req.Tenant, req.Project, req.WorkflowId); err != nil {
		return nil, err
	}
	if req.TaskType == "" {
		return nil, status.Error(codes.InvalidArgument, "task_type is required")
	}
	if s.routeCheck != nil {
		isLocal, _, hint := s.routeCheck.RouteSession(req.Tenant, req.Project, req.WorkflowId)
		if !isLocal {
			st := status.New(codes.FailedPrecondition, "not the leader of this execution's shard")
			if withDetails, derr := st.WithDetails(&goqueuev1.NotLeaderError{LeaderAddr: hint}); derr == nil {
				st = withDetails
			}
			return nil, st.Err()
		}
	}
	already, err := s.coord.Submit(ctx, SubmitSpec{
		Tenant:      req.Tenant,
		Project:     req.Project,
		WorkflowID:  req.WorkflowId,
		TaskType:    req.TaskType,
		Input:       req.Input,
		MaxAttempts: int(req.MaxAttempts),
		LeaseTTL:    time.Duration(req.LeaseTtlMs) * time.Millisecond,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "submit failed: %v", err)
	}
	return &goqueuev1.SubmitWorkflowResponse{AlreadyExists: already}, nil
}

func (s *Service) LeaseTask(ctx context.Context, req *goqueuev1.LeaseTaskRequest) (*goqueuev1.LeaseTaskResponse, error) {
	if req.TaskType == "" || req.WorkerId == "" {
		return nil, status.Error(codes.InvalidArgument, "task_type and worker_id are required")
	}
	max := int(req.MaxTasks)
	if max <= 0 {
		max = 1
	}
	if max > 256 {
		max = 256
	}
	leases, err := s.coord.LeaseBatch(ctx, req.TaskType, req.WorkerId, max, time.Duration(req.WaitMs)*time.Millisecond)
	if err != nil {
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return nil, status.Errorf(codes.Internal, "lease failed: %v", err)
	}
	if len(leases) == 0 {
		return &goqueuev1.LeaseTaskResponse{Found: false}, nil
	}
	resp := &goqueuev1.LeaseTaskResponse{
		Found:                 true,
		Tenant:                leases[0].Tenant,
		Project:               leases[0].Project,
		WorkflowId:            leases[0].WorkflowID,
		TaskType:              leases[0].TaskType,
		Input:                 leases[0].Input,
		Attempt:               int32(leases[0].Attempt),
		LeaseDeadlineUnixNano: leases[0].LeaseDeadline.UnixNano(),
	}
	if req.MaxTasks > 1 {
		resp.Tasks = make([]*goqueuev1.LeasedTaskMsg, 0, len(leases))
		for _, l := range leases {
			resp.Tasks = append(resp.Tasks, &goqueuev1.LeasedTaskMsg{
				Tenant:                l.Tenant,
				Project:               l.Project,
				WorkflowId:            l.WorkflowID,
				TaskType:              l.TaskType,
				Input:                 l.Input,
				Attempt:               int32(l.Attempt),
				LeaseDeadlineUnixNano: l.LeaseDeadline.UnixNano(),
			})
		}
	}
	return resp, nil
}

func (s *Service) SubmitWorkflows(ctx context.Context, req *goqueuev1.SubmitWorkflowsRequest) (*goqueuev1.SubmitWorkflowsResponse, error) {
	if len(req.Requests) == 0 {
		return nil, status.Error(codes.InvalidArgument, "requests must not be empty")
	}
	resp := &goqueuev1.SubmitWorkflowsResponse{}
	for _, r := range req.Requests {
		one, err := s.SubmitWorkflow(ctx, r)
		if err != nil {
			return nil, err
		}
		if one.AlreadyExists {
			resp.AlreadyExists++
		} else {
			resp.Accepted++
		}
	}
	return resp, nil
}

func (s *Service) CompleteTasks(ctx context.Context, req *goqueuev1.CompleteTasksRequest) (*goqueuev1.CompleteTasksResponse, error) {
	if len(req.Requests) == 0 {
		return nil, status.Error(codes.InvalidArgument, "requests must not be empty")
	}
	resp := &goqueuev1.CompleteTasksResponse{}
	for _, r := range req.Requests {
		one, err := s.CompleteTask(ctx, r)
		if err != nil {
			return nil, err
		}
		if one.Accepted {
			resp.Accepted++
		} else {
			resp.Rejected++
		}
	}
	return resp, nil
}

func (s *Service) HeartbeatTask(ctx context.Context, req *goqueuev1.HeartbeatTaskRequest) (*goqueuev1.HeartbeatTaskResponse, error) {
	if err := requireIDs(req.Tenant, req.Project, req.WorkflowId); err != nil {
		return nil, err
	}
	valid, deadline, err := s.coord.Heartbeat(ctx, req.Tenant, req.Project, req.WorkflowId, req.WorkerId, int(req.Attempt))
	if err != nil && err != ErrNotFound {
		return nil, status.Errorf(codes.Internal, "heartbeat failed: %v", err)
	}
	resp := &goqueuev1.HeartbeatTaskResponse{Valid: valid}
	if valid {
		resp.LeaseDeadlineUnixNano = deadline.UnixNano()
	}
	return resp, nil
}

func (s *Service) CompleteTask(ctx context.Context, req *goqueuev1.CompleteTaskRequest) (*goqueuev1.CompleteTaskResponse, error) {
	if err := requireIDs(req.Tenant, req.Project, req.WorkflowId); err != nil {
		return nil, err
	}
	accepted, duplicate, err := s.coord.Complete(ctx, req.Tenant, req.Project, req.WorkflowId, req.WorkerId, int(req.Attempt), req.Result)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "complete failed: %v", err)
	}
	return &goqueuev1.CompleteTaskResponse{Accepted: accepted, Duplicate: duplicate}, nil
}

func (s *Service) FailTask(ctx context.Context, req *goqueuev1.FailTaskRequest) (*goqueuev1.FailTaskResponse, error) {
	if err := requireIDs(req.Tenant, req.Project, req.WorkflowId); err != nil {
		return nil, err
	}
	accepted, willRetry, err := s.coord.Fail(ctx, req.Tenant, req.Project, req.WorkflowId, req.WorkerId, int(req.Attempt), req.Error, req.Retryable)
	if err != nil && err != ErrNotFound {
		return nil, status.Errorf(codes.Internal, "fail failed: %v", err)
	}
	return &goqueuev1.FailTaskResponse{Accepted: accepted, WillRetry: willRetry}, nil
}

func (s *Service) GetExecution(ctx context.Context, req *goqueuev1.GetExecutionRequest) (*goqueuev1.ExecutionStateMsg, error) {
	if err := requireIDs(req.Tenant, req.Project, req.WorkflowId); err != nil {
		return nil, err
	}
	x, ok := s.coord.Store().Get(req.Tenant, req.Project, req.WorkflowId)
	if !ok {
		return &goqueuev1.ExecutionStateMsg{Found: false}, nil
	}
	msg := &goqueuev1.ExecutionStateMsg{
		Found:             true,
		Tenant:            x.Tenant,
		Project:           x.Project,
		WorkflowId:        x.WorkflowID,
		TaskType:          x.TaskType,
		Status:            string(x.Status),
		Attempt:           int32(x.Attempt),
		MaxAttempts:       int32(x.MaxAttempts),
		WorkerId:          x.WorkerID,
		Result:            x.Result,
		Error:             x.Error,
		SubmittedUnixNano: x.SubmittedAt.UnixNano(),
		UpdatedUnixNano:   x.UpdatedAt.UnixNano(),
	}
	if !x.LeaseDeadline.IsZero() {
		msg.LeaseDeadlineUnixNano = x.LeaseDeadline.UnixNano()
	}
	return msg, nil
}

func (s *Service) GetExecutionHistory(ctx context.Context, req *goqueuev1.GetExecutionRequest) (*goqueuev1.ExecutionHistoryResponse, error) {
	if err := requireIDs(req.Tenant, req.Project, req.WorkflowId); err != nil {
		return nil, err
	}
	x, ok := s.coord.Store().Get(req.Tenant, req.Project, req.WorkflowId)
	if !ok {
		return &goqueuev1.ExecutionHistoryResponse{Found: false}, nil
	}
	out := make([]*goqueuev1.TransitionMsg, 0, len(x.Transitions))
	for _, tr := range x.Transitions {
		out = append(out, &goqueuev1.TransitionMsg{
			EventType:  tr.EventType,
			Status:     string(tr.Status),
			Attempt:    int32(tr.Attempt),
			WorkerId:   tr.WorkerID,
			Detail:     tr.Detail,
			AtUnixNano: tr.At.UnixNano(),
		})
	}
	return &goqueuev1.ExecutionHistoryResponse{Found: true, Transitions: out}, nil
}

func (s *Service) ListExecutions(ctx context.Context, req *goqueuev1.ListExecutionsRequest) (*goqueuev1.ListExecutionsResponse, error) {
	limit := int(req.Limit)
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	execs, counts := s.coord.Store().List(Status(req.Status), limit)
	out := make([]*goqueuev1.ExecutionSummaryMsg, 0, len(execs))
	for _, x := range execs {
		out = append(out, &goqueuev1.ExecutionSummaryMsg{
			Tenant:          x.Tenant,
			Project:         x.Project,
			WorkflowId:      x.WorkflowID,
			TaskType:        x.TaskType,
			Status:          string(x.Status),
			Attempt:         int32(x.Attempt),
			UpdatedUnixNano: x.UpdatedAt.UnixNano(),
		})
	}
	countsOut := make(map[string]int64, len(counts))
	for st, n := range counts {
		countsOut[string(st)] = int64(n)
	}
	return &goqueuev1.ListExecutionsResponse{Executions: out, Counts: countsOut}, nil
}
