package controlplane

import (
	"context"

	goqueuev1 "github.com/khangpt2k6/AgentBus/proto"
	"google.golang.org/grpc"
)

// Service adapts a ControlPlane to the gRPC ControlPlaneServer interface.
type Service struct {
	goqueuev1.UnimplementedControlPlaneServer
	cp *ControlPlane
}

// NewService wraps a control plane as a gRPC service.
func NewService(cp *ControlPlane) *Service { return &Service{cp: cp} }

// RegisterService registers the control-plane gRPC service on a server.
func RegisterService(gs grpc.ServiceRegistrar, cp *ControlPlane) {
	goqueuev1.RegisterControlPlaneServer(gs, NewService(cp))
}

func (s *Service) RegisterAgent(_ context.Context, req *goqueuev1.RegisterAgentRequest) (*goqueuev1.RegisterAgentResponse, error) {
	s.cp.Register(req.GetAgentId(), req.GetCapabilities())
	return &goqueuev1.RegisterAgentResponse{}, nil
}

func (s *Service) Heartbeat(_ context.Context, req *goqueuev1.HeartbeatRequest) (*goqueuev1.HeartbeatResponse, error) {
	return &goqueuev1.HeartbeatResponse{Known: s.cp.Heartbeat(req.GetAgentId())}, nil
}

// OpenInbox streams events routed to the agent until the client disconnects.
func (s *Service) OpenInbox(req *goqueuev1.OpenInboxRequest, stream grpc.ServerStreamingServer[goqueuev1.RoutedEventMsg]) error {
	ch := s.cp.OpenInbox(req.GetAgentId())
	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case re, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(&goqueuev1.RoutedEventMsg{
				SessionKey: re.SessionKey,
				Type:       re.Type,
				Payload:    re.Payload,
			}); err != nil {
				return err
			}
		}
	}
}

func (s *Service) GetSessionState(_ context.Context, req *goqueuev1.SessionStateRequest) (*goqueuev1.RunStateMsg, error) {
	rs, ok := s.cp.GetSession(req.GetTenant(), req.GetProject(), req.GetSessionId())
	if !ok {
		return &goqueuev1.RunStateMsg{Found: false}, nil
	}
	return &goqueuev1.RunStateMsg{
		SessionKey:        rs.SessionKey,
		ActiveAgent:       rs.ActiveAgent,
		PendingAgent:      rs.PendingAgent,
		StepCount:         int32(rs.StepCount),
		Status:            string(rs.Status),
		LastEventUnixNano: rs.LastEventAt.UnixNano(),
		Found:             true,
	}, nil
}

func (s *Service) ListAgents(_ context.Context, _ *goqueuev1.ListAgentsRequest) (*goqueuev1.AgentListResponse, error) {
	agents := s.cp.ListAgents()
	out := make([]*goqueuev1.AgentMsg, 0, len(agents))
	for _, a := range agents {
		out = append(out, &goqueuev1.AgentMsg{
			Id:               a.ID,
			Status:           string(a.Status),
			Capabilities:     a.Capabilities,
			CurrentSession:   a.CurrentSession,
			LastSeenUnixNano: a.LastSeen.UnixNano(),
		})
	}
	return &goqueuev1.AgentListResponse{Agents: out}, nil
}
