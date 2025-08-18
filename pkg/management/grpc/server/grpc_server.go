package server

import (
	"context"
	"fmt"
	"net"

	"google.golang.org/grpc"

	"github.com/xaionaro-go/avd/pkg/avd"
	"github.com/xaionaro-go/avd/pkg/avd/types"
	"github.com/xaionaro-go/avd/pkg/management/grpc/proto/avdmanagementgrpc"
	avpipelinegrpc "github.com/xaionaro-go/avpipeline/protobuf/avpipeline"
	"github.com/xaionaro-go/avpipeline/protobuf/goconv"
	"github.com/xaionaro-go/avpipeline/router"
)

type GRPCServer struct {
	avdmanagementgrpc.AvdServiceServer
	Backend    Backend
	Listener   net.Listener
	GRPCServer *grpc.Server
}

type Backend interface {
	GetListeningPorts(ctx context.Context) []avd.ListeningPort
	GetRouter() *router.Router[types.RouteCustomData]
}

func New(
	backend Backend,
	listener net.Listener,
) *GRPCServer {
	srv := &GRPCServer{
		Backend:    backend,
		Listener:   listener,
		GRPCServer: grpc.NewServer(),
	}
	avdmanagementgrpc.RegisterAvdServiceServer(srv.GRPCServer, srv)
	return srv
}

func (srv *GRPCServer) Serve() error {
	if srv.GRPCServer == nil {
		return fmt.Errorf("GRPCServer is not initialized")
	}
	return srv.GRPCServer.Serve(srv.Listener)
}

func (srv *GRPCServer) ServeContext(ctx context.Context) error {
	if srv.GRPCServer == nil {
		return fmt.Errorf("GRPCServer is not initialized")
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.GRPCServer.Serve(srv.Listener)
	}()
	select {
	case <-ctx.Done():
		srv.GRPCServer.GracefulStop()
		return ctx.Err()
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("failed to serve gRPC: %w", err)
		}
		return nil
	}
}

func (srv *GRPCServer) ListPublishers(
	ctx context.Context,
	req *avdmanagementgrpc.ListPublishersRequest,
) (*avdmanagementgrpc.ListPublishersResponse, error) {
	var result []*avpipelinegrpc.Node
	ports := srv.Backend.GetListeningPorts(ctx)
	for _, port := range ports {
		switch port.GetMode() {
		case avd.PortModePublishers:
			for _, conn := range port.GetConnections(ctx) {
				result = append(result, goconv.NodeToGRPC(conn.GetNode(ctx)))
			}
		}
	}
	return &avdmanagementgrpc.ListPublishersResponse{
		Nodes: result,
	}, nil
}

func (srv *GRPCServer) ListRoutes(
	ctx context.Context,
	req *avdmanagementgrpc.ListRoutesRequest,
) (*avdmanagementgrpc.ListRoutesResponse, error) {
	var result []*avpipelinegrpc.Node
	router := srv.Backend.GetRouter()
	router.Locker.Do(ctx, func() {
		for _, route := range router.RoutesByPath {
			if route == nil {
				continue
			}
			node := goconv.NodeToGRPC(route.Node)
			if node != nil {
				result = append(result, node)
			}
		}
	})
	return &avdmanagementgrpc.ListRoutesResponse{
		Nodes: result,
	}, nil
}

func (srv *GRPCServer) ListConsumers(
	ctx context.Context,
	req *avdmanagementgrpc.ListConsumersRequest,
) (*avdmanagementgrpc.ListConsumersResponse, error) {
	var result []*avpipelinegrpc.Node
	ports := srv.Backend.GetListeningPorts(ctx)
	for _, port := range ports {
		switch port.GetMode() {
		case avd.PortModeConsumers:
			for _, conn := range port.GetConnections(ctx) {
				result = append(result, goconv.NodeToGRPC(conn.GetNode(ctx)))
			}
		}
	}
	return &avdmanagementgrpc.ListConsumersResponse{
		Nodes: result,
	}, nil
}
