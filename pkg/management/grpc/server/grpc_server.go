package server

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"

	"google.golang.org/grpc"

	"github.com/facebookincubator/go-belt"
	"github.com/xaionaro-go/avd/pkg/avd"
	"github.com/xaionaro-go/avd/pkg/avd/types"
	"github.com/xaionaro-go/avd/pkg/management/grpc/proto/avdmanagementgrpc"
	"github.com/xaionaro-go/avpipeline/node"
	avpipelinegrpc "github.com/xaionaro-go/avpipeline/protobuf/avpipeline"
	"github.com/xaionaro-go/avpipeline/protobuf/goconv"
	"github.com/xaionaro-go/avpipeline/router"
)

type GRPCServer struct {
	avdmanagementgrpc.AvdServiceServer
	Backend       Backend
	Listener      net.Listener
	GRPCServer    *grpc.Server
	Observability *belt.Belt
	IsServing     atomic.Bool
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
	if srv.IsServing.Swap(true) {
		return fmt.Errorf("GRPCServer is already serving")
	}
	defer func() {
		srv.IsServing.Store(false)
	}()
	return srv.GRPCServer.Serve(srv.Listener)
}

func (srv *GRPCServer) ServeContext(ctx context.Context) error {
	if srv.GRPCServer == nil {
		return fmt.Errorf("GRPCServer is not initialized")
	}
	if srv.IsServing.Swap(true) {
		return fmt.Errorf("GRPCServer is already serving")
	}
	defer func() {
		srv.IsServing.Store(false)
	}()
	srv.Observability = belt.CtxBelt(ctx)
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

func (srv *GRPCServer) ctx(ctx context.Context) context.Context {
	return belt.CtxWithBelt(ctx, srv.Observability)
}

func (srv *GRPCServer) ListPublishers(
	ctx context.Context,
	req *avdmanagementgrpc.ListPublishersRequest,
) (*avdmanagementgrpc.ListPublishersResponse, error) {
	ctx = srv.ctx(ctx)
	var result []*avpipelinegrpc.Node
	ports := srv.Backend.GetListeningPorts(ctx)
	for _, port := range ports {
		switch port.GetMode() {
		case avd.PortModePublishers:
			for _, conn := range port.GetConnections(ctx) {
				result = append(result, goconv.NodeToGRPC(ctx, conn.GetNode(ctx)))
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
	ctx = srv.ctx(ctx)
	var result []*avpipelinegrpc.Node
	router := srv.Backend.GetRouter()
	router.Locker.Do(ctx, func() {
		for _, route := range router.RoutesByPath {
			if route == nil {
				continue
			}
			node := goconv.NodeToGRPC(ctx, route.Node)
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
	ctx = srv.ctx(ctx)
	var result []*avpipelinegrpc.Node
	ports := srv.Backend.GetListeningPorts(ctx)
	for _, port := range ports {
		switch port.GetMode() {
		case avd.PortModeConsumers:
			for _, conn := range port.GetConnections(ctx) {
				result = append(result, goconv.NodeToGRPC(ctx, conn.GetNode(ctx)))
			}
		}
	}
	return &avdmanagementgrpc.ListConsumersResponse{
		Nodes: result,
	}, nil
}

func (srv *GRPCServer) getPipeline(
	ctx context.Context,
) (result []node.Abstract) {
	ports := srv.Backend.GetListeningPorts(ctx)
	for _, port := range ports {
		switch port.GetMode() {
		case avd.PortModePublishers:
			for _, conn := range port.GetConnections(ctx) {
				result = append(result, conn.GetNode(ctx))
			}
		}
	}
	return
}
