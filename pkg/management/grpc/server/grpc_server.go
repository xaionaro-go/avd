// grpc_server.go implements the gRPC server for AVD management.

// Package server provides a gRPC server for the AVD management service.
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
	goconvavp "github.com/xaionaro-go/avpipeline/protobuf/goconv/avpipeline"
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
	SetPrivacyBlurState(ctx context.Context, key avd.PrivacyBlurControlKey, enabled *bool, blurRadius *float64, pixelateBlockSize *int64) error
	GetPrivacyBlurState(ctx context.Context, key avd.PrivacyBlurControlKey) (enabled bool, blurRadius float64, pixelateBlockSize int64, err error)
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
				result = append(result, goconvavp.NodeToGRPC(ctx, conn.GetNode(ctx)))
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
			node := goconvavp.NodeToGRPC(ctx, route.Node)
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
				result = append(result, goconvavp.NodeToGRPC(ctx, conn.GetNode(ctx)))
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

func (srv *GRPCServer) SetPrivacyBlur(
	ctx context.Context,
	req *avdmanagementgrpc.SetPrivacyBlurRequest,
) (*avdmanagementgrpc.SetPrivacyBlurResponse, error) {
	ctx = srv.ctx(ctx)
	key := avd.PrivacyBlurControlKey{
		RoutePath:       router.RoutePath(req.RoutePath),
		ForwardingIndex: int(req.ForwardingIndex),
	}
	var enabled *bool
	if req.Enabled != nil {
		v := *req.Enabled
		enabled = &v
	}
	var blurRadius *float64
	if req.BlurRadius != nil {
		v := *req.BlurRadius
		blurRadius = &v
	}
	var pixelateBlockSize *int64
	if req.PixelateBlockSize != nil {
		v := *req.PixelateBlockSize
		pixelateBlockSize = &v
	}
	if err := srv.Backend.SetPrivacyBlurState(ctx, key, enabled, blurRadius, pixelateBlockSize); err != nil {
		return nil, err
	}
	return &avdmanagementgrpc.SetPrivacyBlurResponse{}, nil
}

func (srv *GRPCServer) GetPrivacyBlur(
	ctx context.Context,
	req *avdmanagementgrpc.GetPrivacyBlurRequest,
) (*avdmanagementgrpc.GetPrivacyBlurResponse, error) {
	ctx = srv.ctx(ctx)
	key := avd.PrivacyBlurControlKey{
		RoutePath:       router.RoutePath(req.RoutePath),
		ForwardingIndex: int(req.ForwardingIndex),
	}
	enabled, blurRadius, pixelateBlockSize, err := srv.Backend.GetPrivacyBlurState(ctx, key)
	if err != nil {
		return nil, err
	}
	return &avdmanagementgrpc.GetPrivacyBlurResponse{
		Enabled:           enabled,
		BlurRadius:        blurRadius,
		PixelateBlockSize: pixelateBlockSize,
	}, nil
}
