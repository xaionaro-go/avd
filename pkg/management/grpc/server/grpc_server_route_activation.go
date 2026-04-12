package server

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/xaionaro-go/avd/pkg/management/grpc/proto/avdmanagementgrpc"
	"github.com/xaionaro-go/avpipeline/router"
)

func (srv *GRPCServer) ActivateRoute(
	ctx context.Context,
	req *avdmanagementgrpc.ActivateRouteRequest,
) (*avdmanagementgrpc.ActivateRouteResponse, error) {
	ctx = srv.ctx(ctx)

	r := srv.Backend.GetRouter()
	route, err := r.GetRoute(ctx, router.RoutePath(req.GetRoutePath()), router.GetRouteModeFailIfNotFound)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "route %q: %v", req.GetRoutePath(), err)
	}

	consumer := sentinelConsumerForRoute(req.GetRoutePath())
	_, err = route.AddConsumer(ctx, consumer)
	switch {
	case err == nil:
	case errors.As(err, &router.ErrAlreadyAConsumer{}):
		// Idempotent: already activated.
	default:
		return nil, status.Errorf(codes.Internal, "activate route %q: %v", req.GetRoutePath(), err)
	}

	return &avdmanagementgrpc.ActivateRouteResponse{}, nil
}

func (srv *GRPCServer) DeactivateRoute(
	ctx context.Context,
	req *avdmanagementgrpc.DeactivateRouteRequest,
) (*avdmanagementgrpc.DeactivateRouteResponse, error) {
	ctx = srv.ctx(ctx)

	r := srv.Backend.GetRouter()
	route, err := r.GetRoute(ctx, router.RoutePath(req.GetRoutePath()), router.GetRouteModeFailIfNotFound)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "route %q: %v", req.GetRoutePath(), err)
	}

	consumer := sentinelConsumerForRoute(req.GetRoutePath())
	_, err = route.RemoveConsumer(ctx, consumer)
	switch {
	case err == nil:
	case errors.As(err, &router.ErrConsumerNotFound{}):
		// Idempotent: already deactivated.
	default:
		return nil, status.Errorf(codes.Internal, "deactivate route %q: %v", req.GetRoutePath(), err)
	}

	return &avdmanagementgrpc.DeactivateRouteResponse{}, nil
}
