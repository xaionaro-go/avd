package avd

import (
	"context"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avpipeline/router"
)

type Server struct {
	*router.Router[RouteCustomData]
}

func NewServer(
	ctx context.Context,
) *Server {
	s := &Server{
		Router: router.New[RouteCustomData](ctx),
	}
	s.Router.OnRouteCreated = s.OnRouteCreated
	s.Router.OnRouteRemoved = s.OnRouteRemoved
	return s
}

func (s *Server) Close(
	ctx context.Context,
) error {
	return s.Router.Close(ctx)
}

func (s *Server) Wait(ctx context.Context) error {
	logger.Debugf(ctx, "Wait")
	defer func() { logger.Debugf(ctx, "/Wait") }()

	// TODO: add a waiter for Listeners to end

	return s.Router.Wait(ctx)
}
