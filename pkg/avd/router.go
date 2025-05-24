package avd

import (
	"context"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avpipeline/router"
)

type RouteCustomData struct{}
type RouteSource[T Publisher] = router.RouteSource[RouteCustomData, T, *ProcessorInput]

func (s *Server) OnRouteCreated(
	ctx context.Context,
	route *router.Route[RouteCustomData],
) {
	logger.Debugf(ctx, "OnRouteCreated: %s", route)
	defer func() { logger.Debugf(ctx, "/OnRouteCreated: %s", route) }()
}

func (s *Server) OnRouteRemoved(
	ctx context.Context,
	route *router.Route[RouteCustomData],
) {
	logger.Debugf(ctx, "OnRouteRemoved: %s", route)
	defer func() { logger.Debugf(ctx, "/OnRouteRemoved: %s", route) }()
}
