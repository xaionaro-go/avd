package avd

import (
	"context"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avpipeline/router"
)

func (s *Server) OnRouteCreated(
	ctx context.Context,
	route *router.Route,
) {
	logger.Debugf(ctx, "OnRouteCreated: %s", route)
	defer func() { logger.Debugf(ctx, "/OnRouteCreated: %s", route) }()
}

func (s *Server) OnRouteRemoved(
	ctx context.Context,
	route *router.Route,
) {
	logger.Debugf(ctx, "OnRouteRemoved: %s", route)
	defer func() { logger.Debugf(ctx, "/OnRouteRemoved: %s", route) }()
}
