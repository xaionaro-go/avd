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
	logger.Debugf(ctx, "OnRouteCreated")
	defer func() { logger.Debugf(ctx, "/OnRouteCreated") }()
}

func (s *Server) OnRouteRemoved(
	ctx context.Context,
	route *router.Route,
) {
	logger.Debugf(ctx, "OnRouteRemoved")
	defer func() { logger.Debugf(ctx, "/OnRouteRemoved") }()
}
