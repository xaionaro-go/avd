// router.go provides router-related methods for the AVD server.

package avd

import (
	"context"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avd/pkg/avd/types"
	"github.com/xaionaro-go/avpipeline/router"
)

type (
	RouteCustomData          = types.RouteCustomData
	RouteSource[T Publisher] = router.RouteSource[RouteCustomData, T, *ProcessorInput]
)

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
	path := route.Path
	if err := s.UnregisterWhisperFiltersByRoutePath(ctx, path); err != nil {
		logger.Errorf(ctx, "UnregisterWhisperFiltersByRoutePath('%s'): %v", path, err)
	}
	if err := s.UnregisterPrivacyBlurFiltersByRoutePath(ctx, path); err != nil {
		logger.Errorf(ctx, "UnregisterPrivacyBlurFiltersByRoutePath('%s'): %v", path, err)
	}
	if err := s.UnregisterDeblemishFiltersByRoutePath(ctx, path); err != nil {
		logger.Errorf(ctx, "UnregisterDeblemishFiltersByRoutePath('%s'): %v", path, err)
	}
	if err := s.UnregisterAVSyncFiltersByRoutePath(ctx, path); err != nil {
		logger.Errorf(ctx, "UnregisterAVSyncFiltersByRoutePath('%s'): %v", path, err)
	}
}
