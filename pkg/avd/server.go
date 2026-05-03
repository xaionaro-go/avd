// server.go implements the AVD server.

// Package avd provides the AVD server implementation.
package avd

import (
	"context"
	"errors"
	"fmt"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avd/pkg/avd/types"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/router"
	"github.com/xaionaro-go/xsync"
)

type Server struct {
	*router.Router[RouteCustomData]

	ListeningPortsLocker xsync.RWMutex
	ListeningPorts       []ListeningPort
	PrivacyBlurFilters   filterRegistry[PrivacyBlurFilterKey, *PrivacyBlurFilter]
	DeblemishFilters     filterRegistry[DeblemishFilterKey, *DeblemishFilter]
	WhisperFilters       filterRegistry[WhisperFilterKey, *WhisperFilter]
	AVSyncFilters        filterRegistry[AVSyncFilterKey, *kernel.AVSync]

	// EndpointResolver materializes regex-pattern endpoints lazily
	// on first arrival. It is set by configapplier.ApplyConfig when
	// the configuration carries any regexp-prefixed `endpoints:`
	// keys, and is consulted by the streaming consumer/publisher
	// entry points so consumer-first connects don't block on a
	// route that has no eagerly-wired forwardings yet.
	EndpointResolver EndpointResolver
}

// EndpointResolver is the minimal contract avd needs from the
// configapplier-side resolver: ensure a route is materialized for a
// given path. Defined here as an interface to avoid an avd ->
// configapplier import (which would be a cycle).
type EndpointResolver interface {
	EnsureWired(ctx context.Context, path types.RoutePath) (matched bool, err error)
}

func NewServer(
	ctx context.Context,
) *Server {
	s := &Server{
		Router:             router.New[RouteCustomData](ctx),
		PrivacyBlurFilters: newFilterRegistry[PrivacyBlurFilterKey, *PrivacyBlurFilter]("privacy_blur"),
		DeblemishFilters:   newFilterRegistry[DeblemishFilterKey, *DeblemishFilter]("deblemish"),
		WhisperFilters:     newFilterRegistry[WhisperFilterKey, *WhisperFilter]("whisper"),
		AVSyncFilters:      newFilterRegistry[AVSyncFilterKey, *kernel.AVSync]("av_sync"),
	}
	s.Router.OnRouteCreated = s.OnRouteCreated
	s.Router.OnRouteRemoved = s.OnRouteRemoved
	return s
}

func (s *Server) Close(
	ctx context.Context,
) error {
	var errs []error
	for _, port := range s.GetListeningPorts(ctx) {
		if err := port.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("unable to close listening port %s: %w", port, err))
		}
	}
	if err := s.Router.Close(ctx); err != nil {
		errs = append(errs, fmt.Errorf("unable to close router: %w", err))
	}
	return errors.Join(errs...)
}

func (s *Server) Wait(ctx context.Context) error {
	logger.Debugf(ctx, "Wait")
	defer func() { logger.Debugf(ctx, "/Wait") }()

	return s.Router.Wait(ctx)
}

func (s *Server) GetRouter() *router.Router[RouteCustomData] {
	return s.Router
}
