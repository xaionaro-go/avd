// server.go implements the AVD server.

// Package avd provides the AVD server implementation.
package avd

import (
	"context"
	"errors"
	"fmt"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avpipeline/router"
	"github.com/xaionaro-go/xsync"
)

type Server struct {
	*router.Router[RouteCustomData]

	ListeningPortsLocker xsync.RWMutex
	ListeningPorts       []ListeningPort
	PrivacyBlurRegistry  privacyBlurRegistry
	DeblemishRegistry    deblemishRegistry
}

func NewServer(
	ctx context.Context,
) *Server {
	s := &Server{
		Router: router.New[RouteCustomData](ctx),
		PrivacyBlurRegistry: privacyBlurRegistry{
			Controls: make(map[PrivacyBlurControlKey]*PrivacyBlurControl),
		},
		DeblemishRegistry: deblemishRegistry{
			Controls: make(map[DeblemishControlKey]*DeblemishControl),
		},
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
