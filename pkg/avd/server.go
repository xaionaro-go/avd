// server.go implements the AVD server.

// Package avd provides the AVD server implementation.
package avd

import (
	"context"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avpipeline/router"
	"github.com/xaionaro-go/xsync"
)

type Server struct {
	*router.Router[RouteCustomData]

	ListeningPortsLocker xsync.RWMutex
	ListeningPorts       []ListeningPort
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

func (s *Server) GetRouter() *router.Router[RouteCustomData] {
	return s.Router
}
