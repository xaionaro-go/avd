package avd

import (
	"context"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avpipeline/router"
)

type Server struct {
	*router.Router
}

func NewServer(
	ctx context.Context,
) *Server {
	return &Server{
		Router: router.New(ctx),
	}
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
