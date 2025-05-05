package avd

import (
	"context"
	"fmt"
	"net"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avd/pkg/avd/types"
	"github.com/xaionaro-go/observability"
)

type Server struct {
	*Router
}

func NewServer(
	ctx context.Context,
) *Server {
	return &Server{
		Router: newRouter(ctx),
	}
}

func (s *Server) Close(
	ctx context.Context,
) error {
	return s.Router.Close(ctx)
}

func (s *Server) ListenRTMP(
	ctx context.Context,
	listener net.Listener,
	mode types.RTMPMode,
	opts ...ListeningPortRTMPOption,
) (_ret *ListeningPortRTMP, _err error) {
	logger.Debugf(ctx, "ListenRTMP(ctx, '%s')", listener.Addr())
	defer func() { logger.Debugf(ctx, "/ListenRTMP(ctx, '%s'): %v %v", listener.Addr(), _ret, _err) }()

	var cfg ListeningPortRTMPConfig
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	result := &ListeningPortRTMP{
		Server:                s,
		Listener:              listener,
		Config:                cfg,
		ConnectionsPublishers: make(map[net.Addr]*ConnectionRTMP[*NodeInput]),
		ConnectionsConsumers:  make(map[net.Addr]*ConnectionRTMP[*NodeOutput]),
	}

	err := result.StartListening(ctx, mode)
	if err != nil {
		return nil, fmt.Errorf("unable to start listening: %w", err)
	}

	return result, nil
}

func (s *Server) Wait(ctx context.Context) {
	logger.Debugf(ctx, "Wait")
	defer func() { logger.Debugf(ctx, "/Wait") }()
	endCh := make(chan struct{})
	observability.Go(ctx, func() { // TODO: fix this leak
		s.WaitGroup.Wait()
		close(endCh)
	})

	select {
	case <-ctx.Done():
		return
	case <-endCh:
		return
	}
}
