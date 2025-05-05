package avd

import (
	"context"
	"fmt"
	"net"
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
) (*ListeningPortRTMP, error) {
	result := &ListeningPortRTMP{
		Server:      s,
		Listener:    listener,
		Connections: make(map[net.Addr]*ConnectionRTMP),
	}

	err := result.StartListening(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to start listening: %w", err)
	}

	return result, nil
}
