package avd

import (
	"context"
	"fmt"
	"net"
)

type Server struct{}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) ListenRTMP(
	ctx context.Context,
	listener net.Listener,
) (*ListeningPortRTMP, error) {
	result := &ListeningPortRTMP{
		Server:      s,
		Listener:    listener,
		Connections: map[net.Addr]*ConnectionRTMP{},
	}
	err := result.StartListening(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to start listening: %w", err)
	}
	return result, nil
}
