package avd

import (
	"context"
	"fmt"
	"net"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avd/pkg/avd/types"
)

func (s *Server) Listen(
	ctx context.Context,
	listener net.Listener,
	protocol Protocol,
	mode types.PortMode,
	opts ...ListenOption,
) (_ret *ListeningPort, _err error) {
	logger.Debugf(ctx, "Listen(ctx, '%s')", listener.Addr())
	defer func() { logger.Debugf(ctx, "/Listen(ctx, '%s'): %v %v", listener.Addr(), _ret, _err) }()

	cfg := ListenOptions(opts).Config()
	result := &ListeningPort{
		Server:                s,
		Listener:              listener,
		Protocol:              protocol,
		Config:                cfg,
		ConnectionsPublishers: make(map[net.Addr]*Connection[*NodeInput]),
		ConnectionsConsumers:  make(map[net.Addr]*Connection[*NodeOutput]),
	}

	err := result.startListening(ctx, mode)
	if err != nil {
		return nil, fmt.Errorf("unable to start listening: %w", err)
	}

	return result, nil
}
