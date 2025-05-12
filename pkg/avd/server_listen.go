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
	portAddr PortAddress,
	protocol Protocol,
	mode types.PortMode,
	opts ...ListenOption,
) (_ret ListeningPort, _err error) {
	logger.Debugf(ctx, "Listen(ctx, '%s')", portAddr)
	defer func() { logger.Debugf(ctx, "/Listen(ctx, '%s'): %v %v", portAddr, _ret, _err) }()

	switch protocol {
	case ProtocolMPEGTSUDP:
		port, err := s.ListenDirect(ctx, portAddr, protocol, mode, opts...)
		if err != nil {
			return nil, err
		}
		return port, nil
	default:
		proto, host, err := portAddr.Parse(ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to parse the port string '%s': %w", portAddr, err)
		}
		logger.Debugf(ctx, "parsed: transport='%s', host='%s' (orig='%s')", proto, host, portAddr)
		listener, err := net.Listen(proto, host)
		if err != nil {
			return nil, fmt.Errorf("unable to start listening on '%s': %w", portAddr, err)
		}

		port, err := s.ListenProxied(ctx, listener, protocol, mode, opts...)
		if err != nil {
			return nil, fmt.Errorf("unable to listen '%s' with the RTMP-%s handler: %w", listener.Addr(), mode, err)
		}

		return port, nil
	}
}
