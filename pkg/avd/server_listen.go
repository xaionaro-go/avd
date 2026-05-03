// server_listen.go provides the main Listen method for the AVD server.

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
	mode types.StreamingPortMode,
	opts ...ListenOption,
) (_ret ListeningPort, _err error) {
	logger.Debugf(ctx, "Listen(ctx, '%s', %s, %s, %#+v)", portAddr, protocol, mode, opts)
	defer func() {
		logger.Debugf(ctx, "/Listen(ctx, '%s', %s, %s, %#+v): %v %v", portAddr, protocol, mode, opts, _ret, _err)
	}()

	switch protocol {
	case ProtocolMPEGTSUDP:
		// NOTE: do NOT add ProtocolSRT to the direct-dispatch path. ListeningPortDirect*
		// opens a single libav AVFormatContext per port — for SRT this means libav's
		// muxer/demuxer accepts one caller and binds, so the port serves at most one
		// client. Multi-client SRT fan-out (one libav instance per remote 5-tuple,
		// streamid demux into separate routes) requires the proxied path even though
		// the SRT handshake on the loopback hop is currently broken (see
		// connection_proxied_srt.go). Switching to direct trades a broken pull for
		// a single-client cap, which is worse for production.
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
		var listener net.Listener
		switch proto {
		case "udp", "srt":
			var err error
			listener, err = NewUDPListener("udp", host)
			if err != nil {
				return nil, fmt.Errorf("unable to start listening on UDP '%s': %w", host, err)
			}
		default:
			var err error
			listener, err = net.Listen(proto, host)
			if err != nil {
				return nil, fmt.Errorf("unable to start listening on '%s': %w", portAddr, err)
			}
		}

		port, err := s.ListenProxied(ctx, listener, protocol, mode, opts...)
		if err != nil {
			return nil, fmt.Errorf("unable to listen '%s' with the RTMP-%s handler: %w", listener.Addr(), mode, err)
		}

		return port, nil
	}
}
