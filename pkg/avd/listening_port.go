package avd

import (
	"context"
	"fmt"
	"net"

	"github.com/facebookincubator/go-belt"
	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avd/pkg/avd/types"
	"github.com/xaionaro-go/observability"
	"github.com/xaionaro-go/xsync"
)

type ListeningPort struct {
	Server                *Server
	Listener              net.Listener
	Protocol              Protocol
	ConnectionsLocker     xsync.Mutex
	ConnectionsPublishers map[net.Addr]*Connection[*NodeInput]
	ConnectionsConsumers  map[net.Addr]*Connection[*NodeOutput]
	Config                ListenConfig
}

func (p *ListeningPort) StartListening(
	ctx context.Context,
	mode types.PortMode,
) error {
	switch mode {
	case PortModePublishers:
		observability.Go(ctx, func() {
			err := p.listenPublishers(ctx)
			if err != nil {
				logger.Errorf(ctx, "unable to listen %s: %v", p.Listener.Addr(), err)
				p.Close(ctx)
			}
		})
	case PortModeConsumers:
		observability.Go(ctx, func() {
			err := p.listenConsumers(ctx)
			if err != nil {
				logger.Errorf(ctx, "unable to listen %s: %v", p.Listener.Addr(), err)
				p.Close(ctx)
			}
		})
	default:
		return fmt.Errorf("unknown RTMP mode '%s'", mode)
	}

	return nil
}

func (p *ListeningPort) GetServer() *Server {
	return p.Server
}

func (p *ListeningPort) GetConfig() ListenConfig {
	return p.Config
}

func (p *ListeningPort) Close(ctx context.Context) error {
	p.Listener.Close()
	return nil
}

func (p *ListeningPort) listenPublishers(
	ctx context.Context,
) error {
	// TODO: deduplicate with listenConsumers
	ctx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()
	ctx = belt.WithField(ctx, "port_mode", PortModePublishers.String())
	observability.Go(ctx, func() {
		<-ctx.Done()
		p.Listener.Close()
	})
	for {
		netConn, err := p.Listener.Accept()
		if err != nil {
			return fmt.Errorf("unable to accept a connection: %w", err)
		}

		conn, err := newConnection[*NodeInput](ctx, p, netConn)
		if err != nil {
			return fmt.Errorf("unable to initialize a connection for '%s': %w", netConn.RemoteAddr(), err)
		}

		if err := xsync.DoR1(ctx, &p.ConnectionsLocker, func() error {
			if oldRTMPConn, ok := p.ConnectionsPublishers[netConn.RemoteAddr()]; ok {
				logger.Errorf(ctx, "there is already a connection from '%s', closing the old one", netConn.RemoteAddr())
				if err := oldRTMPConn.Close(ctx); err != nil {
					logger.Errorf(ctx, "unable to close the old connection from '%s': %v", netConn.RemoteAddr(), err)
				}
			}
			p.ConnectionsPublishers[netConn.RemoteAddr()] = conn
			return nil
		}); err != nil {
			if err := conn.Close(ctx); err != nil {
				logger.Errorf(ctx, "unable to close the connection from '%s': %v", netConn.RemoteAddr(), err)
			}
			return fmt.Errorf("unable to store the new connection: %w", err)
		}
	}
}

func (p *ListeningPort) listenConsumers(
	ctx context.Context,
) error {
	// TODO: deduplicate with listenPublishers
	ctx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()
	ctx = belt.WithField(ctx, "port_mode", PortModeConsumers.String())
	observability.Go(ctx, func() {
		<-ctx.Done()
		p.Listener.Close()
	})
	for {
		conn, err := p.Listener.Accept()
		if err != nil {
			return fmt.Errorf("unable to accept a connection: %w", err)
		}

		rtmpConn, err := newConnection[*NodeOutput](ctx, p, conn)
		if err != nil {
			return fmt.Errorf("unable to initialize a connection for '%s': %w", conn.RemoteAddr(), err)
		}

		if err := xsync.DoR1(ctx, &p.ConnectionsLocker, func() error {
			if oldRTMPConn, ok := p.ConnectionsConsumers[conn.RemoteAddr()]; ok {
				logger.Errorf(ctx, "there is already a connection from '%s', closing the old one", conn.RemoteAddr())
				if err := oldRTMPConn.Close(ctx); err != nil {
					logger.Errorf(ctx, "unable to close the old connection from '%s': %v", conn.RemoteAddr(), err)
				}
			}
			p.ConnectionsConsumers[conn.RemoteAddr()] = rtmpConn
			return nil
		}); err != nil {
			if err := rtmpConn.Close(ctx); err != nil {
				logger.Errorf(ctx, "unable to close the connection from '%s': %v", conn.RemoteAddr(), err)
			}
			return fmt.Errorf("unable to store the new connection: %w", err)
		}
	}
}
