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

type ListeningPortRTMP struct {
	Server                *Server
	Listener              net.Listener
	ConnectionsLocker     xsync.Mutex
	ConnectionsPublishers map[net.Addr]*ConnectionRTMP[*NodeInput]
	ConnectionsConsumers  map[net.Addr]*ConnectionRTMP[*NodeOutput]
	Config                ListeningPortRTMPConfig
}

func (p *ListeningPortRTMP) StartListening(
	ctx context.Context,
	mode types.RTMPMode,
) error {
	switch mode {
	case RTMPModePublishers:
		observability.Go(ctx, func() {
			err := p.listenPublishers(ctx)
			if err != nil {
				logger.Errorf(ctx, "unable to listen %s: %v", p.Listener.Addr(), err)
				p.Close(ctx)
			}
		})
	case RTMPModeConsumers:
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

func (p *ListeningPortRTMP) GetServer() *Server {
	return p.Server
}

func (p *ListeningPortRTMP) GetConfig() ListeningPortRTMPConfig {
	return p.Config
}

func (p *ListeningPortRTMP) Close(ctx context.Context) error {
	p.Listener.Close()
	return nil
}

func (p *ListeningPortRTMP) listenPublishers(
	ctx context.Context,
) error {
	// TODO: deduplicate with listenConsumers
	ctx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()
	ctx = belt.WithField(ctx, "rtmp_mode", RTMPModePublishers.String())
	observability.Go(ctx, func() {
		<-ctx.Done()
		p.Listener.Close()
	})
	for {
		conn, err := p.Listener.Accept()
		if err != nil {
			return fmt.Errorf("unable to accept a connection: %w", err)
		}

		rtmpConn, err := newConnectionRTMP[*NodeInput](ctx, p, conn)
		if err != nil {
			return fmt.Errorf("unable to initialize a connection for '%s': %w", conn.RemoteAddr(), err)
		}

		if err := xsync.DoR1(ctx, &p.ConnectionsLocker, func() error {
			if oldRTMPConn, ok := p.ConnectionsPublishers[conn.RemoteAddr()]; ok {
				logger.Errorf(ctx, "there is already a connection from '%s', closing the old one", conn.RemoteAddr())
				if err := oldRTMPConn.Close(ctx); err != nil {
					logger.Errorf(ctx, "unable to close the old connection from '%s': %v", conn.RemoteAddr(), err)
				}
			}
			p.ConnectionsPublishers[conn.RemoteAddr()] = rtmpConn
			return nil
		}); err != nil {
			if err := rtmpConn.Close(ctx); err != nil {
				logger.Errorf(ctx, "unable to close the connection from '%s': %v", conn.RemoteAddr(), err)
			}
			return fmt.Errorf("unable to store the new connection: %w", err)
		}
	}
}

func (p *ListeningPortRTMP) listenConsumers(
	ctx context.Context,
) error {
	// TODO: deduplicate with listenPublishers
	ctx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()
	ctx = belt.WithField(ctx, "rtmp_mode", RTMPModeConsumers.String())
	observability.Go(ctx, func() {
		<-ctx.Done()
		p.Listener.Close()
	})
	for {
		conn, err := p.Listener.Accept()
		if err != nil {
			return fmt.Errorf("unable to accept a connection: %w", err)
		}

		rtmpConn, err := newConnectionRTMP[*NodeOutput](ctx, p, conn)
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

type ListeningPortRTMPConfig struct {
	DefaultAppName string
}

type ListeningPortRTMPOption interface {
	apply(*ListeningPortRTMPConfig)
}

type ListeningPortRTMPOptionDefaultAppName string

func (opt ListeningPortRTMPOptionDefaultAppName) apply(cfg *ListeningPortRTMPConfig) {
	cfg.DefaultAppName = string(opt)
}
