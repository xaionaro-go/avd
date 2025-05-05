package avd

import (
	"context"
	"fmt"
	"net"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/observability"
	"github.com/xaionaro-go/xsync"
)

type ListeningPortRTMP struct {
	Server            *Server
	Listener          net.Listener
	ConnectionsLocker xsync.Mutex
	Connections       map[net.Addr]*ConnectionRTMP
}

func (p *ListeningPortRTMP) StartListening(ctx context.Context) error {
	observability.Go(ctx, func() {
		err := p.Listen(ctx)
		if err != nil {
			logger.Errorf(ctx, "unable to listen %s: %v", p.Listener.Addr(), err)
			p.Close(ctx)
		}
	})
	return nil
}

func (p *ListeningPortRTMP) Close(ctx context.Context) error {
	p.Listener.Close()
	return nil
}

func (p *ListeningPortRTMP) Listen(ctx context.Context) error {
	ctx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()
	observability.Go(ctx, func() {
		<-ctx.Done()
		p.Listener.Close()
	})
	for {
		conn, err := p.Listener.Accept()
		if err != nil {
			return fmt.Errorf("unable to accept a connection: %w", err)
		}

		rtmpConn, err := newConnectionRTMP(ctx, p, conn)
		if err != nil {
			return fmt.Errorf("unable to initialize a connection for '%s': %w", conn.RemoteAddr(), err)
		}

		if err := xsync.DoR1(ctx, &p.ConnectionsLocker, func() error {
			if oldRTMPConn, ok := p.Connections[conn.RemoteAddr()]; ok {
				logger.Errorf(ctx, "there is already a connection from '%s', closing the old one", conn.RemoteAddr())
				if err := oldRTMPConn.Close(ctx); err != nil {
					logger.Errorf(ctx, "unable to close the old connection from '%s': %v", conn.RemoteAddr(), err)
				}
			}
			p.Connections[conn.RemoteAddr()] = rtmpConn
			return nil
		}); err != nil {
			if err := rtmpConn.Close(ctx); err != nil {
				logger.Errorf(ctx, "unable to close the connection from '%s': %v", conn.RemoteAddr(), err)
			}
			return fmt.Errorf("unable to store the new connection: %w", err)
		}
	}
}
