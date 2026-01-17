// listening_port_proxied.go implements a proxied listening port.

package avd

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/url"
	"slices"

	"github.com/facebookincubator/go-belt"
	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avd/pkg/avd/types"
	"github.com/xaionaro-go/observability"
	"github.com/xaionaro-go/xsync"
)

type ListeningPortProxied struct {
	Server            *Server
	Listener          net.Listener
	Protocol          Protocol
	Mode              PortMode
	ConnectionsLocker xsync.Mutex
	Connections       map[net.Addr]*ConnectionProxied
	Config            ListenConfig
	CancelFn          context.CancelFunc
}

func (s *Server) ListenProxied(
	ctx context.Context,
	listener net.Listener,
	protocol Protocol,
	mode types.StreamingPortMode,
	opts ...ListenOption,
) (_ret *ListeningPortProxied, _err error) {
	logger.Debugf(ctx, "ListenProxied(ctx, '%s', %s, %s, %#+v)", listener.Addr(), protocol, mode, opts)
	defer func() {
		logger.Debugf(ctx, "/ListenProxied(ctx, '%s', %s, %s, %#+v): %v %v", listener.Addr(), protocol, mode, opts, _ret, _err)
	}()
	ctx = belt.WithField(ctx, "listener", listener.Addr().String())
	ctx = belt.WithField(ctx, "proto", protocol)
	ctx = belt.WithField(ctx, "port_mode", mode)

	defer func() {
		if _ret != nil {
			s.addListeningPort(ctx, _ret)
		}
	}()

	cfg := ListenOptions(opts).Config()
	logger.Debugf(ctx, "ListenProxied config: %#+v", cfg)
	result := &ListeningPortProxied{
		Server:      s,
		Listener:    listener,
		Protocol:    protocol,
		Mode:        mode,
		Config:      cfg,
		Connections: make(map[net.Addr]*ConnectionProxied),
	}

	err := result.startListening(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to start listening: %w", err)
	}

	return result, nil
}

func (p *ListeningPortProxied) String() string {
	return fmt.Sprintf("%s[%s](%s)", p.Protocol, p.Mode, p.Listener.Addr())
}

func (p *ListeningPortProxied) GetMode() PortMode {
	return p.Mode
}

func (p *ListeningPortProxied) GetConnections(ctx context.Context) Connections {
	var result Connections
	for _, conn := range p.getConnections(ctx) {
		result = append(result, conn)
	}
	return result
}

func (p *ListeningPortProxied) getConnections(ctx context.Context) []*ConnectionProxied {
	return xsync.DoA1R1(ctx, &p.ConnectionsLocker, p.getConnectionsNoLock, ctx)
}

func (p *ListeningPortProxied) getConnectionsNoLock(ctx context.Context) []*ConnectionProxied {
	return slices.Collect(maps.Values(p.Connections))
}

func (p *ListeningPortProxied) startListening(
	ctx context.Context,
) (_err error) {
	if p.CancelFn != nil {
		return fmt.Errorf("the port was already started")
	}
	ctx, cancelFn := context.WithCancel(ctx)
	p.CancelFn = cancelFn
	logger.Debugf(ctx, "startListening")
	defer func() { logger.Debugf(ctx, "/startListening: %v", _err) }()

	observability.Go(ctx, func(ctx context.Context) {
		err := p.listen(ctx)
		if err == nil {
			return
		}
		if errors.Is(err, net.ErrClosed) {
			logger.Debugf(ctx, "listener %s closed: %v", p.Listener.Addr(), err)
		} else {
			logger.Errorf(ctx, "unable to listen %s: %v", p.Listener.Addr(), err)
		}
		err = p.Close(ctx)
		if err != nil {
			logger.Debugf(ctx, "p.Close() result: %v", err)
		}
	})

	return nil
}

func (p *ListeningPortProxied) GetServer() *Server {
	return p.Server
}

func (p *ListeningPortProxied) GetConfig() ListenConfig {
	return p.Config
}

func (p *ListeningPortProxied) Close(ctx context.Context) (_err error) {
	logger.Debugf(ctx, "Close")
	defer func() { logger.Debugf(ctx, "/Close: %v", _err) }()
	p.CancelFn()
	err := p.Listener.Close()
	logger.Debugf(ctx, "p.Listener.Close() result: %v", err)
	var conns []*ConnectionProxied
	p.ConnectionsLocker.Do(ctx, func() {
		for _, conn := range p.Connections {
			conns = append(conns, conn)
		}
	})
	for _, conn := range conns {
		err := conn.Close(ctx)
		logger.Debugf(ctx, "conn[%s].Close(ctx) result: %v", conn, err)
	}
	p.Server.removeListeningPort(ctx, p)
	return nil
}

func (p *ListeningPortProxied) GetURLForRoute(
	ctx context.Context,
	route string,
) (_ret *url.URL, _err error) {
	logger.Debugf(ctx, "GetURLForRoute")
	defer func() { logger.Debugf(ctx, "/GetURLForRoute: %v %v", _ret, _err) }()
	u := &url.URL{
		Scheme: p.Protocol.String(),
		Host:   p.Listener.Addr().String(),
	}
	switch p.Protocol {
	case ProtocolSRT:
		u.RawQuery = "streamid=" + url.QueryEscape("/"+route)
	default:
		u.Path = route
	}
	return u, nil
}

func (p *ListeningPortProxied) listen(
	ctx context.Context,
) (_err error) {
	logger.Debugf(ctx, "listen")
	defer func() { logger.Debugf(ctx, "/listen: %v", _err) }()

	ctx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()
	observability.Go(ctx, func(ctx context.Context) {
		<-ctx.Done()
		err := p.Listener.Close()
		if err != nil {
			logger.Debugf(ctx, "p.Listener.Close() result: %v", err)
		}
	})
	for {
		netConn, err := p.Listener.Accept()
		if err != nil {
			return fmt.Errorf("unable to accept a connection: %w", err)
		}

		observability.Go(ctx, func(ctx context.Context) {
			err := p.addConnection(ctx, netConn)
			if err != nil {
				logger.Errorf(ctx, "unable to handle a new connection: %v", err)
			}
		})
	}
}

func (p *ListeningPortProxied) addConnection(
	ctx context.Context,
	netConn net.Conn,
) (_err error) {
	logger.Debugf(ctx, "addConnection(ctx, %s)", netConn.RemoteAddr())
	defer func() { logger.Debugf(ctx, "/addConnection(ctx, %s): %v", netConn.RemoteAddr(), _err) }()

	conn, err := newConnectionProxied(ctx, p, netConn)
	if err != nil {
		return fmt.Errorf("unable to initialize a connection for '%s': %w", netConn.RemoteAddr(), err)
	}

	if err := xsync.DoR1(ctx, &p.ConnectionsLocker, func() error {
		if oldConn, ok := p.Connections[netConn.RemoteAddr()]; ok {
			logger.Errorf(ctx, "there is already a connection from '%s', closing the old one", netConn.RemoteAddr())
			if err := oldConn.Close(ctx); err != nil {
				logger.Errorf(ctx, "unable to close the old connection from '%s': %v", netConn.RemoteAddr(), err)
			}
		}
		p.Connections[netConn.RemoteAddr()] = conn
		return nil
	}); err != nil {
		if err := conn.Close(ctx); err != nil {
			logger.Errorf(ctx, "unable to close the connection from '%s': %v", netConn.RemoteAddr(), err)
		}
		return fmt.Errorf("unable to store the new connection: %w", err)
	}

	return nil
}

func (p *ListeningPortProxied) removeConnection(
	ctx context.Context,
	conn *ConnectionProxied,
) (_err error) {
	logger.Debugf(ctx, "removeConnection(ctx, %s)", conn.String())
	defer func() { logger.Debugf(ctx, "/removeConnection(ctx, %s): %v", conn.String(), _err) }()

	if conn == nil {
		return fmt.Errorf("the connection is nil")
	}

	netConn := conn.GetRawConn(ctx)
	if netConn == nil {
		return fmt.Errorf("GetRawConn is nil for the connection '%s'", conn.String())
	}

	remoteAddr := netConn.RemoteAddr()
	p.ConnectionsLocker.Do(ctx, func() {
		oldConn := p.Connections[remoteAddr]
		if oldConn == conn {
			delete(p.Connections, remoteAddr)
		}
	})
	return nil
}
