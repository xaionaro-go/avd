package avd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"

	"github.com/facebookincubator/go-belt"
	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avd/pkg/avd/types"
	"github.com/xaionaro-go/observability"
	"github.com/xaionaro-go/xsync"
)

type ListeningPortProxied struct {
	Server                *Server
	Listener              net.Listener
	Protocol              Protocol
	Mode                  PortMode
	ConnectionsLocker     xsync.Mutex
	ConnectionsPublishers map[net.Addr]*ConnectionProxiedPublisher
	ConnectionsConsumers  map[net.Addr]*ConnectionProxiedConsumer
	Config                ListenConfig
	CancelFn              context.CancelFunc
}

func (s *Server) ListenProxied(
	ctx context.Context,
	listener net.Listener,
	protocol Protocol,
	mode types.PortMode,
	opts ...ListenOption,
) (_ret *ListeningPortProxied, _err error) {
	logger.Debugf(ctx, "ListenProxied(ctx, '%s')", listener.Addr())
	defer func() { logger.Debugf(ctx, "/ListenProxied(ctx, '%s'): %v %v", listener.Addr(), _ret, _err) }()
	ctx = belt.WithField(ctx, "listener", listener.Addr().String())
	ctx = belt.WithField(ctx, "proto", protocol)
	ctx = belt.WithField(ctx, "port_mode", mode)

	cfg := ListenOptions(opts).Config()
	result := &ListeningPortProxied{
		Server:                s,
		Listener:              listener,
		Protocol:              protocol,
		Mode:                  mode,
		Config:                cfg,
		ConnectionsPublishers: make(map[net.Addr]*ConnectionProxiedPublisher),
		ConnectionsConsumers:  make(map[net.Addr]*ConnectionProxiedConsumer),
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

	var addNewConnFunc func(context.Context, net.Conn) error
	switch p.Mode {
	case PortModePublishers:
		addNewConnFunc = p.addInputConnection
	case PortModeConsumers:
		addNewConnFunc = p.addOutputConnection
	default:
		return fmt.Errorf("unknown port mode '%s'", p.Mode)
	}

	observability.Go(ctx, func() {
		err := p.listen(ctx, addNewConnFunc)
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
	p.ConnectionsLocker.Do(ctx, func() {
		for addr, conn := range p.ConnectionsPublishers {
			err := conn.Close(ctx)
			logger.Debugf(ctx, "conn[%s].Close(ctx) result: %v", addr, err)
			delete(p.ConnectionsPublishers, addr)
		}
		for addr, conn := range p.ConnectionsConsumers {
			err := conn.Close(ctx)
			logger.Debugf(ctx, "conn[%s].Close(ctx) result: %v", addr, err)
			delete(p.ConnectionsConsumers, addr)
		}
	})
	return nil
}

func (p *ListeningPortProxied) GetURLForRoute(
	ctx context.Context,
	route string,
) (_ret *url.URL, _err error) {
	logger.Debugf(ctx, "GetURLForRoute")
	defer func() { logger.Debugf(ctx, "/GetURLForRoute: %v %v", _ret, _err) }()
	return &url.URL{
		Scheme: p.Protocol.String(),
		Host:   p.Listener.Addr().String(),
		Path:   route,
	}, nil
}

func (p *ListeningPortProxied) listen(
	ctx context.Context,
	addConnectionFunc func(context.Context, net.Conn) error,
) (_err error) {
	logger.Debugf(ctx, "listen")
	defer func() { logger.Debugf(ctx, "/listen: %v", _err) }()

	ctx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()
	observability.Go(ctx, func() {
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

		observability.Go(ctx, func() {
			err := addConnectionFunc(ctx, netConn)
			if err != nil {
				logger.Errorf(ctx, "unable to handle a new connection: %v", err)
			}
		})
	}
}

func (p *ListeningPortProxied) addInputConnection(
	ctx context.Context,
	netConn net.Conn,
) error {
	conn, err := newConnectionProxiedPublisher(ctx, p, netConn)
	if err != nil {
		return fmt.Errorf("unable to initialize a connection for '%s': %w", netConn.RemoteAddr(), err)
	}

	if err := xsync.DoR1(ctx, &p.ConnectionsLocker, func() error {
		if oldConn, ok := p.ConnectionsPublishers[netConn.RemoteAddr()]; ok {
			logger.Errorf(ctx, "there is already a connection from '%s', closing the old one", netConn.RemoteAddr())
			if err := oldConn.Close(ctx); err != nil {
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

	return nil
}

func (p *ListeningPortProxied) addOutputConnection(
	ctx context.Context,
	netConn net.Conn,
) error {
	conn, err := newConnectionProxiedConsumer(ctx, p, netConn)
	if err != nil {
		return fmt.Errorf("unable to initialize a connection for '%s': %w", netConn.RemoteAddr(), err)
	}

	if err := xsync.DoR1(ctx, &p.ConnectionsLocker, func() error {
		if oldConn, ok := p.ConnectionsConsumers[netConn.RemoteAddr()]; ok {
			logger.Errorf(ctx, "there is already a connection from '%s', closing the old one", netConn.RemoteAddr())
			if err := oldConn.Close(ctx); err != nil {
				logger.Errorf(ctx, "unable to close the old connection from '%s': %v", netConn.RemoteAddr(), err)
			}
		}
		p.ConnectionsConsumers[netConn.RemoteAddr()] = conn
		return nil
	}); err != nil {
		if err := conn.Close(ctx); err != nil {
			logger.Errorf(ctx, "unable to close the connection from '%s': %v", netConn.RemoteAddr(), err)
		}
		return fmt.Errorf("unable to store the new connection: %w", err)
	}

	return nil
}
