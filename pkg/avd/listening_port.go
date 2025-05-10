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

type ListeningPort struct {
	Server                *Server
	Listener              net.Listener
	Protocol              Protocol
	ConnectionsLocker     xsync.Mutex
	ConnectionsPublishers map[net.Addr]*Connection[*NodeInput]
	ConnectionsConsumers  map[net.Addr]*Connection[*NodeOutput]
	Config                ListenConfig
	CancelFn              context.CancelFunc
}

func (p *ListeningPort) startListening(
	ctx context.Context,
	mode types.PortMode,
) (_err error) {
	ctx = belt.WithField(ctx, "port_mode", mode.String())
	if p.CancelFn != nil {
		return fmt.Errorf("the port was already started")
	}
	ctx, cancelFn := context.WithCancel(ctx)
	p.CancelFn = cancelFn
	logger.Debugf(ctx, "StartListening")
	defer func() { logger.Debugf(ctx, "/StartListening: %v", _err) }()

	var addNewConnFunc func(context.Context, net.Conn) error
	switch mode {
	case PortModePublishers:
		addNewConnFunc = p.addInputConnection
	case PortModeConsumers:
		addNewConnFunc = p.addOutputConnection
	default:
		return fmt.Errorf("unknown port mode '%s'", mode)
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

func (p *ListeningPort) GetServer() *Server {
	return p.Server
}

func (p *ListeningPort) GetConfig() ListenConfig {
	return p.Config
}

func (p *ListeningPort) Close(ctx context.Context) (_err error) {
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

func (p *ListeningPort) GetURLForRoute(
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

func (p *ListeningPort) listen(
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
				logger.Error(ctx, "unable to handle a new connection: %v", err)
			}
		})
	}
}

func (p *ListeningPort) addInputConnection(
	ctx context.Context,
	netConn net.Conn,
) error {
	conn, err := newConnection[*NodeInput](ctx, p, netConn)
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

func (p *ListeningPort) addOutputConnection(
	ctx context.Context,
	netConn net.Conn,
) error {
	conn, err := newConnection[*NodeOutput](ctx, p, netConn)
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
