package avd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/asticode/go-astiav"
	"github.com/facebookincubator/go-belt"
	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avcommon"
	xastiav "github.com/xaionaro-go/avcommon/astiav"
	"github.com/xaionaro-go/avd/pkg/avd/types"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/node"
	"github.com/xaionaro-go/observability"
	"github.com/xaionaro-go/secret"
	"github.com/xaionaro-go/xsync"
)

const (
	ConnectionEnableRoutePathUpdaterHack = true
)

type ConnectionProxied struct {
	Handler ConnectionProxiedHandler
	Locker  xsync.Mutex

	// access only when Locker is locked (but better don't access at all if you are not familiar with the code):
	Port         *ListeningPortProxied
	Conn         net.Conn
	CancelFunc   context.CancelFunc
	AVInputURL   *url.URL
	AVInputKey   secret.String
	AVConn       *net.TCPConn
	InitError    error
	InitFinished chan struct{}
	RoutePath    *RoutePath
}

func newConnectionProxied(
	ctx context.Context,
	p *ListeningPortProxied,
	conn net.Conn,
) (_ret *ConnectionProxied, _err error) {
	logger.Debugf(ctx, "newConnectionProxied[%s]", p.Mode)
	defer func() { logger.Debugf(ctx, "/newConnectionProxied[%s]: %v %v", p.Mode, _ret, _err) }()

	ctx, cancelFn := context.WithCancel(ctx)
	ctx = belt.WithField(ctx, "remote_addr", conn.RemoteAddr())
	ctx = belt.WithField(ctx, "port_mode", p.Mode.String())
	ctx = belt.WithField(ctx, "protocol", p.Protocol.String())
	c := &ConnectionProxied{
		Port:         p,
		Conn:         conn,
		CancelFunc:   cancelFn,
		InitFinished: make(chan struct{}),
	}
	switch p.Mode {
	case PortModePublishers:
		c.Handler = newConnectionProxiedPublisher(c)
	case types.PortModeConsumers:
		c.Handler = newConnectionProxiedConsumer(c)
	}
	defer func() {
		if _ret == nil {
			logger.Debugf(ctx, "not initialized")
			c.Close(ctx)
		}
	}()

	err := c.initAVHandler(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to handle connection from %s: %w", conn.RemoteAddr(), err)
	}

	observability.Go(ctx, func() {
		defer func() {
			logger.Debugf(ctx, "the end")
			c.Close(ctx)
		}()
		if ConnectionEnableRoutePathUpdaterHack {
			var negotiate func(context.Context) error
			switch p.Protocol {
			case ProtocolRTMP, ProtocolRTSP:
				negotiate = c.negotiate
			default:
				logger.Errorf(ctx, "negotiation for protocol '%s' is not implemented (yet?)", p.Protocol)
				return
			}
			if err := negotiate(ctx); err != nil {
				logger.Errorf(ctx, "unable to negotiate the connection with %s: %v", conn.RemoteAddr(), err)
				return
			}
		}
		if err := c.forward(ctx); err != nil {
			switch {
			case errors.Is(err, io.EOF):
				logger.Debugf(ctx, "EOF: finished the forward the connection with %s: %v", conn.RemoteAddr(), err)
			default:
				logger.Errorf(ctx, "unable to forward the connection with %s: %v", conn.RemoteAddr(), err)
			}
			return
		}
	})

	return c, nil
}

func (c *ConnectionProxied) String() string {
	ctx := context.TODO()
	if !c.Locker.ManualTryLock(ctx) {
		return fmt.Sprintf(
			"%s[%s](?->?->?->?)",
			strings.ToUpper(c.Port.Protocol.String()), c.Mode(),
		)
	}
	defer c.Locker.ManualUnlock(ctx)

	if c.Conn == nil || c.AVConn == nil {
		return fmt.Sprintf(
			"%s[%s](?->?->?->?)",
			strings.ToUpper(c.Port.Protocol.String()), c.Mode(),
		)
	}

	return fmt.Sprintf(
		"%s[%s](%s->%s->%s->%s)",
		strings.ToUpper(c.Port.Protocol.String()), c.Mode(),
		c.Conn.RemoteAddr(), c.Conn.LocalAddr(), c.AVConn.LocalAddr(), c.AVConn.RemoteAddr(),
	)
}

func (c *ConnectionProxied) Mode() PortMode {
	if c == nil {
		return UndefinedPortMode
	}
	return c.Port.Mode
}

func (c *ConnectionProxied) Close(ctx context.Context) (_err error) {
	return xsync.DoA1R1(ctx, &c.Locker, c.closeLocked, ctx)
}

func (c *ConnectionProxied) closeLocked(ctx context.Context) (_err error) {
	logger.Debugf(ctx, "closeLocked()")
	defer func() { logger.Debugf(ctx, "/closeLocked(): %v", _err) }()
	if c.CancelFunc != nil {
		c.CancelFunc()
		c.CancelFunc = nil
	}
	var errs []error
	if c.AVConn != nil {
		if err := c.AVConn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("unable to close the AVConn: %w", err))
		}
		c.AVConn = nil
	}
	if c.Conn != nil {
		if err := c.Conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("unable to close the Conn: %w", err))
		}
		c.Conn = nil
	}
	if c.Handler != nil {
		if err := c.Handler.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("unable to close the Handler: %w", err))
		}
		c.Handler = nil
	}
	return errors.Join(errs...)
}

func (c *ConnectionProxied) builtAVListenURL(
	ctx context.Context,
) (*url.URL, secret.String, error) {
	if !c.Port.Protocol.IsValid() {
		return nil, secret.New(""), fmt.Errorf("protocol is not set")
	}

	if c.Port.Protocol == ProtocolRTSP && c.Mode() == PortModeConsumers {
		return nil, secret.New(""), fmt.Errorf("AFAIK, libav does not support the server mode for RTSP")
	}

	randomPortTaker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, secret.String{}, fmt.Errorf("unable to take a random port")
	}
	randomAddr := randomPortTaker.Addr().String()
	randomPortTaker.Close()

	defaultRoutePath := c.GetRoutePath()

	logger.Debugf(ctx, "protocol: '%s", c.Port.Protocol)
	url := &url.URL{
		Scheme:   c.Port.Protocol.String(),
		Host:     randomAddr,
		Path:     fmt.Sprintf("%s/", defaultRoutePath),
		RawQuery: "",
	}
	queryWords := strings.Split(url.Path, "/")
	url.Path = strings.Join(queryWords[:len(queryWords)-1], "/")
	secretKey := secret.New(queryWords[len(queryWords)-1])

	c.AVInputURL = url
	c.AVInputKey = secretKey
	logger.Debugf(ctx, "c.AVInputURL: %#+v", c.AVInputURL)
	return url, secretKey, nil

}

func (c *ConnectionProxied) isAsyncOpen(
	ctx context.Context,
) (_ret bool) {
	logger.Debugf(ctx, "isAsyncOpen")
	defer func() { logger.Debugf(ctx, "/isAsyncOpen: %v", _ret) }()
	switch c.Port.Protocol {
	case ProtocolRTMP:
		return true
	}
	switch c.Handler.(type) {
	case *ConnectionProxiedHandlerPublisher:
		return true
	}
	return false
}

func (c *ConnectionProxied) initAVHandler(
	ctx context.Context,
) (_err error) {
	logger.Debugf(ctx, "initAVHandler")
	defer func() { logger.Debugf(ctx, "/initAVHandler: %v", _err) }()

	url, secretKey, err := c.builtAVListenURL(ctx)
	if err != nil {
		return fmt.Errorf("unable to build an URL to be listened by libav's handlers: %w", err)
	}

	host, portString, err := net.SplitHostPort(url.Host)
	if err != nil {
		return fmt.Errorf("unable to split host and port from '%s': %w", url.Host, err)
	}

	port, err := strconv.ParseUint(portString, 10, 16)
	if err != nil {
		return fmt.Errorf("unable to parse port in '%s': %w", portString, err)
	}

	avInputAddr := &net.TCPAddr{
		IP:   net.ParseIP(host),
		Port: int(port),
	}
	logger.Debugf(ctx, "avInputAddr: %#+v", avInputAddr)

	customOpts := c.Port.Config.DictionaryItems(c.Port.Protocol, c.Mode())

	logger.Debugf(ctx, "attempting to listen by libav at '%s'...", url)
	err = c.Handler.InitAVHandler(ctx, url, secretKey, customOpts...)
	if err != nil {
		return fmt.Errorf("unable to initialize the AV handler at %s: %w", url, err)
	}

	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	var connErr error
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			if connErr == nil {
				return fmt.Errorf("internal error: %w", context.DeadlineExceeded)
			}
			return connErr
		case <-t.C:
			var AVConn *net.TCPConn
			AVConn, connErr = net.DialTCP("tcp4", nil, avInputAddr)
			if connErr != nil {
				connErr = fmt.Errorf("unable to connect to the libav input '%s': %w", avInputAddr, connErr)
				logger.Tracef(ctx, "%s", connErr)
				continue
			}

			c.AVConn = AVConn
			return nil
		}
	}
}

func (c *ConnectionProxied) onInitFinished(
	ctx context.Context,
) {
	logger.Debugf(ctx, "onInitFinished")
	defer func() { logger.Debugf(ctx, "/onInitFinished") }()
	c.Locker.Do(ctx, func() {
		if c.Handler == nil {
			logger.Debugf(ctx, "the connection was already closed")
			return
		}

		switch c.Port.Protocol {
		case ProtocolRTMP:
			c.onInitFinishedRTMP(ctx)
		case ProtocolRTSP:
			c.onInitFinishedRTSP(ctx)
		default:
			logger.Errorf(ctx, "onInitFinished is not implemented for protocol '%s' (yet?)", c.Port.Protocol)
		}
		c.AVInputURL.Path = c.GetURLPath()
		c.Handler.SetURL(ctx, c.AVInputURL)
		close(c.InitFinished)
	})
}

func (c *ConnectionProxied) negotiate(
	origCtx context.Context,
) (_err error) {
	logger.Debugf(origCtx, "negotiate")
	defer func() { logger.Debugf(origCtx, "/negotiate: %v", _err) }()

	avConn, conn := c.AVConn, c.Conn

	defer func() {
		if err := avConn.SetDeadline(time.Time{}); err != nil {
			logger.Errorf(origCtx, "unable to revert the deadline for AVConn: %v", err)
		}
		if err := conn.SetDeadline(time.Time{}); err != nil {
			logger.Errorf(origCtx, "unable to revert the deadline for Conn: %v", err)
		}
	}()

	var wg sync.WaitGroup
	defer wg.Wait()

	ctx, cancelFn := context.WithCancel(origCtx)
	defer cancelFn()

	errCh := make(chan error, 2)

	forward := func(
		dst net.Conn,
		msg []byte,
	) error {
		w, err := dst.Write(msg)
		if err != nil {
			return fmt.Errorf("unable to write to the client: %w", err)
		}

		if w != len(msg) {
			return fmt.Errorf("expected to write to the client %d bytes, but wrote %d", len(msg), w)
		}

		return nil
	}

	wg.Add(1)
	observability.Go(ctx, func() {
		defer wg.Done()
		var buf [SizeBuffer]byte
		for {
			logger.Tracef(ctx, "waiting for c.AVConn input...")
			r, err := avConn.Read(buf[:])
			logger.Tracef(ctx, "/waiting for c.AVConn input: %v %v", r, err)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					logger.Debugf(ctx, "it was a deadline, ignoring")
					return
				}
				errCh <- fmt.Errorf("unable to read from the (libav-)server: %w", err)
				return
			}

			msg := buf[:r]
			logger.Tracef(ctx, "waiting for c.Conn output...")
			err = forward(conn, msg)
			logger.Tracef(ctx, "/waiting for c.Conn output")
			if err != nil {
				errCh <- err
				return
			}
		}
	})

	wg.Add(1)
	observability.Go(ctx, func() {
		defer wg.Done()
		var buf [SizeBuffer]byte
		for {
			logger.Tracef(ctx, "waiting for c.Conn input...")
			r, err := conn.Read(buf[:])
			logger.Tracef(ctx, "/waiting for c.Conn input")
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					logger.Debugf(ctx, "it was a deadline, ignoring")
					return
				}
				errCh <- fmt.Errorf("unable to read from the client: %w", err)
				return
			}

			msg := buf[:r]
			routePath, err := c.tryExtractRouteString(ctx, msg)
			if err != nil {
				errCh <- fmt.Errorf("unable to snoop the route path: %w", err)
				return
			}
			if routePath == nil {
				logger.Tracef(ctx, "waiting for c.AVConn output...")
				err := forward(avConn, msg)
				logger.Tracef(ctx, "/waiting for c.AVConn output")
				if err != nil {
					errCh <- err
					return
				}
				continue
			}

			c.RoutePath = routePath
			logger.Debugf(ctx, "routePath == '%s'", *c.RoutePath)
			if !c.isAsyncOpen(ctx) {
				c.onInitFinished(ctx)
			}

			ctx = belt.WithField(ctx, "path", *routePath)
			observability.Go(ctx, func() {
				c.serve(origCtx)
			})

			logger.Tracef(ctx, "waiting for c.AVConn output...")
			err = forward(avConn, msg)
			logger.Tracef(ctx, "/waiting for c.AVConn output")
			if err != nil {
				errCh <- err
			}

			errCh <- nil
			return
		}
	})

	interrupt := func() {
		cancelFn()
		// to interrupt reading from the sockets:
		logger.Debugf(ctx, "setting a deadline in the past for c.AVConn")
		if err := avConn.SetReadDeadline(time.Unix(1, 0)); err != nil {
			logger.Errorf(ctx, "unable to set the read deadline for AVConn: %v", err)
		}
		logger.Debugf(ctx, "setting a deadline in the past for c.Conn")
		if err := conn.SetReadDeadline(time.Unix(1, 0)); err != nil {
			logger.Errorf(ctx, "unable to set the read deadline for Conn: %v", err)
		}
	}

	select {
	case <-ctx.Done():
		interrupt()
		return ctx.Err()
	case err := <-errCh:
		interrupt()
		return err
	}
}

func (c *ConnectionProxied) serve(
	ctx context.Context,
) {
	logger.Debugf(ctx, "serve")
	defer logger.Debugf(ctx, "/serve")
	switch c := c.Handler.(type) {
	case *ConnectionProxiedHandlerPublisher:
		err := c.StartForwarding(ctx)
		if err != nil {
			logger.Errorf(ctx, "unable to start forwarding: %v", err)
			c.Close(ctx)
			return
		}
	}

	errCh := make(chan node.Error, 100)
	defer close(errCh)
	observability.Go(ctx, func() {
		for err := range errCh {
			switch {
			case errors.Is(err, context.Canceled):
				logger.Debugf(ctx, "cancelled: %v", err)
			case errors.Is(err, io.EOF):
				logger.Debugf(ctx, "EOF: %v", err)
			default:
				logger.Errorf(ctx, "got an error: %v", err)
			}
			c.Close(ctx)
		}
	})
	<-c.InitFinished
	if c.InitError != nil {
		logger.Debugf(ctx, "not running Serve, because of InitError: %v", c.InitError)
		return
	}
	if logger.FromCtx(ctx).Level() >= logger.LevelDebug {
		logger.Debugf(ctx, "resulting graph: %s", c.GetNode().(interface{ DotString(bool) string }).DotString(false))
	}

	switch c := c.Handler.(type) {
	case *ConnectionProxiedHandlerConsumer:
		err := c.StartForwarding(ctx)
		if err != nil {
			logger.Errorf(ctx, "unable to start forwarding: %v", err)
			c.Close(ctx)
			return
		}
	}
	c.GetNode().Serve(ctx, node.ServeConfig{}, errCh)
}

func (c *ConnectionProxied) tryExtractRouteString(
	ctx context.Context,
	msg []byte,
) (*RoutePath, error) {
	switch c.Port.Protocol {
	case ProtocolRTMP:
		return c.tryExtractRouteStringRTMP(ctx, msg)
	case ProtocolRTSP:
		return c.tryExtractRouteStringRTSP(ctx, msg)
	default:
		return nil, fmt.Errorf("protocol '%s' is not supported", c.Port.Protocol)
	}
}

func (c *ConnectionProxied) GetNode() node.Abstract {
	return c.Handler.GetNode()
}

func (c *ConnectionProxied) GetKernel() kernel.Abstract {
	return c.Handler.GetKernel()
}

func (c *ConnectionProxied) getFormatContext() *astiav.FormatContext {
	switch k := c.GetKernel().(type) {
	case *kernel.Input:
		return k.FormatContext
	case *kernel.Output:
		return k.FormatContext
	default:
		panic(fmt.Errorf("unexpected type: %T", k))
	}
}

func (c *ConnectionProxied) AVFormatContext() *avcommon.AVFormatContext {
	return avcommon.WrapAVFormatContext(
		xastiav.CFromAVFormatContext(
			c.getFormatContext(),
		),
	)
}

func (c *ConnectionProxied) AVURLContext() *avcommon.URLContext {
	fmtCtx := c.AVFormatContext()
	avioCtx := fmtCtx.Pb()
	if avioCtx == nil {
		panic("internal error: avioCtx == nil")
	}
	return avcommon.WrapURLContext(avioCtx.Opaque())
}

func (c *ConnectionProxied) GetRoutePath() RoutePath {
	if c.RoutePath != nil {
		return *c.RoutePath
	}

	if c.Port.Config.DefaultRoutePath != "" {
		return c.Port.Config.DefaultRoutePath
	}

	return "avd-input"
}

func (c *ConnectionProxied) GetURLPath() string {
	routePath := c.GetRoutePath()
	switch c.Port.Protocol {
	case ProtocolRTMP:
		return string(routePath) + "/"
	case ProtocolRTSP:
		return string(routePath)
	default:
		panic(fmt.Errorf("unsupported protocol: %s", c.Port.Protocol))
	}
}

func (c *ConnectionProxied) forward(
	ctx context.Context,
) (_err error) {
	logger.Debugf(ctx, "forward")
	defer func() { logger.Debugf(ctx, "/forward: %v", _err) }()

	avConn, conn := c.AVConn, c.Conn

	defer func() {
		if err := avConn.SetDeadline(time.Time{}); err != nil {
			logger.Errorf(ctx, "unable to revert the deadline for AVConn: %v", err)
		}
		if err := conn.SetDeadline(time.Time{}); err != nil {
			logger.Errorf(ctx, "unable to revert the deadline for Conn: %v", err)
		}
	}()

	var wg sync.WaitGroup
	defer wg.Wait()

	ctx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	errCh := make(chan error, 2)

	forward := func(
		dst net.Conn,
		msg []byte,
	) error {
		w, err := dst.Write(msg)
		if err != nil {
			return fmt.Errorf("unable to write to the client: %w", err)
		}

		if w != len(msg) {
			return fmt.Errorf("expected to write to the client %d bytes, but wrote %d", len(msg), w)
		}

		return nil
	}

	wg.Add(1)
	observability.Go(ctx, func() {
		defer wg.Done()
		var buf [SizeBuffer]byte
		for {
			logger.Tracef(ctx, "waiting for AVConn input...")
			r, err := avConn.Read(buf[:])
			logger.Tracef(ctx, "/waiting for AVConn input")
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					logger.Debugf(ctx, "it was a deadline, ignoring")
					return
				}
				errCh <- fmt.Errorf("unable to read from the (libav-)server: %w", err)
				return
			}

			msg := buf[:r]
			logger.Tracef(ctx, "waiting for c.Conn output...")
			err = forward(conn, msg)
			logger.Tracef(ctx, "/waiting for c.Conn output")
			if err != nil {
				errCh <- err
				return
			}
		}
	})

	wg.Add(1)
	observability.Go(ctx, func() {
		defer wg.Done()
		var buf [SizeBuffer]byte
		for {
			logger.Tracef(ctx, "waiting for c.Conn input...")
			r, err := conn.Read(buf[:])
			logger.Tracef(ctx, "/waiting for c.Conn input")
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					logger.Debugf(ctx, "it was a deadline, ignoring")
					return
				}
				errCh <- fmt.Errorf("unable to read from the client: %w", err)
				return
			}

			msg := buf[:r]
			logger.Tracef(ctx, "waiting for c.AVConn output...")
			err = forward(avConn, msg)
			logger.Tracef(ctx, "/waiting for c.AVConn output")
			if err != nil {
				errCh <- err
				return
			}
		}
	})

	interrupt := func() {
		cancelFn()
		// to interrupt reading from the sockets:
		logger.Debugf(ctx, "setting a deadline in the past for c.AVConn")
		if err := avConn.SetReadDeadline(time.Unix(1, 0)); err != nil {
			logger.Errorf(ctx, "unable to set the read deadline for AVConn: %v", err)
		}
		logger.Debugf(ctx, "setting a deadline in the past for c.Conn")
		if err := conn.SetReadDeadline(time.Unix(1, 0)); err != nil {
			logger.Errorf(ctx, "unable to set the read deadline for Conn: %v", err)
		}
	}

	select {
	case <-ctx.Done():
		interrupt()
		return ctx.Err()
	case err := <-errCh:
		interrupt()
		return err
	}
}
