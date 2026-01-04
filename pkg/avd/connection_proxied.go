// connection_proxied.go implements a proxied connection that uses libav for protocol handling.

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
	"github.com/go-ng/xatomic"
	"github.com/xaionaro-go/avcommon"
	xastiav "github.com/xaionaro-go/avcommon/astiav"
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
	Handler *ConnectionProxiedHandler
	Locker  xsync.Mutex

	// access only when Locker is locked (but better don't access at all if you are not familiar with the code):
	Port             *ListeningPortProxied
	Conn             *net.Conn
	CancelFunc       context.CancelFunc
	AVInputURL       *url.URL
	AVInputKey       secret.String
	AVConn           *net.TCPConn
	InitError        error
	InitFinished     chan struct{}
	InitFinishedOnce sync.Once
	RoutePath        *RoutePath
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
		CancelFunc:   cancelFn,
		InitFinished: make(chan struct{}),
	}
	c.SetConn(conn)
	switch p.Mode {
	case PortModePublishers:
		c.SetHandler(newConnectionProxiedPublisher(c))
	case PortModeConsumers:
		c.SetHandler(newConnectionProxiedConsumer(c))
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

	observability.Go(ctx, func(ctx context.Context) {
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
			err = negotiate(ctx)
			if err != nil {
				logger.Errorf(ctx, "unable to negotiate the connection with %s: %v", conn.RemoteAddr(), err)
				return
			}
		}
		err = c.forward(ctx)
		if err != nil {
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

func (c *ConnectionProxied) SetHandler(
	handler ConnectionProxiedHandler,
) {
	xatomic.StorePointer(&c.Handler, &handler)
}

func (c *ConnectionProxied) GetHandler() ConnectionProxiedHandler {
	if c == nil {
		return nil
	}
	handler := xatomic.LoadPointer(&c.Handler)
	if handler == nil {
		return nil
	}
	return *handler
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

	conn := c.GetConn()
	avConn := c.GetAVConn()
	return fmt.Sprintf(
		"%s[%s](%s->%s->%s->%s)",
		strings.ToUpper(c.Port.Protocol.String()), c.Mode(),
		conn.RemoteAddr(), conn.LocalAddr(), avConn.LocalAddr(), avConn.RemoteAddr(),
	)
}

func (c *ConnectionProxied) GetRawConn(context.Context) net.Conn {
	return c.GetConn()
}

func (c *ConnectionProxied) Mode() PortMode {
	if c == nil {
		return UndefinedPortMode
	}
	return c.Port.Mode
}

func (c *ConnectionProxied) closeInitFinished() {
	c.InitFinishedOnce.Do(func() {
		close(c.InitFinished)
	})
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

	c.closeInitFinished()

	var errs []error
	if handler := c.GetHandler(); handler != nil {
		if err := handler.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("unable to close the Handler: %w", err))
		}
		c.SetHandler(nil)
	}
	if port := c.GetPort(); port != nil {
		if err := port.removeConnection(ctx, c); err != nil {
			errs = append(errs, fmt.Errorf("unable to remove myself from the listening port: %w", err))
		}
		c.SetPort(nil)
	}
	if avConn := c.GetAVConn(); avConn != nil {
		if err := avConn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("unable to close the AVConn: %w", err))
		}
		c.SetAVConn(nil)
	}
	if conn := c.GetConn(); conn != nil {
		if err := conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("unable to close the Conn: %w", err))
		}
		c.SetConn(nil)
	}
	return errors.Join(errs...)
}

func (c *ConnectionProxied) GetPort() *ListeningPortProxied {
	if c == nil {
		return nil
	}
	return xatomic.LoadPointer(&c.Port)
}

func (c *ConnectionProxied) SetPort(port *ListeningPortProxied) {
	xatomic.StorePointer(&c.Port, port)
}

func (c *ConnectionProxied) GetAVConn() *net.TCPConn {
	if c == nil {
		return nil
	}
	return xatomic.LoadPointer(&c.AVConn)
}

func (c *ConnectionProxied) SetAVConn(avConn *net.TCPConn) {
	xatomic.StorePointer(&c.AVConn, avConn)
}

func (c *ConnectionProxied) GetConn() net.Conn {
	if c == nil {
		return nil
	}
	return *xatomic.LoadPointer(&c.Conn)
}

func (c *ConnectionProxied) SetConn(conn net.Conn) {
	xatomic.StorePointer(&c.Conn, &conn)
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
	port := c.GetPort()
	if port == nil {
		logger.Debugf(ctx, "isAsyncOpen: Port is nil")
		return false
	}
	switch port.Protocol {
	case ProtocolRTMP:
		return true
	}
	handler := c.GetHandler()
	if handler == nil {
		logger.Debugf(ctx, "isAsyncOpen: Handler is nil")
		return false
	}
	switch handler.(type) {
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

	logger.Debugf(ctx, "attempting to listen by libav at '%s'...", url)
	handler := c.GetHandler()
	err = handler.InitAVHandler(ctx, c.Port.Protocol, url, secretKey, c.Port.Config)
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
	handler := c.GetHandler()
	if handler == nil {
		logger.Debugf(ctx, "the connection was already closed")
		return
	}
	port := c.GetPort()
	if port == nil {
		logger.Debugf(ctx, "the port was already closed")
		return
	}

	switch port.Protocol {
	case ProtocolRTMP:
		c.onInitFinishedRTMP(ctx)
	case ProtocolRTSP:
		c.onInitFinishedRTSP(ctx)
	default:
		logger.Errorf(ctx, "onInitFinished is not implemented for protocol '%s' (yet?)", port.Protocol)
	}
	c.AVInputURL.Path = c.GetURLPath()
	c.GetHandler().SetURL(ctx, c.AVInputURL)
	c.closeInitFinished()
}

func (c *ConnectionProxied) negotiate(
	origCtx context.Context,
) (_err error) {
	logger.Debugf(origCtx, "negotiate")
	defer func() { logger.Debugf(origCtx, "/negotiate: %v", _err) }()

	avConn, conn := c.GetAVConn(), c.GetConn()
	if avConn == nil {
		return fmt.Errorf("avConn is nil")
	}
	if conn == nil {
		return fmt.Errorf("conn is nil")
	}

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
	observability.Go(ctx, func(ctx context.Context) {
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
	observability.Go(ctx, func(ctx context.Context) {
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
			observability.Go(ctx, func(ctx context.Context) {
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
	handler := c.GetHandler()
	if handler == nil {
		logger.Debugf(ctx, "not running Serve, because Handler == nil")
		return
	}
	switch c := handler.(type) {
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
	observability.Go(ctx, func(ctx context.Context) {
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

	n := handler.GetNode()
	if n == nil {
		logger.Debugf(ctx, "not running Serve, because Node == nil")
		return
	}

	if logger.FromCtx(ctx).Level() >= logger.LevelDebug {
		logger.Debugf(ctx, "resulting graph: %s", n.(interface{ DotString(bool) string }).DotString(false))
	}

	switch c := handler.(type) {
	case *ConnectionProxiedHandlerConsumer:
		err := c.StartForwarding(ctx)
		if err != nil {
			logger.Errorf(ctx, "unable to start forwarding: %v", err)
			c.Close(ctx)
			return
		}
	}
	n.Serve(ctx, node.ServeConfig{DebugData: c}, errCh)
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

func (c *ConnectionProxied) GetNode(ctx context.Context) node.Abstract {
	handler := c.GetHandler()
	if handler == nil {
		return nil
	}
	return handler.GetNode()
}

func (c *ConnectionProxied) GetKernel() kernel.Abstract {
	handler := c.GetHandler()
	if handler == nil {
		return nil
	}
	return handler.GetKernel()
}

func (c *ConnectionProxied) getFormatContext(ctx context.Context) *astiav.FormatContext {
	k := c.GetKernel()
	if k == nil {
		logger.Errorf(ctx, "getFormatContext: Kernel is nil, unable to get FormatContext")
		return nil
	}
	switch k := k.(type) {
	case *kernel.Input:
		return k.FormatContext
	case *kernel.Output:
		return k.FormatContext
	case *kernel.ChainOfTwo[*kernel.ReorderMonotonicDTS, *kernel.Output]:
		return k.Kernel1.FormatContext
	default:
		panic(fmt.Errorf("unexpected type: %T", k))
	}
}

func (c *ConnectionProxied) AVFormatContext(ctx context.Context) *avcommon.AVFormatContext {
	fmtCtx := c.getFormatContext(ctx)
	if fmtCtx == nil {
		logger.Errorf(ctx, "AVFormatContext is nil, unable to wrap it")
		return nil
	}
	return avcommon.WrapAVFormatContext(
		xastiav.CFromAVFormatContext(
			fmtCtx,
		),
	)
}

func (c *ConnectionProxied) AVURLContext(ctx context.Context) *avcommon.URLContext {
	fmtCtx := c.AVFormatContext(ctx)
	if fmtCtx == nil {
		return nil
	}
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

	avConn, conn := c.GetAVConn(), c.GetConn()
	if avConn == nil {
		return fmt.Errorf("avConn is nil")
	}
	if conn == nil {
		return fmt.Errorf("conn is nil")
	}

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
	observability.Go(ctx, func(ctx context.Context) {
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
	observability.Go(ctx, func(ctx context.Context) {
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
