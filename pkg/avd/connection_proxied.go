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

const (
	initAVHandlerDialRetryTimeout = 10 * time.Second
	initAVHandlerPortRetryCount   = 4
)

type ConnectionProxied struct {
	Handler *ConnectionProxiedHandler
	Locker  xsync.Mutex

	Port             *ListeningPortProxied
	ProxyConn        xatomic.Value[net.Conn]
	CancelFunc       context.CancelFunc
	AVInputURL       *url.URL
	AVInputKey       secret.String
	AVConn           xatomic.Value[net.Conn]
	InitError        error
	InitFinished     chan struct{}
	InitFinishedOnce sync.Once
	RoutePath        *RoutePath

	ProtocolProperties map[string]any
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
		Port:               p,
		CancelFunc:         cancelFn,
		InitFinished:       make(chan struct{}),
		ProtocolProperties: make(map[string]any),
	}
	c.SetProxyConn(conn)
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

	if ConnectionEnableRoutePathUpdaterHack {
		switch p.Protocol {
		case ProtocolRTMP, ProtocolRTSP, ProtocolSRT:
		default:
			return nil, fmt.Errorf("negotiation for protocol '%s' is not implemented (yet?)", p.Protocol)
		}
	}

	observability.Go(ctx, func(ctx context.Context) {
		logger.Tracef(ctx, "newConnectionProxied: forwarder started")
		defer func() {
			logger.Debugf(ctx, "newConnectionProxied: forwarder ended")
			c.Close(ctx)
		}()

		// Start the AVConn→Conn reader once at the connection level so it
		// runs continuously across negotiate and forward phases. This
		// eliminates the gap where RTMP server responses (e.g. onStatus
		// for publish) were lost between negotiate's reader being
		// interrupted and forward's reader starting.
		avConnErrCh := make(chan error, 1)
		observability.Go(ctx, func(ctx context.Context) {
			logger.Tracef(ctx, "newConnectionProxied: avConn→conn reader started")
			defer logger.Tracef(ctx, "newConnectionProxied: avConn→conn reader ended")
			var buf [SizeBuffer]byte
			for {
				avConn := c.GetAVConn()
				proxyConn := c.GetProxyConn()
				if avConn == nil || proxyConn == nil {
					avConnErrCh <- fmt.Errorf("connection closed")
					return
				}

				logger.Tracef(ctx, "waiting for AVConn input...")
				r, err := avConn.Read(buf[:])
				logger.Tracef(ctx, "/waiting for AVConn input: %d %v", r, err)
				if err != nil {
					avConnErrCh <- fmt.Errorf("unable to read from the (libav-)server: %w", err)
					return
				}

				msg := buf[:r]
				logger.Tracef(ctx, "waiting for Conn output...")
				w, writeErr := proxyConn.Write(msg)
				logger.Tracef(ctx, "/waiting for Conn output: %d %v", w, writeErr)
				if writeErr != nil {
					avConnErrCh <- fmt.Errorf("unable to write to the client: %w", writeErr)
					return
				}
				if w != len(msg) {
					avConnErrCh <- fmt.Errorf("expected to write %d bytes, but wrote %d", len(msg), w)
					return
				}
			}
		})

		if ConnectionEnableRoutePathUpdaterHack {
			logger.Tracef(ctx, "newConnectionProxied: p.Protocol == %d (%s)", p.Protocol, p.Protocol.String())
			err := c.negotiate(ctx)
			if err != nil {
				logger.Errorf(ctx, "unable to negotiate the connection with %s: %v", conn.RemoteAddr(), err)
				return
			}
		}
		err := c.forward(ctx, avConnErrCh)
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
	if c == nil {
		return "nil"
	}
	ctx := context.TODO()
	protocol := "?"
	if c.Port != nil {
		protocol = strings.ToUpper(c.Port.Protocol.String())
	}
	if !c.Locker.ManualTryLock(ctx) {
		return fmt.Sprintf(
			"%s[%s](?->?->?->?)",
			protocol, c.Mode(),
		)
	}
	defer c.Locker.ManualUnlock(ctx)

	conn, avConn := c.GetProxyConn(), c.GetAVConn()
	var connRemote, connLocal, avConnLocal, avConnRemote any = "?", "?", "?", "?"
	if conn != nil {
		connRemote, connLocal = conn.RemoteAddr(), conn.LocalAddr()
	}
	if avConn != nil {
		avConnLocal, avConnRemote = avConn.LocalAddr(), avConn.RemoteAddr()
	}
	return fmt.Sprintf(
		"%s[%s](%v->%v->%v->%v)",
		protocol, c.Mode(),
		connRemote, connLocal, avConnLocal, avConnRemote,
	)
}

func (c *ConnectionProxied) GetRawConn(context.Context) net.Conn {
	return c.GetProxyConn()
}

func (c *ConnectionProxied) Mode() PortMode {
	if c == nil || c.Port == nil {
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
	if conn := c.GetProxyConn(); conn != nil {
		if err := conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("unable to close the Conn: %w", err))
		}
		c.SetProxyConn(nil)
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

func (c *ConnectionProxied) GetProxyConn() net.Conn {
	if c == nil {
		return nil
	}
	return c.ProxyConn.Load()
}

func (c *ConnectionProxied) SetProxyConn(conn net.Conn) {
	c.ProxyConn.Store(conn)
}

func (c *ConnectionProxied) GetAVConn() net.Conn {
	if c == nil {
		return nil
	}
	return c.AVConn.Load()
}

func (c *ConnectionProxied) SetAVConn(conn net.Conn) {
	c.AVConn.Store(conn)
}

func (c *ConnectionProxied) getSocketNetworkName() string {
	switch c.Port.Protocol {
	case ProtocolSRT:
		return "udp"
	default:
		return "tcp"
	}
}

func (c *ConnectionProxied) builtAVListenURL(
	ctx context.Context,
) (*url.URL, secret.String, error) {
	if !c.Port.Protocol.IsValid() {
		return nil, secret.New(""), fmt.Errorf("protocol is not set")
	}

	var randomAddr net.Addr
	switch c.getSocketNetworkName() {
	case "udp":
		randomPortTaker, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			return nil, secret.String{}, fmt.Errorf("unable to take a random port: %w", err)
		}
		randomAddr = randomPortTaker.LocalAddr()
		if err := randomPortTaker.Close(); err != nil {
			return nil, secret.String{}, fmt.Errorf("unable to release random port: %w", err)
		}
	default:
		randomPortTaker, err := net.Listen(c.getSocketNetworkName(), "127.0.0.1:0")
		if err != nil {
			return nil, secret.String{}, fmt.Errorf("unable to take a random port: %w", err)
		}
		randomAddr = randomPortTaker.Addr()
		if err := randomPortTaker.Close(); err != nil {
			return nil, secret.String{}, fmt.Errorf("unable to release random port: %w", err)
		}
	}

	defaultRoutePath := c.GetRoutePath()

	logger.Debugf(ctx, "builtAVListenURL: protocol: %v, randomAddr: %v", c.Port.Protocol, randomAddr)
	url := &url.URL{
		Scheme: c.Port.Protocol.String(),
		Host:   randomAddr.String(),
		Path:   fmt.Sprintf("%s/", defaultRoutePath),
	}
	switch c.Port.Protocol {
	case ProtocolSRT:
		// We don't set streamid here, so that libav accepts any streamid.
		// The routing is handled by avd based on the initial handshake.
		url.Path = ""
		url.RawQuery = "mode=listener&transtype=live"
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
	case ProtocolRTMP, ProtocolRTSP, ProtocolSRT:
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

func (c *ConnectionProxied) makeAddr(
	ip net.IP,
	port uint16,
) net.Addr {
	switch c.getSocketNetworkName() {
	case "udp":
		return &net.UDPAddr{
			IP:   ip,
			Port: int(port),
		}
	case "tcp":
		return &net.TCPAddr{
			IP:   ip,
			Port: int(port),
		}
	default:
		panic(fmt.Errorf("unsupported network name: %s", c.getSocketNetworkName()))
	}
}

func (c *ConnectionProxied) initAVHandler(
	ctx context.Context,
) (_err error) {
	logger.Debugf(ctx, "initAVHandler")
	defer func() { logger.Debugf(ctx, "/initAVHandler: %v", _err) }()

	// There is a TOCTOU race between builtAVListenURL (which allocates a port
	// by opening+closing a listener) and libav's subsequent bind on that port.
	// If the port gets snatched in the window, libav's bind fails
	// asynchronously and dialAVInput times out. Retry with a fresh port on
	// such failures before giving up.
	var lastErr error
	for attempt := 0; attempt < initAVHandlerPortRetryCount; attempt++ {
		err := c.tryInitAVHandler(ctx)
		if err == nil {
			return nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		logger.Warnf(ctx, "initAVHandler attempt %d/%d failed (port may have been raced): %v", attempt+1, initAVHandlerPortRetryCount, err)
		lastErr = err

		// Clean up the handler's node from the failed attempt so the next
		// iteration of InitAVHandler can create a fresh one.
		handler := c.GetHandler()
		if handler != nil {
			if closeErr := handler.Close(ctx); closeErr != nil {
				logger.Warnf(ctx, "unable to close handler after failed attempt: %v", closeErr)
			}
		}
		// Re-create the handler and init-finished signal so the next attempt
		// starts clean (the async-open callback from the stale attempt is
		// harmless since the handler was closed above).
		c.Locker.Do(ctx, func() {
			c.InitFinished = make(chan struct{})
			c.InitFinishedOnce = sync.Once{}
			c.InitError = nil
		})
		port := c.GetPort()
		if port == nil {
			return fmt.Errorf("unable to init AV handler: port is nil")
		}
		switch port.Mode {
		case PortModePublishers:
			c.SetHandler(newConnectionProxiedPublisher(c))
		case PortModeConsumers:
			c.SetHandler(newConnectionProxiedConsumer(c))
		}
	}
	return fmt.Errorf("unable to init AV handler after %d attempts: %w", initAVHandlerPortRetryCount, lastErr)
}

func (c *ConnectionProxied) tryInitAVHandler(
	ctx context.Context,
) (_err error) {
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

	ip := net.ParseIP(host)
	if ip == nil || ip.IsUnspecified() {
		logger.Errorf(ctx, "unable to parse IP from host '%s' or it is unspecified, defaulting to 127.0.0.1", host)
		ip = net.ParseIP("127.0.0.1")
	}
	avInputAddr := c.makeAddr(ip, uint16(port))
	logger.Debugf(ctx, "avInputAddr: %#+v", avInputAddr)

	logger.Debugf(ctx, "attempting to listen by libav at '%s' (proto:%v)...", url, c.Port.Protocol)
	handler := c.GetHandler()
	err = handler.InitAVHandler(ctx, c.Port.Protocol, url, secretKey, c.Port.Config)
	if err != nil {
		return fmt.Errorf("unable to initialize the AV handler at %s: %w", url, err)
	}

	t := time.NewTicker(50 * time.Millisecond) // TODO: try to get rid of this ticker
	defer t.Stop()
	deadline := time.NewTimer(initAVHandlerDialRetryTimeout)
	defer deadline.Stop()
	var connErr error
	// TODO: explain why we need to retry here
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
			var avConn net.Conn
			avConn, connErr = c.dialAVInput(ctx, avInputAddr)
			if connErr != nil {
				connErr = fmt.Errorf("unable to connect to the libav input '%s': %w", avInputAddr.String(), connErr)
				continue
			}
			c.SetAVConn(avConn)
			return nil
		}
	}
}

func (c *ConnectionProxied) dialAVInput(
	ctx context.Context,
	addr net.Addr,
) (_ret net.Conn, _err error) {
	logger.Debugf(ctx, "dialAVInput(%q)", addr)
	defer func() { logger.Debugf(ctx, "/dialAVInput(%q): %v %v", addr, _ret, _err) }()
	switch addr := addr.(type) {
	case *net.UDPAddr:
		logger.Tracef(ctx, "checking if UDP port %s is already bound...", addr)
		isBound, err := isLocalUDPPortBound(uint16(addr.Port))
		if err != nil {
			return nil, fmt.Errorf("unable to check if port %s is bound: %w", addr, err)
		}
		if !isBound {
			return nil, fmt.Errorf("port %s is not yet bound by libav", addr)
		}
		logger.Tracef(ctx, "port %s is already bound (probably by libav), proceeding to dial", addr)
	}
	return (&net.Dialer{}).DialContext(ctx, c.getSocketNetworkName(), addr.String())
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
	case ProtocolSRT:
		c.onInitFinishedSRT(ctx)
	default:
		logger.Errorf(ctx, "onInitFinished is not implemented for protocol '%s' (yet?)", port.Protocol)
	}
	urlPath := c.GetURLPath()
	if strings.HasPrefix(urlPath, "?") {
		if c.AVInputURL.RawQuery != "" {
			c.AVInputURL.RawQuery += "&" + urlPath[1:]
		} else {
			c.AVInputURL.RawQuery = urlPath[1:]
		}
	} else {
		c.AVInputURL.Path = urlPath
	}
	c.GetHandler().SetURL(ctx, c.AVInputURL)
	c.closeInitFinished()
}

func (c *ConnectionProxied) negotiate(
	origCtx context.Context,
) (_err error) {
	logger.Debugf(origCtx, "negotiate")
	defer func() { logger.Debugf(origCtx, "/negotiate: %v", _err) }()

	avConn, conn := c.GetAVConn(), c.GetProxyConn()
	if avConn == nil {
		return fmt.Errorf("avConn is nil")
	}
	if conn == nil {
		return fmt.Errorf("conn is nil")
	}

	// Only reset conn's deadline; the AVConn reader lives at the
	// connection level and is not managed by negotiate.
	defer func() {
		logger.Tracef(origCtx, "negotiate: resetting the read deadline...")
		if err := conn.SetDeadline(time.Time{}); err != nil {
			logger.Errorf(origCtx, "unable to revert the deadline for Conn: %v", err)
		}
	}()

	var wg sync.WaitGroup
	defer wg.Wait()

	ctx, cancelFn := context.WithCancel(origCtx)
	defer cancelFn()

	errCh := make(chan error, 1)

	forward := func(
		dst net.Conn,
		msg []byte,
	) error {
		if dst == nil {
			return fmt.Errorf("destination connection is nil")
		}
		w, err := dst.Write(msg)
		if err != nil {
			return fmt.Errorf("unable to write: %w", err)
		}

		if w != len(msg) {
			return fmt.Errorf("expected to write %d bytes, but wrote %d", len(msg), w)
		}

		return nil
	}

	// The AVConn→Conn direction is handled by the connection-level
	// reader goroutine (started in newConnectionProxied). Only the
	// Conn→AVConn reader runs here so we can snoop the route path.

	wg.Add(1)
	observability.Go(ctx, func(ctx context.Context) {
		defer wg.Done()
		var buf [SizeBuffer]byte
		for {
			logger.Tracef(ctx, "waiting for c.Conn input...")
			r, err := conn.Read(buf[:])
			logger.Tracef(ctx, "/waiting for c.Conn input: %v %v", r, err)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					logger.Debugf(ctx, "it was a deadline, ignoring")
					return
				}
				errCh <- fmt.Errorf("unable to read from the client: %w", err)
				return
			}

			msg := buf[:r]
			logger.Debugf(ctx, "received %d bytes from client: %q", r, string(msg))
			routePath, err := c.tryExtractRouteString(ctx, msg)
			if err != nil {
				errCh <- fmt.Errorf("unable to snoop the route path: %w", err)
				return
			}

			msg, err = c.correctMessage(ctx, msg)
			if err != nil {
				errCh <- fmt.Errorf("unable to correct the message: %w", err)
				return
			}

			// Forwarding logic
			forwardToAV := func(msg []byte) error {
				logger.Tracef(ctx, "waiting for c.AVConn output...")
				err := forward(avConn, msg)
				logger.Tracef(ctx, "/waiting for c.AVConn output: %v", err)
				return err
			}

			if routePath == nil {
				if err := forwardToAV(msg); err != nil {
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

			observability.Go(ctx, func(ctx context.Context) {
				c.serve(origCtx)
			})

			if err := forwardToAV(msg); err != nil {
				errCh <- err
			}

			errCh <- nil
			return
		}
	})

	interrupt := func() {
		cancelFn()
		// Interrupt the Conn→AVConn reader only; the AVConn→Conn reader
		// lives at the connection level and must keep running.
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
	case ProtocolSRT:
		return c.tryExtractRouteStringSRT(ctx, msg)
	default:
		return nil, fmt.Errorf("protocol '%s' is not supported", c.Port.Protocol)
	}
}

// TODO: get rid of this!
func (c *ConnectionProxied) correctMessage(
	ctx context.Context,
	msg []byte,
) ([]byte, error) {
	switch c.Port.Protocol {
	case ProtocolRTMP:
		return c.correctMessageRTMP(ctx, msg)
	case ProtocolRTSP:
		return c.correctMessageRTSP(ctx, msg)
	case ProtocolSRT:
		return c.correctMessageSRT(ctx, msg)
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

func (c *ConnectionProxied) getFormatContext(context.Context) *astiav.FormatContext {
	handler := c.GetHandler()
	if handler == nil {
		return nil
	}
	return handler.GetFormatContext()
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
	if c == nil {
		return ""
	}
	if c.RoutePath != nil {
		return *c.RoutePath
	}

	// c.Port is nilled by closeLocked, so a late call to this getter
	// (e.g. from logging or a scheduled goroutine) must not panic.
	// Snapshotting via GetPort() avoids TOCTOU with concurrent close.
	port := c.GetPort()
	if port == nil {
		return ""
	}

	if port.Protocol == ProtocolSRT {
		return ""
	}

	if port.Config.DefaultRoutePath != "" {
		return port.Config.DefaultRoutePath
	}

	return "avd-input"
}

func (c *ConnectionProxied) GetURLPath() string {
	if c == nil {
		return ""
	}
	routePath := c.GetRoutePath()
	// c.Port is nilled by closeLocked, so a late call to this getter must
	// not panic. Snapshotting via GetPort() avoids TOCTOU with concurrent
	// close.
	port := c.GetPort()
	if port == nil {
		return ""
	}
	switch port.Protocol {
	case ProtocolRTMP:
		return string(routePath) + "/"
	case ProtocolRTSP:
		return string(routePath)
	case ProtocolSRT:
		return fmt.Sprintf("?streamid=%s", url.QueryEscape("/"+string(routePath)))
	default:
		// Degrade gracefully to match GetRoutePath so a connection with an
		// unrecognized protocol (e.g. retained by a logger after close, or a
		// protocol whose URL-path wiring is not yet implemented) cannot panic.
		// The only caller (onInitFinished) already logs the unrecognized
		// protocol via logger.Errorf and assigning "" to AVInputURL.Path is
		// safe.
		return ""
	}
}

func (c *ConnectionProxied) forward(
	ctx context.Context,
	avConnErrCh <-chan error,
) (_err error) {
	logger.Debugf(ctx, "forward")
	defer func() { logger.Debugf(ctx, "/forward: %v", _err) }()

	avConn, conn := c.GetAVConn(), c.GetProxyConn()
	if avConn == nil {
		return fmt.Errorf("avConn is nil")
	}
	if conn == nil {
		return fmt.Errorf("conn is nil")
	}

	// Only reset conn's deadline; the AVConn reader lives at the
	// connection level and is not managed by forward.
	defer func() {
		if err := conn.SetDeadline(time.Time{}); err != nil {
			logger.Errorf(ctx, "unable to revert the deadline for Conn: %v", err)
		}
	}()

	var wg sync.WaitGroup
	defer wg.Wait()

	ctx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	errCh := make(chan error, 2)

	// The AVConn→Conn direction is handled by the connection-level
	// reader goroutine (started in newConnectionProxied). Only the
	// Conn→AVConn reader runs here.

	wg.Add(1)
	observability.Go(ctx, func(ctx context.Context) {
		defer wg.Done()
		var buf [SizeBuffer]byte
		for {
			logger.Tracef(ctx, "waiting for c.Conn input...")
			r, err := conn.Read(buf[:])
			logger.Tracef(ctx, "/waiting for c.Conn input: %d %v", r, err)
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
			w, writeErr := avConn.Write(msg)
			logger.Tracef(ctx, "/waiting for c.AVConn output: %d %v", w, writeErr)
			if writeErr != nil {
				errCh <- fmt.Errorf("unable to write to the (libav-)server: %w", writeErr)
				return
			}
			if w != len(msg) {
				errCh <- fmt.Errorf("expected to write %d bytes to AVConn, but wrote %d", len(msg), w)
				return
			}
		}
	})

	// Relay errors from the connection-level AVConn→Conn reader into
	// the local errCh so the select below handles both directions.
	observability.Go(ctx, func(ctx context.Context) {
		select {
		case <-ctx.Done():
		case err, ok := <-avConnErrCh:
			if ok {
				errCh <- err
			}
		}
	})

	interrupt := func() {
		cancelFn()
		// Interrupt the Conn→AVConn reader only; the AVConn→Conn reader
		// lives at the connection level and must keep running until the
		// connection is closed.
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
