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
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/node"
	"github.com/xaionaro-go/avpipeline/processor"
	"github.com/xaionaro-go/avpipeline/router"
	"github.com/xaionaro-go/observability"
	"github.com/xaionaro-go/secret"
	"github.com/xaionaro-go/xsync"
)

const (
	ConnectionEnableRoutePathUpdaterHack = true
)

type ConnectionProxied[N AbstractNodeIO] struct {
	Locker xsync.Mutex

	// access only when Locker is locked (but better don't access at all if you are not familiar with the code):
	Port         *ListeningPortProxied
	Conn         net.Conn
	CancelFunc   context.CancelFunc
	AVInputURL   *url.URL
	AVInputKey   secret.String
	AVConn       *net.TCPConn
	Node         N
	InitError    error
	InitFinished chan struct{}
	RoutePath    *RoutePath
	Route        *router.Route

	Forwarder *router.StreamForwarderCopy
}

var _ = (*ConnectionProxied[*NodeInput])(nil)

func newConnectionProxied[N AbstractNodeIO](
	ctx context.Context,
	p *ListeningPortProxied,
	conn net.Conn,
) (_ret *ConnectionProxied[N], _err error) {
	ctx, cancelFn := context.WithCancel(ctx)
	ctx = belt.WithField(ctx, "remote_addr", conn.RemoteAddr())
	c := &ConnectionProxied[N]{
		Port:         p,
		Conn:         conn,
		CancelFunc:   cancelFn,
		InitFinished: make(chan struct{}),
	}
	logger.Debugf(ctx, "newConnection[%s]", c.Mode())
	defer func() { logger.Debugf(ctx, "/newConnection[%s]: %v %v", c.Mode(), _ret, _err) }()
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

func (c *ConnectionProxied[N]) String() string {
	return fmt.Sprintf(
		"%s[%s](%s->%s->%s->%s)",
		strings.ToUpper(c.Port.Protocol.String()), c.Mode(),
		c.Conn.RemoteAddr(), c.Conn.LocalAddr(), c.AVConn.LocalAddr(), c.AVConn.RemoteAddr(),
	)
}

func (c *ConnectionProxied[N]) GetInputNode(
	context.Context,
) node.Abstract {
	return c.Node
}

func (c *ConnectionProxied[N]) GetOutputRoute(
	context.Context,
) *router.Route {
	return c.Route
}

func (c *ConnectionProxied[N]) Mode() PortMode {
	if c == nil {
		return UndefinedPortMode
	}
	var nodeZeroValue N
	switch any(nodeZeroValue).(type) {
	case *NodeInput:
		return PortModePublishers
	case *NodeOutput:
		return PortModeConsumers
	default:
		panic(fmt.Errorf("unexpected type: '%T'", nodeZeroValue))
	}
}

func (c *ConnectionProxied[N]) Close(ctx context.Context) (_err error) {
	logger.Debugf(ctx, "Close()")
	defer logger.Debugf(ctx, "/Close(): %v", _err)
	c.CancelFunc()
	return xsync.DoR1(ctx, &c.Locker, func() error {
		var errs []error
		if c.Forwarder != nil {
			if err := c.Forwarder.Stop(ctx); err != nil {
				errs = append(errs, fmt.Errorf("unable to stop stream forwarding: %w", err))
			}
			c.Forwarder = nil
		}
		if c.Route != nil {
			switch c.Mode() {
			case PortModePublishers:
				if _, err := c.Route.RemovePublisher(ctx, c); err != nil {
					errs = append(errs, fmt.Errorf("unable to remove myself as a  at '%s': %w", c.Route.Path, err))
				}
			}
			c.Route = nil
		}
		if c.AVConn != nil {
			c.AVConn.Close()
			c.AVConn = nil
		}
		if c.Conn != nil {
			c.Conn.Close()
			c.Conn = nil
		}
		if c.Node != nil {
			if err := c.Node.GetProcessor().Close(ctx); err != nil {
				errs = append(errs, fmt.Errorf("unable to close the node processor: %w", err))
			}
			c.Node = nil
		}
		return errors.Join(errs...)
	})
}

func (c *ConnectionProxied[N]) builtAVListenURL(
	ctx context.Context,
) (*url.URL, secret.String, error) {
	if !c.Port.Protocol.IsValid() {
		return nil, secret.New(""), fmt.Errorf("protocol is not set")
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
func (c *ConnectionProxied[N]) isAsyncOpen(
	ctx context.Context,
) (_ret bool) {
	logger.Debugf(ctx, "isAsyncOpen")
	defer func() { logger.Debugf(ctx, "/isAsyncOpen: %v", _ret) }()
	switch c.Port.Protocol {
	case ProtocolRTMP:
		return true
	}
	switch any(c.Node).(type) {
	case *NodeInput:
		return true
	}
	return false
}

func (c *ConnectionProxied[N]) initAVHandler(
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
	switch c.Mode() {
	case PortModePublishers:
		input, err := kernel.NewInputFromURL(
			ctx,
			url.String(),
			secretKey,
			kernel.InputConfig{
				CustomOptions: customOpts,
				AsyncOpen:     c.isAsyncOpen(ctx),
				OnOpened: func(ctx context.Context, i *kernel.Input) error {
					if !c.isAsyncOpen(ctx) {
						return nil
					}
					c.onInitFinished(ctx)
					return nil
				},
			},
		)
		if err != nil {
			err = fmt.Errorf("unable to start listening '%s' using libav: %w", url.String(), err)
			logger.Errorf(ctx, "%v", err)
			c.InitError = err
			close(c.InitFinished)
			observability.Go(ctx, func() {
				c.Close(ctx)
			})
			return err
		}
		c.Node = any(newInputNode(ctx, c, input)).(N)
	case PortModeConsumers:
		node, err := newOutputNode(
			ctx,
			c,
			nil,
			url.String(),
			secretKey,
			kernel.OutputConfig{
				CustomOptions: customOpts,
				AsyncOpen:     c.isAsyncOpen(ctx),
				OnOpened: func(ctx context.Context, i *kernel.Output) error {
					if !c.isAsyncOpen(ctx) {
						return nil
					}
					c.onInitFinished(ctx)
					return nil
				},
			},
		)
		if err != nil {
			err = fmt.Errorf("unable to start listening '%s' using libav: %w", url.String(), err)
			logger.Errorf(ctx, "%v", err)
			c.InitError = err
			close(c.InitFinished)
			observability.Go(ctx, func() {
				c.Close(ctx)
			})
			return err
		}
		c.Node = any(node).(N)
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

func (c *ConnectionProxied[N]) onInitFinished(
	ctx context.Context,
) {
	logger.Debugf(ctx, "onInitFinished")
	defer func() { logger.Debugf(ctx, "/onInitFinished") }()
	switch c.Port.Protocol {
	case ProtocolRTMP:
		c.onInitFinishedRTMP(ctx)
	case ProtocolRTSP:
		c.onInitFinishedRTSP(ctx)
	default:
		logger.Errorf(ctx, "onInitFinished is not implemented for protocol '%s' (yet?)", c.Port.Protocol)
	}
	c.AVInputURL.Path = c.GetURLPath()
	switch c.Mode() {
	case PortModePublishers:
		n := any(c.Node).(*NodeInput)
		n.Processor.Kernel.URL = c.AVInputURL.String()
	case PortModeConsumers:
		n := any(c.Node).(*NodeOutput)
		n.Processor.Kernel.URL = c.AVInputURL.String()
		n.Processor.Kernel.URLParsed = c.AVInputURL
	}
	close(c.InitFinished)
}

func (c *ConnectionProxied[N]) negotiate(
	origCtx context.Context,
) (_err error) {
	logger.Debugf(origCtx, "negotiate")
	defer func() { logger.Debugf(origCtx, "/negotiate: %v", _err) }()

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
			r, err := c.AVConn.Read(buf[:])
			logger.Tracef(ctx, "/waiting for c.AVConn input: %v %v", r, err)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					// revert back:
					c.AVConn.SetDeadline(time.Time{})
				}
				errCh <- fmt.Errorf("unable to read from the (libav-)server: %w", err)
				return
			}

			msg := buf[:r]
			logger.Tracef(ctx, "waiting for c.Conn output...")
			err = forward(c.Conn, msg)
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
			select {
			case <-ctx.Done():
				return
			default:
			}

			logger.Tracef(ctx, "waiting for c.Conn input...")
			r, err := c.Conn.Read(buf[:])
			logger.Tracef(ctx, "/waiting for c.Conn input")
			if err != nil {
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
				err := forward(c.AVConn, msg)
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
			err = forward(c.AVConn, msg)
			logger.Tracef(ctx, "/waiting for c.AVConn output")
			if err != nil {
				errCh <- err
			}

			errCh <- nil
			return
		}
	})

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		cancelFn()
		// to interrupt reading from the socket:
		logger.Debugf(ctx, "setting a deadline in the past for c.AVConn")
		if err := c.AVConn.SetReadDeadline(time.Unix(1, 0)); err != nil {
			logger.Errorf(ctx, "unable to set the read deadline for AVConn: %v", err)
		}
		return err
	}
}

func (c *ConnectionProxied[N]) serve(
	ctx context.Context,
) {
	var routeGetMode router.GetRouteMode
	switch c.Mode() {
	case PortModePublishers:
		routeGetMode = router.GetRouteModeCreateIfNotFound
	case PortModeConsumers:
		routeGetMode = router.GetRouteModeWaitForPublisher
	}

	routePath := *c.RoutePath
	route, err := c.Port.GetServer().GetRoute(ctx, routePath, routeGetMode)
	if err != nil {
		logger.Errorf(ctx, "unable to create a route '%s': %v", routePath, err)
		c.Close(ctx)
		return
	}
	c.Route = route
	switch c.Mode() {
	case PortModePublishers:
		if err := route.AddPublisher(ctx, c); err != nil {
			logger.Errorf(ctx, "unable to add myself as a  to '%s': %v", routePath, err)
			c.Close(ctx)
			return
		}
		c.Node.AddPushPacketsTo(route.Node)
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
		logger.Debugf(ctx, "resulting graph: %s", c.Node.DotString(false))
	}
	switch c.Mode() {
	case PortModeConsumers:
		err := c.startStreamForward(ctx, c.Route.Node, c.Node)
		if err != nil {
			logger.Errorf(ctx, "unable to forward the stream: %v", err)
			c.Close(ctx)
			return
		}
	}
	c.Node.Serve(ctx, node.ServeConfig{}, errCh)
}

func (c *ConnectionProxied[N]) startStreamForward(
	ctx context.Context,
	src *NodeRouting,
	dst node.Abstract,
) (_err error) {
	logger.Debugf(ctx, "startStreamForward(%s, %s)", src, dst)
	defer func() { logger.Debugf(ctx, "/startStreamForward(%s, %s): %v", src, dst, _err) }()

	fwd, err := router.NewStreamForwarderCopy(ctx, src, dst)
	if err != nil {
		return fmt.Errorf("unable to make a new stream forwarder from %s to %s: %w", src, dst, err)
	}

	c.Forwarder = fwd
	if err := fwd.Start(ctx); err != nil {
		return fmt.Errorf("unable to start forwarding the traffic from %s to %s: %w", src, dst, err)
	}
	return nil
}

func (c *ConnectionProxied[N]) tryExtractRouteString(
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

func (c *ConnectionProxied[N]) getKernel() kernel.Abstract {
	switch p := c.Node.GetProcessor().(type) {
	case *processor.FromKernel[*kernel.Input]:
		return p.Kernel
	case *processor.FromKernel[*kernel.Output]:
		return p.Kernel
	default:
		panic(fmt.Errorf("unexpected type: %T", p))
	}
}

func (c *ConnectionProxied[N]) getFormatContext() *astiav.FormatContext {
	switch k := c.getKernel().(type) {
	case *kernel.Input:
		return k.FormatContext
	case *kernel.Output:
		return k.FormatContext
	default:
		panic(fmt.Errorf("unexpected type: %T", k))
	}
}

func (c *ConnectionProxied[N]) AVFormatContext() *avcommon.AVFormatContext {
	return avcommon.WrapAVFormatContext(
		xastiav.CFromAVFormatContext(
			c.getFormatContext(),
		),
	)
}

func (c *ConnectionProxied[N]) AVURLContext() *avcommon.URLContext {
	fmtCtx := c.AVFormatContext()
	avioCtx := fmtCtx.Pb()
	if avioCtx == nil {
		panic("internal error: avioCtx == nil")
	}
	return avcommon.WrapURLContext(avioCtx.Opaque())
}

func (c *ConnectionProxied[N]) GetRoutePath() RoutePath {
	if c.RoutePath != nil {
		return *c.RoutePath
	}

	if c.Port.Config.DefaultRoutePath != "" {
		return c.Port.Config.DefaultRoutePath
	}

	return "avd-input"
}

func (c *ConnectionProxied[N]) GetURLPath() string {
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

func (c *ConnectionProxied[N]) forward(
	ctx context.Context,
) (_err error) {
	logger.Debugf(ctx, "forward")
	defer func() { logger.Debugf(ctx, "/forward: %v", _err) }()

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
			select {
			case <-ctx.Done():
				return
			default:
			}

			logger.Tracef(ctx, "waiting for AVConn input...")
			r, err := c.AVConn.Read(buf[:])
			logger.Tracef(ctx, "/waiting for AVConn input")
			if err != nil {
				errCh <- fmt.Errorf("unable to read from the (libav-)server: %w", err)
				return
			}

			msg := buf[:r]
			logger.Tracef(ctx, "waiting for c.Conn output...")
			err = forward(c.Conn, msg)
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
			select {
			case <-ctx.Done():
				return
			default:
			}

			logger.Tracef(ctx, "waiting for c.Conn input...")
			r, err := c.Conn.Read(buf[:])
			logger.Tracef(ctx, "/waiting for c.Conn input")
			if err != nil {
				errCh <- fmt.Errorf("unable to read from the client: %w", err)
				return
			}

			msg := buf[:r]
			logger.Tracef(ctx, "waiting for c.AVConn output...")
			err = forward(c.AVConn, msg)
			logger.Tracef(ctx, "/waiting for c.AVConn output")
			if err != nil {
				errCh <- err
				return
			}
		}
	})

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		cancelFn()
		return err
	}
}
