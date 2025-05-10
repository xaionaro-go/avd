package avd

import (
	"bytes"
	"context"
	"encoding/binary"
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
	"github.com/xaionaro-go/avpipeline"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/processor"
	avpipelinetypes "github.com/xaionaro-go/avpipeline/types"
	"github.com/xaionaro-go/observability"
	"github.com/xaionaro-go/secret"
	"github.com/xaionaro-go/xsync"
)

type Connection[N AbstractNodeIO] struct {
	Locker xsync.Mutex

	// access only when Locker is locked (but better don't access at all if you are not familiar with the code):
	Port         *ListeningPort
	Conn         net.Conn
	CancelFunc   context.CancelFunc
	AVInputURL   *url.URL
	AVInputKey   secret.String
	AVConn       *net.TCPConn
	Node         N
	InitError    error
	InitFinished chan struct{}
	RoutePath    *string
	Route        *Route
}

var _ = (*Connection[*NodeInput])(nil)

func newConnection[N AbstractNodeIO](
	ctx context.Context,
	p *ListeningPort,
	conn net.Conn,
) (_ret *Connection[N], _err error) {
	ctx, cancelFn := context.WithCancel(ctx)
	ctx = belt.WithField(ctx, "remote_addr", conn.RemoteAddr())
	c := &Connection[N]{
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
			case ProtocolRTMP:
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
			logger.Errorf(ctx, "unable to forward the connection with %s: %v", conn.RemoteAddr(), err)
			return
		}
	})

	return c, nil
}

func (c *Connection[N]) String() string {
	return fmt.Sprintf(
		"%s[%s](%s->%s->%s->%s)",
		strings.ToUpper(c.Port.Protocol.String()), c.Mode(),
		c.Conn.RemoteAddr(), c.Conn.LocalAddr(), c.AVConn.LocalAddr(), c.AVConn.RemoteAddr(),
	)
}

func (c *Connection[N]) GetInputNode(
	context.Context,
) avpipeline.AbstractNode {
	return c.Node
}

func (c *Connection[N]) GetOutputRoute(
	context.Context,
) *Route {
	return c.Route
}

func (c *Connection[N]) Mode() PortMode {
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

func (c *Connection[N]) Close(ctx context.Context) (_err error) {
	logger.Debugf(ctx, "Close()")
	defer logger.Debugf(ctx, "/Close(): %v", _err)
	c.CancelFunc()
	return xsync.DoR1(ctx, &c.Locker, func() error {
		var errs []error
		if c.Route != nil {
			switch c.Mode() {
			case PortModePublishers:
				if _, err := c.Route.removePublisher(ctx, c); err != nil {
					errs = append(errs, fmt.Errorf("unable to remove myself as a  at '%s': %w", c.Route.Path, err))
				}
			case PortModeConsumers:
				if err := avpipeline.RemovePushPacketsTo(ctx, c.Route.Node, c.Node); err != nil {
					errs = append(errs, fmt.Errorf("unable to unsubscribe from packets from '%s': %w", c.Route.Path, err))
				}
			}
			c.Route = nil
		}
		if c.AVConn != nil {
			c.AVConn.SetDeadline(time.Unix(1, 0))
			c.AVConn = nil
		}
		if c.Conn != nil {
			c.Conn.SetDeadline(time.Unix(1, 0))
			c.Conn = nil
		}
		if c.Node != nil {
			if err := c.Node.GetProcessor().Close(ctx); err != nil {
				errs = append(errs, fmt.Errorf("unable to close the input node processor: %w", err))
			}
			c.Node = nil
		}
		return errors.Join(errs...)
	})
}

func (c *Connection[N]) builtAVListenURL(
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

func (c *Connection[N]) initAVHandler(
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

	customOpts := avpipelinetypes.DictionaryItems{
		{Key: "listen", Value: "1"},
	}
	if c.Port.Config.MaxBufferSize != 0 {
		customOpts = append(customOpts, avpipelinetypes.DictionaryItem{
			Key: "buffer_size", Value: fmt.Sprintf("%d", c.Port.Config.MaxBufferSize),
		})
	}
	if c.Port.Config.ReorderQueueSize != 0 {
		customOpts = append(customOpts, avpipelinetypes.DictionaryItem{
			Key: "reorder_queue_size", Value: fmt.Sprintf("%d", c.Port.Config.ReorderQueueSize),
		})
	}
	if c.Port.Config.Timeout != 0 {
		customOpts = append(customOpts, avpipelinetypes.DictionaryItem{
			Key: "timeout", Value: fmt.Sprintf("%d", c.Port.Config.Timeout.Microseconds()),
		})
	}
	switch c.Port.Protocol {
	case ProtocolRTMP:
		customOpts = append(customOpts, avpipelinetypes.DictionaryItems{
			{Key: "rtmp_app", Value: c.GetRoutePath()},
			{Key: "rtmp_live", Value: "live"},
			{Key: "rtmp_buffer", Value: fmt.Sprintf("%d", c.Port.GetConfig().GetBufferDuration().Milliseconds())},
		}...)
	case ProtocolRTSP:
		customOpts = append(customOpts, avpipelinetypes.DictionaryItems{
			{Key: "rtsp_flags", Value: "listen"},
		}...)
		if c.Port.Config.RTSP.PacketSize != 0 {
			customOpts = append(customOpts, avpipelinetypes.DictionaryItem{
				Key: "pkt_size", Value: fmt.Sprintf("%d", c.Port.Config.RTSP.PacketSize),
			})
		}
		if c.Port.Config.RTSP.TransportProtocol != UndefinedTransportProtocol {
			customOpts = append(customOpts, avpipelinetypes.DictionaryItem{
				Key: "rtsp_transport", Value: c.Port.Config.RTSP.TransportProtocol.String(),
			})
		}
	case ProtocolSRT:
		customOpts = append(customOpts, avpipelinetypes.DictionaryItems{
			{Key: "smoother", Value: "live"},
			{Key: "transtype", Value: "live"},
		}...)
	}

	logger.Debugf(ctx, "attempting to listen by libav at '%s'...", url)
	switch c.Mode() {
	case PortModePublishers:
		input, err := kernel.NewInputFromURL(
			ctx,
			url.String(),
			secretKey,
			kernel.InputConfig{
				CustomOptions: append(avpipelinetypes.DictionaryItems{}, customOpts...),
				AsyncOpen:     true,
				OnOpened: func(ctx context.Context, i *kernel.Input) error {
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
			return
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
				CustomOptions: append(avpipelinetypes.DictionaryItems{
					{Key: "f", Value: c.Port.Protocol.FormatName()},
				}, customOpts...),
				AsyncOpen: true,
				OnOpened: func(ctx context.Context, o *kernel.Output) error {
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
			return
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

func (c *Connection[N]) onInitFinished(
	ctx context.Context,
) {
	switch c.Port.Protocol {
	case ProtocolRTMP:
		c.onInitFinishedRTMP(ctx)
	case ProtocolRTSP:
		c.onInitFinishedRTSP(ctx)
	default:
		logger.Errorf(ctx, "onInitFinished is not implemented for protocol '%s' (yet?)", c.Port.Protocol)
	}
	close(c.InitFinished)
}

func (c *Connection[N]) onInitFinishedRTMP(
	ctx context.Context,
) {
	routePath := c.GetRoutePath()
	rtmpCtx := c.AVRTMPContext()
	logger.Debugf(ctx, "updating the app name: '%s' -> '%s'", rtmpCtx.App(), routePath)
	rtmpCtx.SetApp(routePath)
}

func (c *Connection[N]) onInitFinishedRTSP(
	ctx context.Context,
) {
	routePath := c.GetRoutePath()
	rtspState := c.AVRTSPState()
	logger.Debugf(ctx, "updating the control URI: '%s' -> '%s'", rtspState.ControlURI(), routePath)
	rtspState.SetControlURI(routePath)
	for idx, stream := range rtspState.RTSPStreams() {
		logger.Debugf(ctx, "updating the control URL in stream #%d: '%s' -> '%s'", idx, stream.ControlURL(), routePath)
		stream.SetControlURL(routePath)
	}
}

func (c *Connection[N]) negotiate(
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
			logger.Tracef(ctx, "msg: %X (suspect 'connect': %t)", msg, bytes.Contains(msg, []byte("connect")))
			if !bytes.Contains(msg, connectMagic) {
				logger.Tracef(ctx, "waiting for c.AVConn output...")
				err := forward(c.AVConn, msg)
				logger.Tracef(ctx, "/waiting for c.AVConn output")
				if err != nil {
					errCh <- err
					return
				}
				continue
			}

			appName, err := parseAppName(ctx, msg)
			if err != nil {
				errCh <- fmt.Errorf("unable to parse the route path from the 'connect' message: %w", err)
				return
			}
			c.RoutePath = ptr(string(appName))
			logger.Debugf(ctx, "appName == '%s'", *c.RoutePath)

			routePath := *c.RoutePath
			ctx = belt.WithField(ctx, "path", routePath)
			route, err := c.Port.GetServer().GetRoute(ctx, routePath, GetRouteModeCreateIfNotFound)
			if err != nil {
				errCh <- fmt.Errorf("unable to create a route '%s': %w", routePath, err)
				return
			}
			c.Route = route
			switch c.Mode() {
			case PortModePublishers:
				if err := route.addPublisherIfNoPublishers(ctx, c); err != nil {
					errCh <- fmt.Errorf("unable to add myself as a  to '%s': %w", routePath, err)
					return
				}
				c.Node.AddPushPacketsTo(route.Node)

			}
			observability.Go(ctx, func() {
				errCh := make(chan avpipeline.ErrNode, 100)
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
					}
				})
				<-c.InitFinished
				if c.InitError != nil {
					logger.Debugf(ctx, "not running Serve, because of InitError: %v", c.InitError)
					return
				}
				logger.Debugf(ctx, "resulting graph: %s", c.Node.DotString(false))
				serveCtx := belt.WithField(origCtx, "path", routePath)
				switch c.Mode() {
				case types.PortModeConsumers:
					c.Route.Node.AddPushPacketsTo(c.Node)
				}
				c.Node.Serve(serveCtx, avpipeline.ServeConfig{}, errCh)
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

func (c *Connection[N]) getKernel() kernel.Abstract {
	switch p := c.Node.GetProcessor().(type) {
	case *processor.FromKernel[*kernel.Input]:
		return p.Kernel
	case *processor.FromKernel[*kernel.Output]:
		return p.Kernel
	default:
		panic(fmt.Errorf("unexpected type: %T", p))
	}
}

func (c *Connection[N]) getFormatContext() *astiav.FormatContext {
	switch k := c.getKernel().(type) {
	case *kernel.Input:
		return k.FormatContext
	case *kernel.Output:
		return k.FormatContext
	default:
		panic(fmt.Errorf("unexpected type: %T", k))
	}
}

func (c *Connection[N]) AVURLContext() *avcommon.URLContext {
	fmtCtx := avcommon.WrapAVFormatContext(
		xastiav.CFromAVFormatContext(
			c.getFormatContext(),
		),
	)
	avioCtx := fmtCtx.Pb()
	return avcommon.WrapURLContext(avioCtx.Opaque())
}

func (c *Connection[N]) AVRTMPContext() *avcommon.RTMPContext {
	return avcommon.WrapRTMPContext(c.AVURLContext().PrivData())
}

func (c *Connection[N]) AVRTSPState() *avcommon.RTSPState {
	return avcommon.WrapRTSPState(c.AVURLContext().PrivData())
}

func (c *Connection[N]) GetRoutePath() string {
	if c.RoutePath != nil {
		return *c.RoutePath
	}

	if c.Port.Config.DefaultRoutePath != "" {
		return c.Port.Config.DefaultRoutePath
	}

	return "avd-input"
}

func (c *Connection[N]) forward(
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

const (
	ConnectionEnableRoutePathUpdaterHack = true
)

var connectMagic []byte

func init() {
	/* An example of a packet:
	00000000  03 00 00 00 00 00 91 14  00 00 00 00 02 00 07 63  |...............c|
	00000010  6f 6e 6e 65 63 74 00 3f  f0 00 00 00 00 00 00 03  |onnect.?........|
	00000020  00 03 61 70 70 02 00 07  74 65 73 74 41 70 70 00  |..app...testApp.|
	00000030  04 74 79 70 65 02 00 0a  6e 6f 6e 70 72 69 76 61  |.type...nonpriva|
	00000040  74 65 00 08 66 6c 61 73  68 56 65 72 02 00 23 46  |te..flashVer..#F|
	00000050  4d 4c 45 2f 33 2e 30 20  28 63 6f 6d 70 61 74 69  |MLE/3.0 (compati|
	00000060  62 6c 65 3b 20 4c 61 76  66 36 31 2e 37 2e 31 30  |ble; Lavf61.7.10|
	00000070  30 29 00 05 74 63 55 72  6c 02 00 1e 72 74 6d 70  |0)..tcUrl...rtmp|
	00000080  3a 2f 2f 31 32 37 2e 30  2e 30 2e 31 c3 3a 34 33  |://127.0.0.1.:43|
	00000090  36 34 35 2f 74 65 73 74  41 70 70 00 00 09        |645/testApp...|
	0000009e
	*/
	connectMagic = append(connectMagic, []byte{0x02, 0x00, 0x07}...)
	connectMagic = append(connectMagic, []byte("connect\000")...)
	connectMagic = append(connectMagic, []byte{0x3f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03}...)
}

func parseAppName(
	ctx context.Context,
	msg []byte,
) (_ret []byte, _err error) {
	logger.Tracef(ctx, "parseAppName(ctx, %X)", msg)
	defer func() { logger.Tracef(ctx, "/parseAppName: '%v' %v", string(_ret), _err) }()
	/*
			An example of a packet:

		00000000  03 00 00 00 00 00 8b 14  00 00 00 00 02 00 07 63  |...............c|
		00000010  6f 6e 6e 65 63 74 00 3f  f0 00 00 00 00 00 00 03  |onnect.?........|
		00000020  00 03 61 70 70 02 00 04  74 65 73 74 00 04 74 79  |..app...test..ty|
		00000030  70 65 02 00 0a 6e 6f 6e  70 72 69 76 61 74 65 00  |pe...nonprivate.|
		00000040  08 66 6c 61 73 68 56 65  72 02 00 23 46 4d 4c 45  |.flashVer..#FMLE|
		00000050  2f 33 2e 30 20 28 63 6f  6d 70 61 74 69 62 6c 65  |/3.0 (compatible|
		00000060  3b 20 4c 61 76 66 36 31  2e 37 2e 31 30 30 29 00  |; Lavf61.7.100).|
		00000070  05 74 63 55 72 6c 02 00  1b 72 74 6d 70 3a 2f 2f  |.tcUrl...rtmp://|
		00000080  31 32 37 2e 30 2e 30 2e  31 3a 34 32 c3 34 34 39  |127.0.0.1:42.449|
		00000090  2f 74 65 73 74 00 00 09                           |/test...|
	*/

	connectMagicIdx := bytes.Index(msg, connectMagic)
	if connectMagicIdx < 0 {
		return nil, fmt.Errorf("internal error: the 'connect' magic was not found")
	}

	idx := connectMagicIdx + len(connectMagic)
	for {
		if idx+2 >= len(msg) {
			return nil, fmt.Errorf("the message was too short: cannot get the length of the key: %d >= %d", idx+2, len(msg))
		}
		keyLen := binary.BigEndian.Uint16(msg[idx:])
		idx += 2

		if idx+int(keyLen) >= len(msg) {
			return nil, fmt.Errorf("the message was too short: cannot get the key: %d >= %d", idx+int(keyLen), len(msg))
		}
		key := string(msg[idx : idx+int(keyLen)])
		idx += int(keyLen)

		if idx+1 >= len(msg) {
			return nil, fmt.Errorf("the message was too short: cannot get the the value type: %d >= %d (key: '%s')", idx+1, len(msg), key)
		}
		valueType := msg[idx]
		idx++

		if valueType != 0x02 {
			return nil, fmt.Errorf("we currently support only string values, but received type ID %d (key: '%s')", valueType, key)
		}

		if idx+2 >= len(msg) {
			return nil, fmt.Errorf("the message was too short: cannot get the length of the value: %d >= %d (key: '%s')", idx+2, len(msg), key)
		}
		valueLen := binary.BigEndian.Uint16(msg[idx:])
		idx += 2

		if idx+int(valueLen) >= len(msg) {
			return nil, fmt.Errorf("the message was too short: cannot get the value: %d >= %d (key: '%s')", idx+int(valueLen), len(msg), key)
		}
		value := msg[idx : idx+int(valueLen)]
		idx += int(valueLen)

		logger.Debugf(ctx, "'%s': '%s'", key, value)
		switch key {
		case "app":
			return value, nil
		}
	}
}
