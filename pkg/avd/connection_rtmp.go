package avd

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
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

type ConnectionRTMP[N NodeIO] struct {
	Locker xsync.Mutex

	// access only when Locker is locked (but better don't access at all if you are not familiar with the code):
	Port         *ListeningPortRTMP
	Conn         net.Conn
	CancelFunc   context.CancelFunc
	AVInputURL   *url.URL
	AVInputKey   secret.String
	AVConn       *net.TCPConn
	Node         N
	InitError    error
	InitFinished chan struct{}
	AppName      *string
	Route        *Route
}

var _ = (*ConnectionRTMP[*NodeInput])(nil)

func newConnectionRTMP[N NodeIO](
	ctx context.Context,
	p *ListeningPortRTMP,
	conn net.Conn,
) (_ret *ConnectionRTMP[N], _err error) {
	ctx, cancelFn := context.WithCancel(ctx)
	ctx = belt.WithField(ctx, "remote_addr", conn.RemoteAddr())
	c := &ConnectionRTMP[N]{
		Port:         p,
		Conn:         conn,
		CancelFunc:   cancelFn,
		InitFinished: make(chan struct{}),
	}
	logger.Debugf(ctx, "newConnectionRTMP[%s]", c.Mode())
	defer func() { logger.Debugf(ctx, "/newConnectionRTMP[%s]: %v %v", c.Mode(), _ret, _err) }()
	defer func() {
		if _ret == nil {
			logger.Debugf(ctx, "not initialized")
			c.Close(ctx)
		}
	}()

	err := c.initRTMPHandler(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to handle connection from %s: %w", conn.RemoteAddr(), err)
	}

	observability.Go(ctx, func() {
		defer func() {
			logger.Debugf(ctx, "the end")
			c.Close(ctx)
		}()
		if ConnectionRTMPEnableAppNameUpdaterHack {
			if err := c.negotiate(ctx); err != nil {
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

func (c *ConnectionRTMP[N]) String() string {
	return fmt.Sprintf("RTMP[%s](%s->%s->%s->%s)", c.Mode(), c.Conn.RemoteAddr(), c.Conn.LocalAddr(), c.AVConn.LocalAddr(), c.AVConn.RemoteAddr())
}

func (c *ConnectionRTMP[N]) GetInputNode(
	context.Context,
) avpipeline.AbstractNode {
	return c.Node
}

func (c *ConnectionRTMP[N]) GetOutputRoute(
	context.Context,
) *Route {
	return c.Route
}

func (c *ConnectionRTMP[N]) Mode() RTMPMode {
	var nodeZeroValue N
	switch any(nodeZeroValue).(type) {
	case *NodeInput:
		return RTMPModePublishers
	case *NodeOutput:
		return RTMPModeConsumers
	default:
		panic(fmt.Errorf("unexpected type: '%T'", nodeZeroValue))
	}
}

func (c *ConnectionRTMP[N]) Close(ctx context.Context) (_err error) {
	logger.Debugf(ctx, "Close()")
	defer logger.Debugf(ctx, "/Close(): %v", _err)
	c.CancelFunc()
	return xsync.DoR1(ctx, &c.Locker, func() error {
		var errs []error
		if c.Route != nil {
			switch c.Mode() {
			case RTMPModePublishers:
				if _, err := c.Route.removePublisher(ctx, c); err != nil {
					errs = append(errs, fmt.Errorf("unable to remove myself as a  at '%s': %w", c.Route.Path, err))
				}
			case RTMPModeConsumers:
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

func (c *ConnectionRTMP[N]) initRTMPHandler(
	ctx context.Context,
) (_err error) {
	logger.Debugf(ctx, "initRTMPHandler")
	defer func() { logger.Debugf(ctx, "/initRTMPHandler: %v", _err) }()

	randomPortTaker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("unable to take a random port")
	}
	randomPort := randomPortTaker.Addr().String()
	randomPortTaker.Close()

	defaultAppName := "avd-input"
	if c.Port.GetConfig().DefaultAppName != "" {
		defaultAppName = c.Port.GetConfig().DefaultAppName
	}

	listenURL := fmt.Sprintf("rtmp://%s/%s/", randomPort, defaultAppName)
	url, err := url.Parse(listenURL)
	if err != nil {
		return fmt.Errorf("unable to parse URL '%s' (it is an internal URL for communication between AVD and LibAV): %w", listenURL, err)
	}
	queryWords := strings.Split(url.Path, "/")
	url.Path = strings.Join(queryWords[:len(queryWords)-1], "/")
	secretKey := secret.New(queryWords[len(queryWords)-1])

	c.AVInputURL = url
	c.AVInputKey = secretKey
	logger.Debugf(ctx, "c.AVInputURL: %#+v", c.AVInputURL)

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
	switch c.Mode() {
	case RTMPModePublishers:
		input, err := kernel.NewInputFromURL(
			ctx,
			url.String(),
			secretKey,
			kernel.InputConfig{
				CustomOptions: avpipelinetypes.DictionaryItems{
					{Key: "listen", Value: "1"},
				},
				AsyncOpen: true,
				OnOpened: func(ctx context.Context, i *kernel.Input) error {
					c.onInitFinished(ctx)
					return nil
				},
			},
		)
		if err != nil {
			err = fmt.Errorf("unable to start listening '%s' using libav: %w", listenURL, err)
			logger.Errorf(ctx, "%v", err)
			c.InitError = err
			close(c.InitFinished)
			observability.Go(ctx, func() {
				c.Close(ctx)
			})
			return
		}
		c.Node = any(newInputNode(ctx, c, input)).(N)
	case RTMPModeConsumers:
		c.Node = any(newOutputNode(
			ctx,
			c,
			func(ctx context.Context) error {
				return nil
			},
			url.String(),
			secretKey,
			kernel.OutputConfig{
				CustomOptions: avpipelinetypes.DictionaryItems{
					{Key: "f", Value: "flv"},
					{Key: "listen", Value: "1"},
				},
				AsyncOpen: true,
				OnOpened: func(ctx context.Context, o *kernel.Output) error {
					c.onInitFinished(ctx)
					return nil
				},
			},
		)).(N)
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
			var avInputConn *net.TCPConn
			avInputConn, connErr = net.DialTCP("tcp4", nil, avInputAddr)
			if connErr != nil {
				logger.Tracef(ctx, "unable to connect to the libav input '%s': %w", avInputAddr, err)
				continue
			}

			c.AVConn = avInputConn
			return nil
		}
	}
}

func (c *ConnectionRTMP[N]) onInitFinished(
	ctx context.Context,
) {
	appName := c.GetAppName()
	rtmpCtx := c.AVRTMPContext()
	logger.Debugf(ctx, "updating the app name: '%s' -> '%s'", rtmpCtx.App(), appName)
	rtmpCtx.SetApp(appName)
	close(c.InitFinished)
}

func (c *ConnectionRTMP[N]) negotiate(
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
			logger.Tracef(ctx, "waiting for c.AVInputConn input...")
			r, err := c.AVConn.Read(buf[:])
			logger.Tracef(ctx, "/waiting for c.AVInputConn input: %v %v", r, err)
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
				logger.Tracef(ctx, "waiting for c.AVInputConn output...")
				err := forward(c.AVConn, msg)
				logger.Tracef(ctx, "/waiting for c.AVInputConn output")
				if err != nil {
					errCh <- err
					return
				}
				continue
			}

			appName, err := parseAppName(ctx, msg)
			if err != nil {
				errCh <- fmt.Errorf("unable to parse the app name from the 'connect' message: %w", err)
				return
			}
			c.AppName = ptr(string(appName))
			logger.Debugf(ctx, "appName == '%s'", *c.AppName)

			routePath := *c.AppName
			ctx = belt.WithField(ctx, "path", routePath)
			route, err := c.Port.GetServer().GetRoute(ctx, routePath, GetRouteModeCreateIfNotFound)
			if err != nil {
				errCh <- fmt.Errorf("unable to create a route '%s': %w", routePath, err)
				return
			}
			c.Route = route
			switch c.Mode() {
			case RTMPModePublishers:
				if err := route.addPublisherIfNoPublishers(ctx, c); err != nil {
					errCh <- fmt.Errorf("unable to add myself as a  to '%s': %w", routePath, err)
					return
				}
				c.Node.AddPushPacketsTo(route.Node)
			case types.RTMPModeConsumers:
				c.Route.Node.AddPushPacketsTo(c.Node)
			}
			observability.Go(ctx, func() {
				errCh := make(chan avpipeline.ErrNode, 100)
				defer close(errCh)
				observability.Go(ctx, func() {
					for err := range errCh {
						logger.Errorf(ctx, "got an error: %v", err)
					}
				})
				<-c.InitFinished
				if c.InitError != nil {
					logger.Debugf(ctx, "not running Serve, because of InitError: %v", c.InitError)
					return
				}
				logger.Debugf(ctx, "resulting graph: %s", c.Node.DotString(false))
				serveCtx := belt.WithField(origCtx, "path", routePath)
				c.Node.Serve(serveCtx, avpipeline.ServeConfig{}, errCh)
			})

			logger.Tracef(ctx, "waiting for c.AVInputConn output...")
			err = forward(c.AVConn, msg)
			logger.Tracef(ctx, "/waiting for c.AVInputConn output")
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
		logger.Debugf(ctx, "setting a deadline in the past for c.AVInputConn")
		if err := c.AVConn.SetReadDeadline(time.Unix(1, 0)); err != nil {
			logger.Errorf(ctx, "unable to set the read deadline for AVInputConn: %v", err)
		}
		return err
	}
}

func (c *ConnectionRTMP[N]) getKernel() kernel.Abstract {
	switch p := c.Node.GetProcessor().(type) {
	case *processor.FromKernel[*kernel.Input]:
		return p.Kernel
	case *processor.FromKernel[*kernel.Retry[*kernel.Output]]:
		return p.Kernel
	default:
		panic(fmt.Errorf("unexpected type: %T", p))
	}
}

func (c *ConnectionRTMP[N]) getFormatContext() *astiav.FormatContext {
	switch k := c.getKernel().(type) {
	case *kernel.Input:
		return k.FormatContext
	case *kernel.Retry[*kernel.Output]:
		return k.Kernel.FormatContext
	default:
		panic(fmt.Errorf("unexpected type: %T", k))
	}
}

func (c *ConnectionRTMP[N]) AVRTMPContext() *avcommon.RTMPContext {
	fmtCtx := avcommon.WrapAVFormatContext(
		xastiav.CFromAVFormatContext(
			c.getFormatContext(),
		),
	)
	avioCtx := fmtCtx.Pb()
	urlCtx := avcommon.WrapURLContext(avioCtx.Opaque())
	return avcommon.WrapRTMPContext(urlCtx.PrivData())
}

func (c *ConnectionRTMP[N]) GetAppName() string {
	if ConnectionRTMPEnableAppNameUpdaterHack {
		return *c.AppName
	} else {
		return c.AVRTMPContext().App()
	}
}

func (c *ConnectionRTMP[N]) forward(
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

			logger.Tracef(ctx, "waiting for AVInputConn input...")
			r, err := c.AVConn.Read(buf[:])
			logger.Tracef(ctx, "/waiting for AVInputConn input")
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
			logger.Tracef(ctx, "waiting for c.AVInputConn output...")
			err = forward(c.AVConn, msg)
			logger.Tracef(ctx, "/waiting for c.AVInputConn output")
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
	ConnectionRTMPEnableAppNameUpdaterHack = true
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
