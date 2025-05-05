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

	"github.com/facebookincubator/go-belt"
	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avcommon"
	xastiav "github.com/xaionaro-go/avcommon/astiav"
	"github.com/xaionaro-go/avpipeline"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/types"
	"github.com/xaionaro-go/observability"
	"github.com/xaionaro-go/secret"
	"github.com/xaionaro-go/xsync"
)

const (
	connectionRTMPPublisherEnableAppNameUpdaterHack = true
)

type ConnectionRTMPPublisher struct {
	Locker xsync.Mutex

	// access only when Locker is locked (but better don't access at all if you are not familiar with the code):
	Port         *ListeningPortRTMPPublisher
	Conn         net.Conn
	CancelFunc   context.CancelFunc
	AVInputURL   *url.URL
	AVInputKey   secret.String
	AVInputConn  *net.TCPConn
	Node         *NodeInput
	InitError    error
	InitFinished chan struct{}
	AppName      *string
	Route        *Route
}

var _ Publisher = (*ConnectionRTMPPublisher)(nil)

func newConnectionRTMPPublisher(
	ctx context.Context,
	p *ListeningPortRTMPPublisher,
	conn net.Conn,
) (_ret *ConnectionRTMPPublisher, _err error) {
	ctx, cancelFn := context.WithCancel(ctx)
	ctx = belt.WithField(ctx, "remote_addr", conn.RemoteAddr())
	c := &ConnectionRTMPPublisher{
		Port:         p,
		Conn:         conn,
		CancelFunc:   cancelFn,
		InitFinished: make(chan struct{}),
	}
	logger.Debugf(ctx, "newConnectionRTMP")
	defer func() { logger.Debugf(ctx, "/newConnectionRTMP: %v %v", _ret, _err) }()
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
		if connectionRTMPPublisherEnableAppNameUpdaterHack {
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

func (c *ConnectionRTMPPublisher) String() string {
	return fmt.Sprintf("RTMP-publisher(%s->%s->%s->%s)", c.Conn.RemoteAddr(), c.Conn.LocalAddr(), c.AVInputConn.LocalAddr(), c.AVInputConn.RemoteAddr())
}

func (c *ConnectionRTMPPublisher) GetNode(
	context.Context,
) *NodeInput {
	return c.Node
}
func (c *ConnectionRTMPPublisher) GetRoute(
	context.Context,
) *Route {
	return c.Route
}

func (c *ConnectionRTMPPublisher) Close(ctx context.Context) (_err error) {
	logger.Debugf(ctx, "Close()")
	defer logger.Debugf(ctx, "/Close(): %v", _err)
	c.CancelFunc()
	return xsync.DoR1(ctx, &c.Locker, func() error {
		var errs []error
		if c.Route != nil {
			if _, err := c.Route.removePublisher(ctx, c); err != nil {
				errs = append(errs, fmt.Errorf("unable to remove myself as a publisher at '%s': %w", c.Route.Path, err))
			}
			c.Route = nil
		}
		if c.AVInputConn != nil {
			c.AVInputConn.SetDeadline(time.Unix(1, 0))
			c.AVInputConn = nil
		}
		if c.Conn != nil {
			c.Conn.SetDeadline(time.Unix(1, 0))
			c.Conn = nil
		}
		if c.Node != nil {
			if err := c.Node.Processor.Close(ctx); err != nil {
				errs = append(errs, fmt.Errorf("unable to close the input node processor: %w", err))
			}
			c.Node = nil
		}
		return errors.Join(errs...)
	})
}

func (c *ConnectionRTMPPublisher) initRTMPHandler(
	ctx context.Context,
) (_err error) {
	logger.Debugf(ctx, "initRTMPHandler")
	defer func() { logger.Debugf(ctx, "/initRTMPHandler: %v", _err) }()

	listenURL := "rtmp://127.0.0.1:53423/avd-input/"
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

	logger.Debugf(ctx, "attempting to listen by libav at %s...", url)
	input, err := kernel.NewInputFromURL(
		ctx,
		url.String(),
		secretKey,
		kernel.InputConfig{
			CustomOptions: types.DictionaryItems{
				//{Key: "f", Value: "flv"},
				{Key: "listen", Value: "1"},
			},
			AsyncOpen: true,
			OnOpened: func(ctx context.Context, i *kernel.Input) error {
				appName := c.GetAppName()
				rtmpCtx := c.AVRTMPContext()
				logger.Debugf(ctx, "updating the app name: '%s' -> '%s'", rtmpCtx.App(), appName)
				rtmpCtx.SetApp(appName)
				close(c.InitFinished)
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
	c.Node = newInputNode(ctx, c, input)

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

			c.AVInputConn = avInputConn
			return nil
		}
	}
}

func (c *ConnectionRTMPPublisher) negotiate(
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
			r, err := c.AVInputConn.Read(buf[:])
			logger.Tracef(ctx, "/waiting for c.AVInputConn input: %v %v", r, err)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					// revert back:
					c.AVInputConn.SetDeadline(time.Time{})
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
				err := forward(c.AVInputConn, msg)
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
			route, err := c.Port.Server.GetRoute(ctx, routePath, GetRouteModeCreateIfNotFound)
			if err != nil {
				errCh <- fmt.Errorf("unable to create a route '%s': %w", routePath, err)
				return
			}
			if err := route.addPublisherIfNoPublishers(ctx, c); err != nil {
				errCh <- fmt.Errorf("unable to add myself as a publisher to '%s': %w", routePath, err)
				return
			}
			c.Route = route
			c.Node.AddPushPacketsTo(route.Node)
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
			err = forward(c.AVInputConn, msg)
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
		if err := c.AVInputConn.SetReadDeadline(time.Unix(1, 0)); err != nil {
			logger.Errorf(ctx, "unable to set the read deadline for AVInputConn: %v", err)
		}
		return err
	}
}

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

func (c *ConnectionRTMPPublisher) AVRTMPContext() *avcommon.RTMPContext {
	fmtCtx := avcommon.WrapAVFormatContext(
		xastiav.CFromAVFormatContext(
			c.Node.Processor.Kernel.FormatContext,
		),
	)
	avioCtx := fmtCtx.Pb()
	urlCtx := avcommon.WrapURLContext(avioCtx.Opaque())
	return avcommon.WrapRTMPContext(urlCtx.PrivData())
}

func (c *ConnectionRTMPPublisher) GetAppName() string {
	if connectionRTMPPublisherEnableAppNameUpdaterHack {
		return *c.AppName
	} else {
		return c.AVRTMPContext().App()
	}
}

func (c *ConnectionRTMPPublisher) forward(
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
			r, err := c.AVInputConn.Read(buf[:])
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
			err = forward(c.AVInputConn, msg)
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
