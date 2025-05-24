package avd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avd/pkg/avd/types"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/node"
	"github.com/xaionaro-go/avpipeline/router"
	"github.com/xaionaro-go/observability"
	"github.com/xaionaro-go/secret"
	"github.com/xaionaro-go/xsync"
)

type ConnectionProxiedConsumer struct {
	*ConnectionProxiedCommons
	Node              *NodeOutputProxied
	Route             *router.Route[RouteCustomData]
	ConsumerForwarder *router.StreamForwarderCopy[*router.Route[RouteCustomData], *router.ProcessorRouting]
}

var _ ConnectionProxied = (*ConnectionProxiedConsumer)(nil)

func newConnectionProxiedConsumer(
	ctx context.Context,
	p *ListeningPortProxied,
	conn net.Conn,
) (*ConnectionProxiedConsumer, error) {
	c := &ConnectionProxiedConsumer{}
	commons, err := newConnectionProxiedCommons(ctx, p, conn, c)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize the commons: %w", err)
	}
	c.ConnectionProxiedCommons = commons
	panic("start?")
	return c, nil
}

func (c *ConnectionProxiedConsumer) InitAVHandler(
	ctx context.Context,
	url *url.URL,
	secretKey secret.String,
	customOpts ...types.DictionaryItem,
) error {
	node, err := newProxiedOutputNode(
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
	c.Node = node
	return nil
}

func (c *ConnectionProxiedConsumer) setConsumingRoute(
	ctx context.Context,
) error {
	routePath := *c.RoutePath
	route, err := c.Port.GetServer().GetRoute(ctx, routePath, router.GetRouteModeWaitForPublisher)
	if err != nil {
		return err
	}
	c.Route = route
	return nil
}

func (c *ConnectionProxiedConsumer) startStreamForwardConsumer(
	ctx context.Context,
	src *NodeRouting,
	dst node.Abstract,
) (_err error) {
	logger.Debugf(ctx, "startStreamForwardConsumer(%s, %s)", src, dst)
	defer func() { logger.Debugf(ctx, "/startStreamForwardConsumer(%s, %s): %v", src, dst, _err) }()

	fwd, err := router.NewStreamForwarderCopy(ctx, src, dst)
	if err != nil {
		return fmt.Errorf("unable to make a new stream forwarder from %s to %s: %w", src, dst, err)
	}

	c.ConsumerForwarder = fwd
	if err := fwd.Start(ctx); err != nil {
		return fmt.Errorf("unable to start forwarding the traffic from %s to %s: %w", src, dst, err)
	}
	return nil
}

func (c *ConnectionProxiedConsumer) GetKernel() kernel.Abstract {
	return c.Node.Processor.Kernel
}

func (c *ConnectionProxiedConsumer) Close(ctx context.Context) (_err error) {
	logger.Debugf(ctx, "Close()")
	defer logger.Debugf(ctx, "/Close(): %v", _err)
	c.CancelFunc()
	return xsync.DoR1(ctx, &c.Locker, func() error {
		var errs []error
		if c.ConsumerForwarder != nil {
			if err := c.ConsumerForwarder.Stop(ctx); err != nil {
				errs = append(errs, fmt.Errorf("unable to stop stream forwarding (consumer): %w", err))
			}
			c.ConsumerForwarder = nil
		}
		if c.Route != nil {
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
		if c.ConnectionProxiedCommons != nil {
			if err := c.ConnectionProxiedCommons.closeCommonsLocked(ctx); err != nil {
				errs = append(errs, fmt.Errorf("unable to close the conn-commons: %w", err))
			}
			c.ConnectionProxiedCommons = nil
		}
		return errors.Join(errs...)
	})
}

func (c *ConnectionProxiedConsumer) SetURL(
	ctx context.Context,
	url *url.URL,
) {
	c.Node.Processor.Kernel.URL = c.AVInputURL.String()
	c.Node.Processor.Kernel.URLParsed = c.AVInputURL
}
