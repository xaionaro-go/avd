package avd

import (
	"context"
	"errors"
	"fmt"
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

type ConnectionProxiedHandlerConsumer struct {
	Parent            *ConnectionProxied
	Locker            xsync.Mutex
	Node              *NodeOutputProxied
	Route             *router.Route[RouteCustomData]
	ConsumerForwarder *router.StreamForwarderCopy[router.GoBug63285RouteInterface[RouteCustomData], *router.ProcessorRouting]
}

var _ ConnectionProxiedHandler = (*ConnectionProxiedHandlerConsumer)(nil)

func newConnectionProxiedConsumer(
	parent *ConnectionProxied,
) *ConnectionProxiedHandlerConsumer {
	return &ConnectionProxiedHandlerConsumer{
		Parent: parent,
	}
}

func (c *ConnectionProxiedHandlerConsumer) InitAVHandler(
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
			AsyncOpen:     c.Parent.isAsyncOpen(ctx),
			OnOpened: func(ctx context.Context, i *kernel.Output) error {
				if !c.Parent.isAsyncOpen(ctx) {
					return nil
				}
				c.Parent.onInitFinished(ctx)
				return nil
			},
		},
	)
	if err != nil {
		err = fmt.Errorf("unable to start listening '%s' using libav: %w", url.String(), err)
		logger.Errorf(ctx, "%v", err)
		c.Parent.InitError = err
		close(c.Parent.InitFinished)
		observability.Go(ctx, func() {
			c.Close(ctx)
		})
		return err
	}
	c.Node = node
	return nil
}

func (c *ConnectionProxiedHandlerConsumer) GetNode() node.Abstract {
	return c.Node
}

func (c *ConnectionProxiedHandlerConsumer) StartForwarding(
	ctx context.Context,
) (_err error) {
	logger.Debugf(ctx, "StartForwarding")
	defer func() { logger.Debugf(ctx, "/StartForwarding: %v", _err) }()

	routePath := *c.Parent.RoutePath
	route, err := c.Parent.Port.GetServer().GetRoute(ctx, routePath, router.GetRouteModeWaitForPublisher)
	if err != nil {
		return err
	}
	c.Route = route

	return c.startForwarding(ctx, route.Node, c.Node)
}

func (c *ConnectionProxiedHandlerConsumer) startForwarding(
	ctx context.Context,
	src *NodeRouting,
	dst node.Abstract,
) (_err error) {
	logger.Debugf(ctx, "startForwarding(%s, %s)", src, dst)
	defer func() { logger.Debugf(ctx, "/startForwarding(%s, %s): %v", src, dst, _err) }()

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

func (c *ConnectionProxiedHandlerConsumer) GetKernel() kernel.Abstract {
	return c.Node.Processor.Kernel
}

func (c *ConnectionProxiedHandlerConsumer) Close(ctx context.Context) (_err error) {
	logger.Debugf(ctx, "Close()")
	defer logger.Debugf(ctx, "/Close(): %v", _err)
	return xsync.DoA1R1(ctx, &c.Locker, c.closeLocked, ctx)
}

func (c *ConnectionProxiedHandlerConsumer) closeLocked(ctx context.Context) (_err error) {
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
	if c.Node != nil {
		if err := c.Node.GetProcessor().Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("unable to close the node processor: %w", err))
		}
		c.Node = nil
	}
	return errors.Join(errs...)
}

func (c *ConnectionProxiedHandlerConsumer) SetURL(
	ctx context.Context,
	url *url.URL,
) {
	c.Node.Processor.Kernel.URL = url.String()
	c.Node.Processor.Kernel.URLParsed = url
}
