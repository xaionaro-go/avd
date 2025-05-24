package avd

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"github.com/facebookincubator/go-belt/tool/experimental/errmon"
	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avd/pkg/avd/types"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/node"
	"github.com/xaionaro-go/avpipeline/router"
	"github.com/xaionaro-go/observability"
	"github.com/xaionaro-go/secret"
	"github.com/xaionaro-go/xsync"
)

type ConnectionProxiedHandlerPublisher struct {
	Parent        *ConnectionProxied
	Locker        xsync.Mutex
	Node          *NodeInputProxied
	AsRouteSource *RouteSource[*ConnectionProxiedHandlerPublisher]
}

var _ ConnectionProxiedHandler = (*ConnectionProxiedHandlerPublisher)(nil)

func newConnectionProxiedPublisher(
	parent *ConnectionProxied,
) *ConnectionProxiedHandlerPublisher {
	return &ConnectionProxiedHandlerPublisher{
		Parent: parent,
	}
}

func (c *ConnectionProxiedHandlerPublisher) String() string {
	return c.Parent.String()
}

func (c *ConnectionProxiedHandlerPublisher) InitAVHandler(
	ctx context.Context,
	url *url.URL,
	secretKey secret.String,
	customOpts ...types.DictionaryItem,
) error {
	input, err := kernel.NewInputFromURL(
		ctx,
		url.String(),
		secretKey,
		kernel.InputConfig{
			CustomOptions: customOpts,
			AsyncOpen:     c.Parent.isAsyncOpen(ctx),
			OnOpened: func(ctx context.Context, i *kernel.Input) error {
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
	c.Node = newProxiedInputNode(ctx, c, input)
	return nil
}

func (c *ConnectionProxiedHandlerPublisher) GetPublishMode(
	ctx context.Context,
) router.PublishMode {
	return c.Parent.Port.Config.PublishMode
}

func (c *ConnectionProxiedHandlerPublisher) GetInputNode(
	context.Context,
) node.Abstract {
	return c.Node
}

func (c *ConnectionProxiedHandlerPublisher) GetNode() node.Abstract {
	return c.Node
}

func (c *ConnectionProxiedHandlerPublisher) GetOutputRoute(
	context.Context,
) *router.Route[RouteCustomData] {
	return c.AsRouteSource.Output
}

func (c *ConnectionProxiedHandlerPublisher) StartForwarding(
	ctx context.Context,
) error {
	routePath := *c.Parent.RoutePath
	routeSource, err := router.AddRouteSource(
		ctx,
		c.Parent.Port.Server.Router,
		c.Node,
		routePath,
		c.Parent.Port.Config.PublishMode,
		nil,
		c.onRouteSourceStart,
		c.onRouteSourceStop,
	)
	if err != nil {
		return fmt.Errorf("unable to add a source to route '%s': %w", routePath, err)
	}

	c.AsRouteSource = routeSource
	err = routeSource.Start(ctx)
	if err != nil {
		errmon.ObserveErrorCtx(ctx, routeSource.Close(ctx))
		return fmt.Errorf("unable to start sourcing data to the route '%s': %w", routePath, err)
	}

	return nil
}

func (c *ConnectionProxiedHandlerPublisher) onRouteSourceStart(
	ctx context.Context,
	rs *RouteSource[*ConnectionProxiedHandlerPublisher],
) {
	logger.Debugf(ctx, "onRouteSourceStart")
	defer func() { logger.Debugf(ctx, "/onRouteSourceStart") }()
}

func (c *ConnectionProxiedHandlerPublisher) onRouteSourceStop(
	ctx context.Context,
	rs *RouteSource[*ConnectionProxiedHandlerPublisher],
) {
	logger.Debugf(ctx, "onRouteSourceStop")
	defer func() { logger.Debugf(ctx, "/onRouteSourceStop") }()
	err := PublisherClose(ctx, c, c.Parent.Port.Config.OnEndAction)
	if err != nil {
		logger.Errorf(ctx, "unable to close the publisher: %v", err)
	}
}

func (c *ConnectionProxiedHandlerPublisher) GetKernel() kernel.Abstract {
	return c.Node.Processor.Kernel
}

func (c *ConnectionProxiedHandlerPublisher) Close(ctx context.Context) (_err error) {
	logger.Debugf(ctx, "Close()")
	defer logger.Debugf(ctx, "/Close(): %v", _err)
	return xsync.DoA1R1(ctx, &c.Locker, c.closeLocked, ctx)
}

func (c *ConnectionProxiedHandlerPublisher) closeLocked(ctx context.Context) (_err error) {
	var errs []error
	if c.AsRouteSource != nil {
		if err := c.AsRouteSource.Stop(ctx); err != nil {
			errs = append(errs, fmt.Errorf("unable to stop stream forwarding (publisher): %w", err))
		}
		c.AsRouteSource = nil
	}
	if c.Node != nil {
		if err := c.Node.GetProcessor().Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("unable to close the node processor: %w", err))
		}
		c.Node = nil
	}
	return errors.Join(errs...)
}

func (c *ConnectionProxiedHandlerPublisher) SetURL(
	ctx context.Context,
	url *url.URL,
) {
	c.Node.Processor.Kernel.URL = url.String()
}
