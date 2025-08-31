package avd

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync/atomic"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/go-ng/xatomic"
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
	IsForwarding  atomic.Bool
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
			OnPostOpen: func(ctx context.Context, i *kernel.Input) error {
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
		observability.Go(ctx, func(ctx context.Context) {
			c.Close(ctx)
		})
		return err
	}
	c.SetNodeTyped(newProxiedInputNode(ctx, c, input))
	return nil
}

func (c *ConnectionProxiedHandlerPublisher) GetPublishMode(
	ctx context.Context,
) router.PublishMode {
	port := c.Parent.GetPort()
	if port == nil {
		return router.UndefinedPublishMode
	}
	return port.Config.GetPublishMode()
}

func (c *ConnectionProxiedHandlerPublisher) GetInputNode(
	context.Context,
) node.Abstract {
	return c.GetNode()
}

func (c *ConnectionProxiedHandlerPublisher) GetNode() node.Abstract {
	n := c.GetNodeTyped()
	if n == nil {
		return nil
	}
	return n
}

func (c *ConnectionProxiedHandlerPublisher) GetNodeTyped() *NodeInputProxied {
	return xatomic.LoadPointer(&c.Node)
}

func (c *ConnectionProxiedHandlerPublisher) SetNodeTyped(n *NodeInputProxied) {
	xatomic.StorePointer(&c.Node, n)
}

func (c *ConnectionProxiedHandlerPublisher) GetOutputRoute(
	context.Context,
) *router.Route[RouteCustomData] {
	asRouteSource := c.GetAsRouteSource()
	if asRouteSource == nil {
		return nil
	}
	return asRouteSource.Output
}

func (c *ConnectionProxiedHandlerPublisher) StartForwarding(
	ctx context.Context,
) error {
	routePath := *c.Parent.RoutePath
	port := c.Parent.GetPort()
	if port == nil {
		return fmt.Errorf("port is nil, unable to start forwarding")
	}
	n := c.GetNodeTyped()
	if n == nil {
		return fmt.Errorf("node is nil, unable to start forwarding")
	}
	publishMode := c.GetPublishMode(ctx)
	if publishMode == router.UndefinedPublishMode {
		return fmt.Errorf("publish mode is undefined, unable to start forwarding")
	}
	routeSource, err := router.AddRouteSource(
		ctx,
		port.Server.Router,
		n,
		routePath,
		publishMode,
		nil,
		c.onRouteSourcePostStart,
		c.onRouteSourcePreStop,
		c.onRouteSourcePostStop,
	)
	if err != nil {
		return fmt.Errorf("unable to add a source to route '%s': %w", routePath, err)
	}
	c.SetAsRouteSource(routeSource)

	return nil
}

func (c *ConnectionProxiedHandlerPublisher) SetAsRouteSource(
	rs *RouteSource[*ConnectionProxiedHandlerPublisher],
) {
	if c == nil {
		return
	}
	xatomic.StorePointer(&c.AsRouteSource, rs)
}

func (c *ConnectionProxiedHandlerPublisher) GetAsRouteSource() *RouteSource[*ConnectionProxiedHandlerPublisher] {
	if c == nil {
		return nil
	}
	return xatomic.LoadPointer(&c.AsRouteSource)
}

func (c *ConnectionProxiedHandlerPublisher) SetIsForwarding(
	isForwarding bool,
) bool {
	if c == nil {
		return false
	}

	return c.IsForwarding.Swap(isForwarding) != isForwarding
}

func (c *ConnectionProxiedHandlerPublisher) onRouteSourcePostStart(
	ctx context.Context,
	rs *RouteSource[*ConnectionProxiedHandlerPublisher],
) {
	logger.Debugf(ctx, "onRouteSourcePostStart")
	defer func() { logger.Debugf(ctx, "/onRouteSourcePostStart") }()
	c.SetIsForwarding(true)
	observability.Go(ctx, func(ctx context.Context) {
		s := c.GetNodeTyped().DotString(false)
		logger.Debugf(ctx, "onRouteSourcePostStart: pipeline: %s", s)
	})
}

func (c *ConnectionProxiedHandlerPublisher) onRouteSourcePreStop(
	ctx context.Context,
	rs *RouteSource[*ConnectionProxiedHandlerPublisher],
) {
	logger.Debugf(ctx, "onRouteSourcePreStop")
	defer func() { logger.Debugf(ctx, "/onRouteSourcePreStop") }()
	if !c.SetIsForwarding(false) {
		return
	}
	port := c.Parent.GetPort()
	if port == nil {
		logger.Debugf(ctx, "onRouteSourcePreStop: port is nil, skipping closing the publisher")
		return
	}
	err := PublisherClose(ctx, c, port.Config.OnEndAction)
	if err != nil {
		logger.Errorf(ctx, "unable to close the publisher: %v", err)
	}
}

func (c *ConnectionProxiedHandlerPublisher) onRouteSourcePostStop(
	ctx context.Context,
	rs *RouteSource[*ConnectionProxiedHandlerPublisher],
) {
	logger.Debugf(ctx, "onRouteSourcePostStop")
	defer func() { logger.Debugf(ctx, "/onRouteSourcePostStop") }()
	observability.Go(ctx, func(ctx context.Context) {
		s := c.GetNodeTyped().DotString(false)
		logger.Debugf(ctx, "onRouteSourcePostStop: pipeline: %s", s)
	})
}

func (c *ConnectionProxiedHandlerPublisher) GetKernel() kernel.Abstract {
	n := c.GetNodeTyped()
	if n == nil {
		return nil
	}
	return n.Processor.Kernel
}

func (c *ConnectionProxiedHandlerPublisher) Close(ctx context.Context) (_err error) {
	logger.Debugf(ctx, "Close()")
	defer logger.Debugf(ctx, "/Close(): %v", _err)
	return xsync.DoA1R1(ctx, &c.Locker, c.closeLocked, ctx)
}

func (c *ConnectionProxiedHandlerPublisher) closeLocked(ctx context.Context) (_err error) {
	logger.Debugf(ctx, "closeLocked")
	defer func() { logger.Debugf(ctx, "/closeLocked: %v", _err) }()
	var errs []error

	if asRouteSource := c.GetAsRouteSource(); asRouteSource != nil {
		if err := asRouteSource.Stop(ctx); err != nil {
			errs = append(errs, fmt.Errorf("unable to stop stream forwarding (publisher): %w", err))
		}
		c.SetAsRouteSource(nil)
	}
	if n := c.GetNodeTyped(); n != nil {
		if err := n.GetProcessor().Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("unable to close the node processor: %w", err))
		}
		c.SetNodeTyped(nil)
	}
	return errors.Join(errs...)
}

func (c *ConnectionProxiedHandlerPublisher) SetURL(
	ctx context.Context,
	url *url.URL,
) {
	n := c.GetNodeTyped()
	if n == nil {
		logger.Errorf(ctx, "SetURL: node is nil, unable to set URL")
		return
	}
	n.Processor.Kernel.URL = url.String()
}
