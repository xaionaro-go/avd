package avd

import (
	"context"
	"errors"
	"fmt"
	"net"
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

type ConnectionProxiedPublisher struct {
	*ConnectionProxiedCommons
	Node          *NodeInputProxied
	AsRouteSource *RouteSource[*ConnectionProxiedPublisher]
}

var _ ConnectionProxied = (*ConnectionProxiedPublisher)(nil)

func newConnectionProxiedPublisher(
	ctx context.Context,
	p *ListeningPortProxied,
	conn net.Conn,
) (*ConnectionProxiedPublisher, error) {
	c := &ConnectionProxiedPublisher{}
	commons, err := newConnectionProxiedCommons(ctx, p, conn, c)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize the commons: %w", err)
	}
	c.ConnectionProxiedCommons = commons
	panic("start?")
	return c, nil
}

func (c *ConnectionProxiedPublisher) InitAVHandler(
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
	c.Node = newProxiedInputNode(ctx, c, input)
	return nil
}

func (c *ConnectionProxiedPublisher) GetPublishMode(
	ctx context.Context,
) router.PublishMode {
	return c.Port.Config.PublishMode
}

func (c *ConnectionProxiedPublisher) GetInputNode(
	context.Context,
) node.Abstract {
	return c.Node
}

func (c *ConnectionProxiedPublisher) GetOutputRoute(
	context.Context,
) *router.Route[RouteCustomData] {
	return c.AsRouteSource.Output
}

func (c *ConnectionProxiedPublisher) StartForwarding(
	ctx context.Context,
) error {
	routePath := *c.RoutePath
	routeSource, err := router.AddRouteSource(
		ctx,
		c.Port.Server.Router,
		c.Node,
		routePath,
		c.Port.Config.PublishMode,
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

func (c *ConnectionProxiedPublisher) onRouteSourceStart(
	ctx context.Context,
	rs *RouteSource[*ConnectionProxiedPublisher],
) {
	logger.Debugf(ctx, "onRouteSourceStart")
	defer func() { logger.Debugf(ctx, "/onRouteSourceStart") }()
}

func (c *ConnectionProxiedPublisher) onRouteSourceStop(
	ctx context.Context,
	rs *RouteSource[*ConnectionProxiedPublisher],
) {
	logger.Debugf(ctx, "onRouteSourceStop")
	defer func() { logger.Debugf(ctx, "/onRouteSourceStop") }()
	err := PublisherClose(ctx, c, c.Port.Config.OnEndAction)
	if err != nil {
		logger.Errorf(ctx, "unable to close the publisher: %v", err)
	}
}

func (c *ConnectionProxiedPublisher) GetKernel() kernel.Abstract {
	return c.Node.Processor.Kernel
}

func (c *ConnectionProxiedPublisher) Close(ctx context.Context) (_err error) {
	logger.Debugf(ctx, "Close()")
	defer logger.Debugf(ctx, "/Close(): %v", _err)
	c.CancelFunc()
	return xsync.DoR1(ctx, &c.Locker, func() error {
		var errs []error
		if c.AsRouteSource != nil {
			if err := c.AsRouteSource.Stop(ctx); err != nil {
				errs = append(errs, fmt.Errorf("unable to stop stream forwarding (publisher): %w", err))
			}
			c.AsRouteSource = nil
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

func (c *ConnectionProxiedPublisher) SetURL(
	ctx context.Context,
	url *url.URL,
) {
	c.Node.Processor.Kernel.URL = c.AVInputURL.String()
}
