package avd

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avpipeline/router"
	"github.com/xaionaro-go/observability"
)

func (c *ConnectionProxied[N]) setPublishingRouteAndStartPublishing(
	ctx context.Context,
) error {
	routePath := *c.RoutePath
	for {
		route, err := c.Port.GetServer().GetRoute(ctx, routePath, router.GetRouteModeCreateIfNotFound)
		if err != nil {
			return fmt.Errorf("unable to create a route '%s': %w", routePath, err)
		}
		c.Route = route
		logger.Debugf(ctx, "AddPublisher")
		var wg sync.WaitGroup
		route.LockDo(ctx, func(ctx context.Context) {
			for _, publisher := range route.Publishers {
				wg.Add(1)
				observability.Go(ctx, func() {
					defer wg.Done()
					publisher.Close(ctx)
				})
			}
		})
		wg.Wait()
		// TODO: resolve the race condition between LockDo above and AddPublisher below
		if err := route.AddPublisher(ctx, c); err != nil {
			if errors.Is(err, router.ErrRouteClosed{}) {
				logger.Debugf(ctx, "the route is closed, trying to re-find it")
				continue
			}
			return fmt.Errorf("unable to add myself as a  to '%s': %w", routePath, err)
		}
		err = c.startStreamForwardPublisher(ctx, any(c.Node).(*NodeInput), c.Route.Node)
		if err != nil {
			return fmt.Errorf("unable to forward the stream: %w", err)
		}
		break
	}

	return nil
}

func (c *ConnectionProxied[N]) startStreamForwardPublisher(
	ctx context.Context,
	src *NodeInput,
	dst *NodeRouting,
) (_err error) {
	logger.Debugf(ctx, "startStreamForwardPublisher(%s, %s)", src, dst)
	defer func() { logger.Debugf(ctx, "/startStreamForwardPublisher(%s, %s): %v", src, dst, _err) }()

	fwd, err := router.NewStreamForwarderCopy(ctx, src, dst)
	if err != nil {
		return fmt.Errorf("unable to make a new stream forwarder from %s to %s: %w", src, dst, err)
	}

	c.PublisherForwarder = fwd
	if err := fwd.Start(ctx); err != nil {
		return fmt.Errorf("unable to start forwarding the traffic from %s to %s: %w", src, dst, err)
	}
	return nil
}
