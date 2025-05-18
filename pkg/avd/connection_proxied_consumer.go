package avd

import (
	"context"
	"fmt"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avpipeline/node"
	"github.com/xaionaro-go/avpipeline/router"
)

func (c *ConnectionProxied[N]) setConsumingRoute(
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

func (c *ConnectionProxied[N]) startStreamForwardConsumer(
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
