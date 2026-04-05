// on_demand.go wires an on-demand forwarding's activator onto the
// destination route's consumer hooks.

package configapplier

import (
	"context"
	"fmt"
	"time"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avd/pkg/avd"
	"github.com/xaionaro-go/avpipeline/router"
)

// wireOnDemandLocalForwarding takes a freshly-created local
// RouteForwarding whose inner transcoder has just been started eagerly,
// stops that transcoder, and attaches an OnDemandConsumerTracker to the
// destination route's consumer hooks so the transcoder resumes whenever
// a consumer attaches and stops again after idleTimeout of zero
// consumers.
//
// Known limitation: if the source route's underlying node is closed
// and re-opened, RouteForwarding internally rebuilds the StreamForwarder
// and starts it eagerly. The OnDemandConsumerTracker still holds the
// original RouteForwarding pointer; since that pointer exposes
// Activate/Deactivate by delegating to the current StreamForwarder, the
// rebuilt forwarder is still controllable — but the eager start
// performed during the rebuild is not prevented. Live-reconfig callers
// should re-apply on-demand wiring after a source restart.
func wireOnDemandLocalForwarding(
	ctx context.Context,
	fwd *router.RouteForwarding[avd.RouteCustomData],
	idleTimeout time.Duration,
) error {
	logger.Debugf(ctx, "wireOnDemandLocalForwarding(idleTimeout=%s)", idleTimeout)

	dstRoute := fwd.GetOutputRoute(ctx)
	if dstRoute == nil {
		return fmt.Errorf("forwarding has no destination route to attach consumer hooks to")
	}
	if err := fwd.Deactivate(ctx); err != nil {
		return fmt.Errorf("unable to deactivate eagerly-started forwarder: %w", err)
	}
	tracker := router.NewOnDemandConsumerTracker(fwd, idleTimeout)
	dstRoute.Locker().Do(ctx, func() {
		dstRoute.OnConsumerAdded = func(
			ctx context.Context,
			_ *router.Route[avd.RouteCustomData],
			_ router.Consumer[avd.RouteCustomData],
		) {
			if err := tracker.OnConsumerAdded(ctx); err != nil {
				logger.Errorf(ctx, "on-demand activation failed for route '%s': %v", dstRoute.Path, err)
			}
		}
		dstRoute.OnConsumerRemoved = func(
			ctx context.Context,
			_ *router.Route[avd.RouteCustomData],
			_ router.Consumer[avd.RouteCustomData],
		) {
			if err := tracker.OnConsumerRemoved(ctx); err != nil {
				logger.Errorf(ctx, "on-demand deactivation tracking failed for route '%s': %v", dstRoute.Path, err)
			}
		}
	})
	logger.Debugf(ctx, "on-demand wiring installed for destination route '%s'", dstRoute.Path)
	return nil
}
