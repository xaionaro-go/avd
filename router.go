package avd

import (
	"context"
	"sync"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avpipeline"
	"github.com/xaionaro-go/observability"
	"github.com/xaionaro-go/xcontext"
	"github.com/xaionaro-go/xsync"
)

type Router struct {
	Locker       xsync.Mutex
	RoutesByPath map[string]*Route
	CloseChan    chan struct{}
	ErrorChan    chan avpipeline.ErrNode
	WaitGroup    sync.WaitGroup
}

func newRouter(ctx context.Context) *Router {
	r := &Router{
		RoutesByPath: map[string]*Route{},
		CloseChan:    make(chan struct{}),
		ErrorChan:    make(chan avpipeline.ErrNode, 100),
	}
	r.init(ctx)
	return r
}

func (r *Router) Close(
	ctx context.Context,
) error {
	r.Locker.Do(ctx, func() { // to make sure we don't have anybody adding more processes
		close(r.CloseChan)
		r.WaitGroup.Wait()
		close(r.ErrorChan)
	})
	return nil
}

func (r *Router) init(
	ctx context.Context,
) {
	observability.Go(ctx, func() {
		for err := range r.ErrorChan {
			route := err.Node.(*Node).CustomData
			if route == nil {
				logger.Errorf(ctx, "got an error on node %p: %w", err.Node, err.Err)
				continue
			}
			logger.Errorf(ctx, "got an error on node %p (path: '%s'): %w", err.Node, route.Path, err)
		}
	})
}

func (r *Router) onRouteOpen(
	ctx context.Context,
	route *Route,
) {
	r.WaitGroup.Add(1)
}

func (r *Router) onRouteClosed(
	ctx context.Context,
	route *Route,
) {
	r.WaitGroup.Done()
}

func (r *Router) GetRoute(
	ctx context.Context,
	path string,
) *Route {
	return xsync.DoA2R1(ctx, &r.Locker, r.getRoute, ctx, path)
}

func (r *Router) getRoute(
	ctx context.Context,
	path string,
) *Route {
	return r.RoutesByPath[path]
}

func (r *Router) GetOrCreateRoute(
	ctx context.Context,
	path string,
) *Route {
	return xsync.DoA2R1(ctx, &r.Locker, r.getOrCreateRoute, ctx, path)
}

func (r *Router) getOrCreateRoute(
	ctx context.Context,
	path string,
) *Route {
	route := r.RoutesByPath[path]
	if route != nil {
		return route
	}

	select {
	case <-r.CloseChan:
		return nil
	default:
	}
	route = newRoute(
		xcontext.DetachDone(ctx),
		path,
		r.ErrorChan,
		r.onRouteOpen,
		r.onRouteClosed,
	)
	r.RoutesByPath[path] = route
	return route
}

func (r *Router) RemoveRoute(
	ctx context.Context,
	path string,
) *Route {
	return xsync.DoA2R1(ctx, &r.Locker, r.removeRoute, ctx, path)
}

func (r *Router) removeRoute(
	ctx context.Context,
	path string,
) *Route {
	route := r.RoutesByPath[path]
	delete(r.RoutesByPath, path)
	return route
}
