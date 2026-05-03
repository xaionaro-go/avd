// command_handler.go handles commands triggered by route events.

package configapplier

import (
	"context"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avd/pkg/config"
	"github.com/xaionaro-go/avpipeline/router"
	"github.com/xaionaro-go/xcontext"
	"github.com/xaionaro-go/xsync"
)

type routePublisherKey[T any] struct {
	RoutePath router.RoutePath
	Publisher router.Publisher[T]
}

type routeConsumerKey[T any] struct {
	RoutePath router.RoutePath
	Consumer  router.Consumer[T]
}

type commandHandler[T any] struct {
	xsync.Mutex

	// Resolver, if non-nil, is used to drive refcount-based
	// eviction of regex-pattern-materialized routes. Set by
	// ApplyConfig at startup.
	Resolver *EndpointResolver

	OnRoutePublisherAddedConfig   map[router.RoutePath]config.Command
	OnRoutePublisherRemovedConfig map[router.RoutePath]config.Command
	OnRouteConsumerAddedConfig    map[router.RoutePath]config.Command
	OnRouteConsumerRemovedConfig  map[router.RoutePath]config.Command

	RunningCommandsOnRoutePublisherAdded   map[routePublisherKey[T]]*process
	RunningCommandsOnRoutePublisherRemoved map[routePublisherKey[T]]*process
	RunningCommandsOnRouteConsumerAdded    map[routeConsumerKey[T]]*process
	RunningCommandsOnRouteConsumerRemoved  map[routeConsumerKey[T]]*process
}

func newCommandHandler[T any](
	ctx context.Context,
	cfg config.Config,
) *commandHandler[T] {
	h := &commandHandler[T]{
		OnRoutePublisherAddedConfig:            map[router.RoutePath]config.Command{},
		OnRoutePublisherRemovedConfig:          map[router.RoutePath]config.Command{},
		OnRouteConsumerAddedConfig:             map[router.RoutePath]config.Command{},
		OnRouteConsumerRemovedConfig:           map[router.RoutePath]config.Command{},
		RunningCommandsOnRoutePublisherAdded:   map[routePublisherKey[T]]*process{},
		RunningCommandsOnRoutePublisherRemoved: map[routePublisherKey[T]]*process{},
		RunningCommandsOnRouteConsumerAdded:    map[routeConsumerKey[T]]*process{},
		RunningCommandsOnRouteConsumerRemoved:  map[routeConsumerKey[T]]*process{},
	}
	for path, endpoint := range cfg.Endpoints {
		h.RegisterEndpointCommands(ctx, path, endpoint)
	}
	return h
}

// RegisterEndpointCommands installs path-keyed entries in the
// OnRouteXXXConfig maps for whatever command hooks the endpoint
// declares. Used by newCommandHandler for literal endpoints at startup
// and by serverWirer for pattern-rendered endpoints at lazy-wire time
// (so a pattern body declaring on_publisher_added / on_consumer_added
// fires the same hooks as a literal endpoint with the same body).
// Concurrent-safe: takes the handler's mutex while writing the maps.
func (h *commandHandler[T]) RegisterEndpointCommands(
	ctx context.Context,
	path router.RoutePath,
	endpoint config.EndpointConfig,
) {
	h.Mutex.Do(ctx, func() {
		if endpoint.OnPublisherAdded != nil && len(endpoint.OnPublisherAdded.Command) != 0 {
			h.OnRoutePublisherAddedConfig[path] = *endpoint.OnPublisherAdded
		}
		if endpoint.OnPublisherRemoved != nil && len(endpoint.OnPublisherRemoved.Command) != 0 {
			h.OnRoutePublisherRemovedConfig[path] = *endpoint.OnPublisherRemoved
		}
		if endpoint.OnConsumerAdded != nil && len(endpoint.OnConsumerAdded.Command) != 0 {
			h.OnRouteConsumerAddedConfig[path] = *endpoint.OnConsumerAdded
		}
		if endpoint.OnConsumerRemoved != nil && len(endpoint.OnConsumerRemoved.Command) != 0 {
			h.OnRouteConsumerRemovedConfig[path] = *endpoint.OnConsumerRemoved
		}
	})
}

// UnregisterEndpointCommands deletes any command-hook entries for path.
// Called by serverWirer when a pattern-rendered route is evicted so the
// dropped path's hooks don't outlive the route.
func (h *commandHandler[T]) UnregisterEndpointCommands(
	ctx context.Context,
	path router.RoutePath,
) {
	h.Mutex.Do(ctx, func() {
		delete(h.OnRoutePublisherAddedConfig, path)
		delete(h.OnRoutePublisherRemovedConfig, path)
		delete(h.OnRouteConsumerAddedConfig, path)
		delete(h.OnRouteConsumerRemovedConfig, path)
	})
}

func (h *commandHandler[T]) OnRoutePublisherAdded(
	ctx context.Context,
	route *router.Route[T],
	publisher router.Publisher[T],
) {
	logger.Tracef(ctx, "OnRoutePublisherAdded(ctx, '%s', publisher)", route)
	defer func() { logger.Tracef(ctx, "/OnRoutePublisherAdded(ctx, '%s', publisher)", route) }()
	if h.Resolver != nil {
		if err := h.Resolver.Acquire(ctx, route.Path); err != nil {
			logger.Errorf(ctx, "endpoint resolver Acquire('%s') failed on publisher arrival: %v", route.Path, err)
		}
	}
	xsync.DoA3(ctx, &h.Mutex, h.onRoutePublisherAdded, ctx, route, publisher)
}

func (h *commandHandler[T]) onRoutePublisherAdded(
	ctx context.Context,
	route *router.Route[T],
	publisher router.Publisher[T],
) {
	key := routePublisherKey[T]{
		RoutePath: route.Path,
		Publisher: publisher,
	}

	if process, ok := h.RunningCommandsOnRoutePublisherRemoved[key]; ok {
		if process != nil {
			process.Close(ctx)
		}
		delete(h.RunningCommandsOnRoutePublisherRemoved, key)
	}

	cmd, ok := h.OnRoutePublisherAddedConfig[route.Path]
	if !ok {
		logger.Tracef(ctx, "no OnPublisherAdded command specified for route '%s'", route.Path)
		return
	}
	logger.Tracef(ctx, "OnPublisherAdded command '%v' is specified for route '%s'", cmd, route.Path)

	process, err := h.runCommand(ctx, cmd)
	if err != nil {
		logger.Errorf(ctx, "unable to run command %#+v: %v", cmd, err)
	}
	if process != nil {
		h.RunningCommandsOnRoutePublisherAdded[key] = process
	}
}

func (h *commandHandler[T]) OnRoutePublisherRemoved(
	ctx context.Context,
	route *router.Route[T],
	publisher router.Publisher[T],
) {
	logger.Tracef(ctx, "OnRoutePublisherRemoved(ctx, '%s', publisher)", route)
	defer func() { logger.Tracef(ctx, "/OnRoutePublisherRemoved(ctx, '%s', publisher)", route) }()
	xsync.DoA3(ctx, &h.Mutex, h.onRoutePublisherRemoved, ctx, route, publisher)
	if h.Resolver != nil {
		h.Resolver.Release(ctx, route.Path)
	}
}

func (h *commandHandler[T]) onRoutePublisherRemoved(
	ctx context.Context,
	route *router.Route[T],
	publisher router.Publisher[T],
) {
	ctx = xcontext.DetachDone(ctx)

	key := routePublisherKey[T]{
		RoutePath: route.Path,
		Publisher: publisher,
	}

	if process, ok := h.RunningCommandsOnRoutePublisherAdded[key]; ok {
		if process != nil {
			process.Close(ctx)
		}
		delete(h.RunningCommandsOnRoutePublisherAdded, key)
	}

	cmd, ok := h.OnRoutePublisherRemovedConfig[route.Path]
	if !ok {
		logger.Tracef(ctx, "no OnPublisherRemoved command specified for route '%s'", route.Path)
		return
	}
	logger.Tracef(ctx, "OnPublisherRemoved command '%v' is specified for route '%s'", cmd, route.Path)

	process, err := h.runCommand(ctx, cmd)
	if err != nil {
		logger.Errorf(ctx, "unable to run command %#+v: %v", cmd, err)
	}
	if process != nil {
		h.RunningCommandsOnRoutePublisherRemoved[key] = process
	}
}

// OnRouteConsumerAdded mirrors OnRoutePublisherAdded for consumers:
// drives the resolver's refcount and runs the optional
// `on_consumer_added` command for this route path.
func (h *commandHandler[T]) OnRouteConsumerAdded(
	ctx context.Context,
	route *router.Route[T],
	consumer router.Consumer[T],
) {
	logger.Tracef(ctx, "OnRouteConsumerAdded(ctx, '%s', consumer)", route)
	defer func() { logger.Tracef(ctx, "/OnRouteConsumerAdded(ctx, '%s', consumer)", route) }()
	if h.Resolver != nil {
		if err := h.Resolver.Acquire(ctx, route.Path); err != nil {
			logger.Errorf(ctx, "endpoint resolver Acquire('%s') failed on consumer arrival: %v", route.Path, err)
		}
	}
	xsync.DoA3(ctx, &h.Mutex, h.onRouteConsumerAdded, ctx, route, consumer)
}

func (h *commandHandler[T]) onRouteConsumerAdded(
	ctx context.Context,
	route *router.Route[T],
	consumer router.Consumer[T],
) {
	key := routeConsumerKey[T]{
		RoutePath: route.Path,
		Consumer:  consumer,
	}

	if process, ok := h.RunningCommandsOnRouteConsumerRemoved[key]; ok {
		if process != nil {
			process.Close(ctx)
		}
		delete(h.RunningCommandsOnRouteConsumerRemoved, key)
	}

	cmd, ok := h.OnRouteConsumerAddedConfig[route.Path]
	if !ok {
		logger.Tracef(ctx, "no OnConsumerAdded command specified for route '%s'", route.Path)
		return
	}
	logger.Tracef(ctx, "OnConsumerAdded command '%v' is specified for route '%s'", cmd, route.Path)

	process, err := h.runCommand(ctx, cmd)
	if err != nil {
		logger.Errorf(ctx, "unable to run command %#+v: %v", cmd, err)
	}
	if process != nil {
		h.RunningCommandsOnRouteConsumerAdded[key] = process
	}
}

// OnRouteConsumerRemoved mirrors OnRoutePublisherRemoved for consumers.
func (h *commandHandler[T]) OnRouteConsumerRemoved(
	ctx context.Context,
	route *router.Route[T],
	consumer router.Consumer[T],
) {
	logger.Tracef(ctx, "OnRouteConsumerRemoved(ctx, '%s', consumer)", route)
	defer func() { logger.Tracef(ctx, "/OnRouteConsumerRemoved(ctx, '%s', consumer)", route) }()
	xsync.DoA3(ctx, &h.Mutex, h.onRouteConsumerRemoved, ctx, route, consumer)
	if h.Resolver != nil {
		h.Resolver.Release(ctx, route.Path)
	}
}

func (h *commandHandler[T]) onRouteConsumerRemoved(
	ctx context.Context,
	route *router.Route[T],
	consumer router.Consumer[T],
) {
	ctx = xcontext.DetachDone(ctx)

	key := routeConsumerKey[T]{
		RoutePath: route.Path,
		Consumer:  consumer,
	}

	if process, ok := h.RunningCommandsOnRouteConsumerAdded[key]; ok {
		if process != nil {
			process.Close(ctx)
		}
		delete(h.RunningCommandsOnRouteConsumerAdded, key)
	}

	cmd, ok := h.OnRouteConsumerRemovedConfig[route.Path]
	if !ok {
		logger.Tracef(ctx, "no OnConsumerRemoved command specified for route '%s'", route.Path)
		return
	}
	logger.Tracef(ctx, "OnConsumerRemoved command '%v' is specified for route '%s'", cmd, route.Path)

	process, err := h.runCommand(ctx, cmd)
	if err != nil {
		logger.Errorf(ctx, "unable to run command %#+v: %v", cmd, err)
	}
	if process != nil {
		h.RunningCommandsOnRouteConsumerRemoved[key] = process
	}
}

func (h *commandHandler[T]) runCommand(
	ctx context.Context,
	cmd config.Command,
) (_ret *process, _err error) {
	logger.Debugf(ctx, "running command %#+v", cmd)
	defer func() { logger.Debugf(ctx, "/running command %#+v: %v", cmd, _err) }()
	p := newProcess(ctx, cmd)
	p.Start(ctx)
	return p, nil
}
