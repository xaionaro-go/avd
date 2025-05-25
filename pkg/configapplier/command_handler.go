package configapplier

import (
	"context"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avd/pkg/config"
	"github.com/xaionaro-go/avpipeline/router"
	"github.com/xaionaro-go/xsync"
)

type routePublisherKey[T any] struct {
	RoutePath router.RoutePath
	Publisher router.Publisher[T]
}

type commandHandler[T any] struct {
	xsync.Mutex

	OnRoutePublisherAddedConfig   map[router.RoutePath]config.Command
	OnRoutePublisherRemovedConfig map[router.RoutePath]config.Command

	RunningCommandsOnRoutePublisherAdded   map[routePublisherKey[T]]*process
	RunningCommandsOnRoutePublisherRemoved map[routePublisherKey[T]]*process
}

func newCommandHandler[T any](
	_ context.Context,
	cfg config.Config,
) *commandHandler[T] {
	h := &commandHandler[T]{
		OnRoutePublisherAddedConfig:            map[router.RoutePath]config.Command{},
		OnRoutePublisherRemovedConfig:          map[router.RoutePath]config.Command{},
		RunningCommandsOnRoutePublisherAdded:   map[routePublisherKey[T]]*process{},
		RunningCommandsOnRoutePublisherRemoved: map[routePublisherKey[T]]*process{},
	}
	for path, endpoint := range cfg.Endpoints {
		if endpoint.OnPublisherAdded != nil && len(endpoint.OnPublisherAdded.Command) != 0 {
			h.OnRoutePublisherAddedConfig[path] = *endpoint.OnPublisherAdded
		}
		if endpoint.OnPublisherRemoved != nil && len(endpoint.OnPublisherRemoved.Command) != 0 {
			h.OnRoutePublisherRemovedConfig[path] = *endpoint.OnPublisherRemoved
		}
	}
	return h
}

func (h *commandHandler[T]) OnRoutePublisherAdded(
	ctx context.Context,
	route *router.Route[T],
	publisher router.Publisher[T],
) {
	logger.Tracef(ctx, "OnRoutePublisherAdded(ctx, '%s', publisher)", route)
	defer func() { logger.Tracef(ctx, "/OnRoutePublisherAdded(ctx, '%s', publisher)", route) }()
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
		process.Close(ctx)
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
		logger.Errorf(ctx, "unable to run command %#+v: %w", cmd, err)
	}
	h.RunningCommandsOnRoutePublisherAdded[key] = process
}

func (h *commandHandler[T]) OnRoutePublisherRemoved(
	ctx context.Context,
	route *router.Route[T],
	publisher router.Publisher[T],
) {
	logger.Tracef(ctx, "OnRoutePublisherRemoved(ctx, '%s', publisher)", route)
	defer func() { logger.Tracef(ctx, "/OnRoutePublisherRemoved(ctx, '%s', publisher)", route) }()
	xsync.DoA3(ctx, &h.Mutex, h.onRoutePublisherRemoved, ctx, route, publisher)
}

func (h *commandHandler[T]) onRoutePublisherRemoved(
	ctx context.Context,
	route *router.Route[T],
	publisher router.Publisher[T],
) {
	key := routePublisherKey[T]{
		RoutePath: route.Path,
		Publisher: publisher,
	}

	if process, ok := h.RunningCommandsOnRoutePublisherAdded[key]; ok {
		process.Close(ctx)
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
		logger.Errorf(ctx, "unable to run command %#+v: %w", cmd, err)
	}
	h.RunningCommandsOnRoutePublisherRemoved[key] = process
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
