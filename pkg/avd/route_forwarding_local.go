package avd

import (
	"context"
	"errors"
	"fmt"

	"github.com/facebookincubator/go-belt/tool/logger"
	transcodertypes "github.com/xaionaro-go/avpipeline/chain/transcoderwithpassthrough/types"
	"github.com/xaionaro-go/avpipeline/node"
)

type RouteForwardingLocal struct {
	StreamForwarder
	Input  *Route
	Output *Route
}

var _ Publisher = (*RouteForwardingLocal)(nil)

func (s *Server) AddRouteForwardingLocal(
	ctx context.Context,
	srcPath RoutePath,
	getSrcRouteMode GetRouteMode,
	dstPath RoutePath,
	getDstRouteMode GetRouteMode,
	recoderConfig *transcodertypes.RecoderConfig,
) (_ret *RouteForwardingLocal, _err error) {
	logger.Debugf(ctx, "AddRouteForwardingLocal(ctx, '%s', '%s', '%s', '%s')", srcPath, getSrcRouteMode, dstPath, getDstRouteMode)
	defer func() {
		logger.Debugf(ctx, "/AddRouteForwardingLocal(ctx, '%s', '%s', '%s', '%s'): %v %v", srcPath, getSrcRouteMode, dstPath, getDstRouteMode, _ret, _err)
	}()

	src, err := s.Router.GetRoute(ctx, srcPath, getSrcRouteMode)
	if err != nil {
		return nil, fmt.Errorf("unable to get the source route by path '%s' with mode '%s': %w", srcPath, getSrcRouteMode, err)
	}
	if src == nil {
		return nil, fmt.Errorf("there is no active route by path '%s' (source)", srcPath)
	}

	dst, err := s.Router.GetRoute(ctx, dstPath, getDstRouteMode)
	if err != nil {
		return nil, fmt.Errorf("unable to get the source route by path '%s' with mode '%s': %w", srcPath, getSrcRouteMode, err)
	}
	if dst == nil {
		return nil, fmt.Errorf("there is no active route by path '%s' (source)", srcPath)
	}

	f, err := newStreamForwarder(ctx, src.Node, dst.Node, recoderConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize a forwarder from '%s' to '%s' (%#+v): %w", src.Path, dst.Path, recoderConfig, err)
	}
	fwd := &RouteForwardingLocal{
		StreamForwarder: f,
		Input:           src,
		Output:          dst,
	}
	defer func() {
		if _err != nil {
			fwd.Close(ctx)
		}
	}()
	if err := fwd.init(ctx); err != nil {
		return nil, fmt.Errorf("unable to initialize: %w", err)
	}

	return fwd, nil
}

func (fwd *RouteForwardingLocal) init(ctx context.Context) (_err error) {
	logger.Debugf(ctx, "init")
	defer func() { logger.Debugf(ctx, "/init: %v", _err) }()

	if err := fwd.Output.addPublisherIfNoPublishers(ctx, fwd); err != nil {
		return fmt.Errorf("unable to add the forwarder as a publisher to '%s': %w", fwd.Output.Path, err)
	}

	if err := fwd.StreamForwarder.Start(ctx); err != nil {
		return fmt.Errorf("unable to start stream forwarding: %w", err)
	}

	return nil
}

func (fwd *RouteForwardingLocal) Close(
	ctx context.Context,
) (_err error) {
	logger.Debugf(ctx, "Close")
	defer func() { logger.Debugf(ctx, "/Close: %v", _err) }()
	var errs []error
	if err := fwd.StreamForwarder.Stop(ctx); err != nil {
		errs = append(errs, fmt.Errorf("fwd.Forwarder.Stop: %w", err))
	}
	if _, err := fwd.Output.removePublisher(ctx, fwd); err != nil {
		errs = append(errs, fmt.Errorf("removePublisher: %w", err))
	}
	return errors.Join(errs...)
}

func (fwd *RouteForwardingLocal) String() string {
	return fmt.Sprintf("fwd('%s'->'%s')", fwd.Input.Path, fwd.Output.Path)
}

func (fwd *RouteForwardingLocal) GetInputNode(
	ctx context.Context,
) node.Abstract {
	if len(fwd.Input.Publishers) == 0 {
		return nil
	}
	if len(fwd.Input.Publishers) > 1 {
		logger.Errorf(ctx, "source '%s' has more than one publisher; which is not well supported by a local forwarder, yet", fwd.Input.Path)
	}
	publisher := fwd.Input.Publishers[0]
	return publisher.GetInputNode(ctx)
}

func (fwd *RouteForwardingLocal) GetOutputRoute(
	ctx context.Context,
) *Route {
	return fwd.Output
}
