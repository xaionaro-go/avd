package avd

import (
	"context"
	"errors"
	"fmt"

	"slices"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/xsync"
)

type ForwardingLocal struct {
	Source      *Route
	Destination *Route
	*NodeRouting
}

var _ Publisher = (*ForwardingLocal)(nil)

func (s *Server) AddForwardingLocal(
	ctx context.Context,
	srcPath string,
	getSrcRouteMode GetRouteMode,
	dstPath string,
	getDstRouteMode GetRouteMode,
) (_ret *ForwardingLocal, _err error) {
	logger.Debugf(ctx, "AddForwardingLocal(ctx, '%s', '%s', '%s', '%s')", srcPath, getSrcRouteMode, dstPath, getDstRouteMode)
	defer func() {
		logger.Debugf(ctx, "/AddForwardingLocal(ctx, '%s', '%s', '%s', '%s'): %v %v", srcPath, getSrcRouteMode, dstPath, getDstRouteMode, _ret, _err)
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

	fwd := &ForwardingLocal{
		Source:      src,
		Destination: dst,
		NodeRouting: dst.Node,
	}
	if err := fwd.init(ctx); err != nil {
		return nil, fmt.Errorf("unable to initialize: %w", err)
	}

	return fwd, nil
}

func (fwd *ForwardingLocal) init(ctx context.Context) error {
	if err := fwd.Destination.addPublisherIfNoPublishers(ctx, fwd); err != nil {
		return fmt.Errorf("unable to add the forwarder as a publisher to '%s': %w", fwd.Destination.Path, err)
	}

	if err := fwd.addPacketsPushing(ctx); err != nil {
		return err
	}

	return nil
}

func (fwd *ForwardingLocal) Close(
	ctx context.Context,
) error {
	var errs []error
	if err := fwd.removePacketsPushing(ctx); err != nil {
		errs = append(errs, fmt.Errorf("removePacketsPushing: %w", err))
	}
	return errors.Join(errs...)
}

func (fwd *ForwardingLocal) addPacketsPushing(
	ctx context.Context,
) error {
	return xsync.DoR1(ctx, &fwd.Source.Locker, func() error {
		pushTos := fwd.Source.Node.GetPushPacketsTos()
		for _, pushTo := range pushTos {
			if pushTo.Node == fwd {
				return fmt.Errorf("packets pushing is already added")
			}
		}
		fwd.Source.Node.AddPushPacketsTo(fwd) // it will push to fwd.NodeRouting (which is the same as fwd.Destination.Node)
		return nil
	})
}

func (fwd *ForwardingLocal) removePacketsPushing(
	ctx context.Context,
) error {
	return xsync.DoR1(ctx, &fwd.Source.Locker, func() error {
		pushTos := fwd.Source.Node.GetPushPacketsTos()
		for idx, pushTo := range pushTos {
			if pushTo.Node == fwd {
				pushTos = slices.Delete(pushTos, idx, idx+1)
				fwd.Source.Node.SetPushPacketsTos(pushTos)
				return nil
			}
		}
		return fmt.Errorf("have not found myself as a consumer of '%s'", fwd.Source.Path)
	})
}

func (fwd *ForwardingLocal) String() string {
	return fmt.Sprintf("fwd('%s'->'%s')", fwd.Source.Path, fwd.Destination.Path)
}

func (fwd *ForwardingLocal) GetNode(
	ctx context.Context,
) *NodeInput {
	if len(fwd.Source.Publishers) == 0 {
		return nil
	}
	if len(fwd.Source.Publishers) > 1 {
		logger.Errorf(ctx, "source '%s' has more than one publisher; which is not well supported by a local forwarder, yet", fwd.Source.Path)
	}
	publisher := fwd.Source.Publishers[0]
	return publisher.GetNode(ctx)
}

func (fwd *ForwardingLocal) GetRoute(
	ctx context.Context,
) *Route {
	return fwd.Destination
}
