package avd

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/facebookincubator/go-belt"
	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avpipeline"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/observability"
	"github.com/xaionaro-go/recoder"
	"github.com/xaionaro-go/secret"
	"github.com/xaionaro-go/xsync"
)

const (
	forwardingToRemoteWaitForInput = true
)

type ForwardingToRemote struct {
	Source *Route
	*NodeOutput
	ErrChan    chan avpipeline.ErrNode
	CancelFunc context.CancelFunc
	CloseOnce  sync.Once
}

func (s *Server) AddForwardingToRemote(
	ctx context.Context,
	sourcePath string,
	dstURL string,
	streamKey secret.String,
	getRouteMode GetRouteMode,
	encodersConfig *recoder.EncodersConfig,
) (_ret *ForwardingToRemote, _err error) {
	logger.Debugf(ctx, "AddForwardingToRemote(ctx, '%s', '%s', '%s')", sourcePath, dstURL, getRouteMode)
	defer func() {
		logger.Debugf(ctx, "/AddForwardingToRemote(ctx, '%s', '%s', '%s'): %v %v", sourcePath, dstURL, getRouteMode, _ret, _err)
	}()

	ctx, cancelFn := context.WithCancel(ctx)
	defer func() {
		if _err != nil {
			cancelFn()
		}
	}()
	ctx = belt.WithField(ctx, "source_path", sourcePath)
	ctx = belt.WithField(ctx, "dst_url", dstURL)

	if encodersConfig != nil {
		return nil, fmt.Errorf("recoding/transcoding is not implemented, yet")
	}

	logger.Tracef(ctx, "s.Router.GetRoute")
	source, err := s.Router.GetRoute(ctx, sourcePath, getRouteMode)
	logger.Tracef(ctx, "/s.Router.GetRoute: %v %v", source, err)
	if err != nil {
		return nil, fmt.Errorf("unable to get a route by path '%s' with mode '%s': %w", sourcePath, getRouteMode, err)
	}
	if source == nil {
		return nil, fmt.Errorf("there is no active route by path '%s'", sourcePath)
	}

	logger.Tracef(ctx, "building ForwardingToRemote")
	fwd := &ForwardingToRemote{
		Source:     source,
		ErrChan:    make(chan avpipeline.ErrNode, 100),
		CancelFunc: cancelFn,
	}
	fwd.NodeOutput = newOutputNode(ctx, fwd, func(ctx context.Context) error {
		_, err := fwd.Source.WaitForPublisher(ctx)
		return err
	}, dstURL, streamKey, kernel.OutputConfig{})
	logger.Tracef(ctx, "built ForwardingToRemote")
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

func (fwd *ForwardingToRemote) GetNode(
	context.Context,
) *NodeOutput {
	return fwd.NodeOutput
}

func (fwd *ForwardingToRemote) init(
	ctx context.Context,
) (_err error) {
	logger.Debugf(ctx, "init")
	defer func() { logger.Debugf(ctx, "/init: %v", _err) }()
	if err := fwd.addPacketsPushing(ctx); err != nil {
		return fmt.Errorf("unable to add myself into the source's 'PushPacketsTo': %w", err)
	}
	observability.Go(ctx, func() {
		defer close(fwd.ErrChan)
		fwd.NodeOutput.Serve(ctx, avpipeline.ServeConfig{}, fwd.ErrChan)
	})
	observability.Go(ctx, func() {
		for err := range fwd.ErrChan {
			logger.Errorf(ctx, "got an error: %v", err)
			_ = fwd.Close(ctx)
		}
	})
	return nil
}

func (fwd *ForwardingToRemote) Close(
	ctx context.Context,
) (_err error) {
	var errs []error
	fwd.CloseOnce.Do(func() {
		logger.Debugf(ctx, "Close")
		defer func() { logger.Debugf(ctx, "/Close: %v", _err) }()
		fwd.CancelFunc()
		if fwd.NodeOutput != nil {
			if err := fwd.removePacketsPushing(ctx); err != nil {
				errs = append(errs, fmt.Errorf("unable to remove myself from the source's 'PushPacketsTo': %w", err))
			}
			fwd.NodeOutput = nil
		}
	})
	return errors.Join(errs...)
}

func (fwd *ForwardingToRemote) addPacketsPushing(
	ctx context.Context,
) (_err error) {
	logger.Debugf(ctx, "addPacketsPushing")
	defer func() { logger.Debugf(ctx, "/addPacketsPushing: %v", _err) }()
	return xsync.DoR1(ctx, &fwd.Source.Locker, func() error {
		pushTos := fwd.Source.Node.GetPushPacketsTos()
		for _, pushTo := range pushTos {
			if pushTo.Node == fwd {
				return fmt.Errorf("packets pushing is already added")
			}
		}

		fwd.Source.Node.AddPushPacketsTo(fwd) // it will push to fwd.NodeOutput
		logger.Debugf(
			ctx,
			"fwd.Source.Node: %T; fwd.Source.Node.PushPacketsTos: %#+v",
			fwd.Source.Node, fwd.Source.Node.GetPushPacketsTos(),
		)
		return nil
	})
}

func (fwd *ForwardingToRemote) removePacketsPushing(
	ctx context.Context,
) (_err error) {
	logger.Debugf(ctx, "removePacketsPushing")
	defer func() { defer logger.Debugf(ctx, "/removePacketsPushing: %v", _err) }()
	return xsync.DoR1(ctx, &fwd.Source.Locker, func() error {
		pushTos := fwd.Source.Node.GetPushPacketsTos()
		for idx, pushTo := range pushTos {
			if pushTo.Node == fwd {
				pushTos = slices.Delete(pushTos, idx, idx+1)
				fwd.Source.Node.SetPushPacketsTos(pushTos)
				logger.Debugf(
					ctx,
					"fwd.Source.Node: %T; fwd.Source.Node.PushPacketsTos: %#+v",
					fwd.Source.Node, fwd.Source.Node.GetPushPacketsTos(),
				)
				return nil
			}
		}
		return fmt.Errorf("have not found myself as a consumer of '%s'", fwd.Source.Path)
	})
}
