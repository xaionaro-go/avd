package avd

import (
	"context"
	"fmt"
	"sync"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avpipeline"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/processor"
	"github.com/xaionaro-go/observability"
	"github.com/xaionaro-go/recoder"
	"github.com/xaionaro-go/secret"
)

type ForwardingToRemote struct {
	Source      *Route
	Destination *NodeOutput
	ErrChan     chan avpipeline.ErrNode
	CancelFunc  context.CancelFunc
	CloseOnce   sync.Once
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

	if encodersConfig != nil {
		return nil, fmt.Errorf("recoding/transcoding is not implemented, yet")
	}

	source, err := s.Router.GetRoute(ctx, sourcePath, getRouteMode)
	if err != nil {
		return nil, fmt.Errorf("unable to get a route by path '%s' with mode '%s': %w", sourcePath, getRouteMode, err)
	}
	if source == nil {
		return nil, fmt.Errorf("there is no active route by path '%s'", sourcePath)
	}

	output, err := kernel.NewOutputFromURL(ctx, dstURL, streamKey, kernel.OutputConfig{})
	if err != nil {
		return nil, fmt.Errorf("unable to open the output: %w", err)
	}

	fwd := &ForwardingToRemote{
		Source:     source,
		ErrChan:    make(chan avpipeline.ErrNode, 100),
		CancelFunc: cancelFn,
	}
	fwd.Destination = newOutputNode(ctx, fwd, output)
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
	return fwd.Destination
}

func (fwd *ForwardingToRemote) init(
	ctx context.Context,
) (_err error) {
	logger.Debugf(ctx, "init")
	defer func() { logger.Debugf(ctx, "/init: %v", _err) }()
	observability.Go(ctx, func() {
		defer close(fwd.ErrChan)
		fwd.Destination.Serve(ctx, avpipeline.ServeConfig{}, fwd.ErrChan)
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
	fwd.CloseOnce.Do(func() {
		logger.Debugf(ctx, "Close")
		defer func() { logger.Debugf(ctx, "/Close: %v", _err) }()
		fwd.CancelFunc()
	})
	return nil
}

type NodeOutput = avpipeline.NodeWithCustomData[Sender, *processor.FromKernel[*kernel.Output]]

type Sender any

func newOutputNode(
	ctx context.Context,
	sender Sender,
	output *kernel.Output,
) *NodeOutput {
	node := avpipeline.NewNodeWithCustomDataFromKernel[Sender](
		ctx, output, processor.DefaultOptionsOutput()...,
	)
	node.CustomData = sender
	return node
}
