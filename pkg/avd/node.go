package avd

import (
	"context"
	"fmt"
	"time"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/node"
	"github.com/xaionaro-go/avpipeline/processor"
	"github.com/xaionaro-go/observability/xlogger"
	"github.com/xaionaro-go/secret"
)

type NodeInput = node.NodeWithCustomData[Publisher, *processor.FromKernel[*kernel.Input]]

func newInputNode(
	ctx context.Context,
	publisher Publisher,
	input *kernel.Input,
) *NodeInput {
	n := node.NewWithCustomDataFromKernel[Publisher](
		ctx, input, processor.DefaultOptionsInput()...,
	)
	n.CustomData = publisher
	return n
}

type Sender any

type NodeOutput = node.NodeWithCustomData[Sender, *processor.FromKernel[*kernel.Output]]

func newOutputNode(
	ctx context.Context,
	sender Sender,
	waitForInputFunc func(context.Context) error,
	dstURL string,
	streamKey secret.String,
	cfg kernel.OutputConfig,
) (*NodeOutput, error) {
	logger.Tracef(ctx, "newOutputNode")
	defer func() { logger.Tracef(ctx, "/newOutputNode") }()

	if waitForInputFunc != nil {
		err := waitForInputFunc(ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to wait for input: %w", err)
		}
	}

	outputKernel, err := kernel.NewOutputFromURL(ctx, dstURL, streamKey, cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to open the output: %w", err)
	}

	n := node.NewWithCustomDataFromKernel[Sender](
		ctx, outputKernel, processor.DefaultOptionsOutput()...,
	)
	n.CustomData = sender
	return n, nil
}

type NodeRetryOutput = node.NodeWithCustomData[Sender, *processor.FromKernel[*kernel.Retry[*kernel.Output]]]

func newRetryOutputNode(
	ctx context.Context,
	sender Sender,
	waitForInputFunc func(context.Context) error,
	dstURL string,
	streamKey secret.String,
	cfg kernel.OutputConfig,
) *NodeRetryOutput {
	logger.Tracef(ctx, "newOutputNode")
	defer func() { logger.Tracef(ctx, "/newOutputNode") }()

	outputKernel := kernel.NewRetry(xlogger.CtxWithMaxLoggingLevel(ctx, logger.LevelWarning),
		func(ctx context.Context) (*kernel.Output, error) {
			if waitForInputFunc != nil {
				err := waitForInputFunc(ctx)
				if err != nil {
					return nil, fmt.Errorf("unable to wait for input: %w", err)
				}
			}
			return kernel.NewOutputFromURL(ctx, dstURL, streamKey, cfg)
		},
		func(ctx context.Context, k *kernel.Output) error {
			return nil
		},
		func(ctx context.Context, k *kernel.Output, err error) error {
			logger.Debugf(ctx, "connection ended: %v", err)
			time.Sleep(time.Second)
			return kernel.ErrRetry{Err: err}
		},
	)
	n := node.NewWithCustomDataFromKernel[Sender](
		ctx, outputKernel, processor.DefaultOptionsOutput()...,
	)
	n.CustomData = sender
	return n
}

type NodeRouting = node.NodeWithCustomData[*Route, *processor.FromKernel[*kernel.MapStreamIndices]]

type AbstractNodeOutput interface {
	*NodeOutput | *NodeRetryOutput
}

type AbstractNodeIO interface {
	*NodeInput | AbstractNodeOutput
	node.Abstract
	DotString(bool) string
}
