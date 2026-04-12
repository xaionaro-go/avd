// node.go defines node types and factory functions for AVD.

package avd

import (
	"context"
	"fmt"
	"time"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avpipeline/kernel"
	kernelcondition "github.com/xaionaro-go/avpipeline/kernel/condition"
	"github.com/xaionaro-go/avpipeline/node"
	"github.com/xaionaro-go/avpipeline/processor"
	"github.com/xaionaro-go/avpipeline/router"
	"github.com/xaionaro-go/secret"
)

type (
	ProcessorInput   = processor.FromKernel[*kernel.Input]
	NodeInputProxied = node.NodeWithCustomData[*ConnectionProxiedHandlerPublisher, *ProcessorInput]
	NodeInputDirect  = node.NodeWithCustomData[*ListeningPortDirectPublishers, *ProcessorInput]
)

func newProxiedInputNode(
	ctx context.Context,
	publisher *ConnectionProxiedHandlerPublisher,
	input *kernel.Input,
) *NodeInputProxied {
	n := node.NewWithCustomDataFromKernel[*ConnectionProxiedHandlerPublisher](
		ctx, input, processor.DefaultOptionsInput()...,
	)
	n.CustomData = publisher
	return n
}

type (
	Sender            = router.Sender
	ProcessorOutput   = processor.FromKernel[*kernel.ChainOfTwo[*kernel.ReorderMonotonicDTS, *kernel.Output]]
	NodeOutputProxied = node.NodeWithCustomData[*ConnectionProxiedHandlerConsumer, *ProcessorOutput]
	NodeOutputDirect  = node.NodeWithCustomData[*ListeningPortDirectConsumers, *ProcessorOutput]
)

func newProxiedOutputNode(
	ctx context.Context,
	sender *ConnectionProxiedHandlerConsumer,
	waitForInputFunc func(context.Context) error,
	dstURL string,
	streamKey secret.String,
	cfg kernel.OutputConfig,
) (*NodeOutputProxied, error) {
	logger.Tracef(ctx, "newProxiedOutputNode")
	defer func() { logger.Tracef(ctx, "/newProxiedOutputNode") }()

	if waitForInputFunc != nil {
		err := waitForInputFunc(ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to wait for input: %w", err)
		}
	}

	minStreams := uint(0)
	if w := cfg.WaitForOutputStreams; w != nil {
		minStreams = max(w.MinStreams, w.MinStreamsAudio+w.MinStreamsVideo+w.MinStreamsSubtitle+w.MinStreamsData)
	}
	outputKernel, err := kernel.NewOutputFromURL(ctx, dstURL, streamKey, cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to open the output: %w", err)
	}

	n := node.NewWithCustomDataFromKernel[*ConnectionProxiedHandlerConsumer](
		ctx,
		kernel.NewChainOfTwo(
			kernel.NewReorderMonotonicDTS(
				ctx,
				kernelcondition.Function[*kernel.ReorderMonotonicDTS](func(
					ctx context.Context,
					k *kernel.ReorderMonotonicDTS,
				) bool {
					logger.Tracef(ctx, "%d out of %d streams were seen", len(k.StreamsDTSs), minStreams)
					return len(k.StreamsDTSs) >= int(minStreams)
				}), 10000, time.Second, true),
			outputKernel,
		),
		processor.DefaultOptionsOutput()...,
	)
	n.CustomData = sender
	return n, nil
}

type NodeRouting = router.NodeRouting[RouteCustomData]

type AbstractNodeInput interface {
	*NodeInputDirect | *NodeInputProxied
}

type AbstractNodeOutput interface {
	*NodeOutputDirect | *NodeOutputProxied
}

type AbstractNodeIO interface {
	AbstractNodeInput | AbstractNodeOutput
	node.Abstract
	DotString(bool) string
}
