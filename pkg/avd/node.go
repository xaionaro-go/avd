package avd

import (
	"context"
	"fmt"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/node"
	"github.com/xaionaro-go/avpipeline/processor"
	"github.com/xaionaro-go/avpipeline/router"
	"github.com/xaionaro-go/secret"
)

type ProcessorInput = processor.FromKernel[*kernel.Input]
type NodeInputProxied = node.NodeWithCustomData[*ConnectionProxiedPublisher, *ProcessorInput]
type NodeInputDirect = node.NodeWithCustomData[*ListeningPortDirectPublishers, *ProcessorInput]

func newProxiedInputNode(
	ctx context.Context,
	publisher *ConnectionProxiedPublisher,
	input *kernel.Input,
) *NodeInputProxied {
	n := node.NewWithCustomDataFromKernel[*ConnectionProxiedPublisher](
		ctx, input, processor.DefaultOptionsInput()...,
	)
	n.CustomData = publisher
	return n
}

type Sender = router.Sender
type ProcessorOutput = processor.FromKernel[*kernel.Output]
type NodeOutputProxied = node.NodeWithCustomData[*ConnectionProxiedConsumer, *ProcessorOutput]
type NodeOutputDirect = node.NodeWithCustomData[*ListeningPortDirectConsumers, *ProcessorOutput]

func newProxiedOutputNode(
	ctx context.Context,
	sender *ConnectionProxiedConsumer,
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

	outputKernel, err := kernel.NewOutputFromURL(ctx, dstURL, streamKey, cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to open the output: %w", err)
	}

	n := node.NewWithCustomDataFromKernel[*ConnectionProxiedConsumer](
		ctx, outputKernel, processor.DefaultOptionsOutput()...,
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
