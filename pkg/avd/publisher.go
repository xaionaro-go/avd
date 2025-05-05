package avd

import (
	"context"
	"fmt"
	"strings"

	"github.com/xaionaro-go/avpipeline"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/processor"
)

type Publisher interface {
	fmt.Stringer
	GetNode(ctx context.Context) *NodeInput
	GetRoute(ctx context.Context) *Route
}

type Publishers []Publisher

func (s Publishers) String() string {
	switch len(s) {
	case 0:
		return "NONE"
	case 1:
		return s[0].String()
	}

	var result []string
	for _, publisher := range s {
		result = append(result, publisher.String())
	}

	return "[" + strings.Join(result, ",") + "]"
}

type NodeInput = avpipeline.NodeWithCustomData[Publisher, *processor.FromKernel[*kernel.Input]]

func newInputNode(
	ctx context.Context,
	publisher Publisher,
	input *kernel.Input,
) *NodeInput {
	node := avpipeline.NewNodeWithCustomDataFromKernel[Publisher](
		ctx, input, processor.DefaultOptionsInput()...,
	)
	node.CustomData = publisher
	return node
}
