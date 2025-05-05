package avd

import (
	"context"

	"github.com/xaionaro-go/avpipeline"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/processor"
	"github.com/xaionaro-go/avpipeline/types"
	"github.com/xaionaro-go/observability"
	"github.com/xaionaro-go/typing"
)

type Node = avpipeline.NodeWithCustomData[*Route, *processor.FromKernel[*kernel.MapStreamIndices]]

type Route struct {
	Path string
	*Node
}

func newRoute(
	ctx context.Context,
	path string,
	errCh chan<- avpipeline.ErrNode,
	onOpen func(context.Context, *Route),
	onClosed func(context.Context, *Route),
) *Route {
	r := &Route{
		Path: path,
	}
	r.Node = avpipeline.NewNodeWithCustomDataFromKernel[*Route](
		ctx,
		kernel.NewMapStreamIndices(ctx, r),
		processor.DefaultOptionsRecoder()...,
	)
	r.Node.CustomData = r
	if onOpen != nil {
		onOpen(ctx, r)
	}
	observability.Go(ctx, func() {
		if onClosed != nil {
			defer onClosed(ctx, r)
		}
		r.Node.Serve(ctx, avpipeline.ServeConfig{}, errCh)
	})
	return r
}

func (r *Route) StreamIndexAssign(
	ctx context.Context,
	input types.InputPacketOrFrameUnion,
) (typing.Optional[int], error) {
	return typing.Opt(input.GetStreamIndex()), nil
}
