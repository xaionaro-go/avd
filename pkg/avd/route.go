package avd

import (
	"context"
	"fmt"

	"slices"

	"github.com/xaionaro-go/avpipeline"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/processor"
	"github.com/xaionaro-go/avpipeline/types"
	"github.com/xaionaro-go/observability"
	"github.com/xaionaro-go/typing"
	"github.com/xaionaro-go/xsync"
)

type NodeRouting = avpipeline.NodeWithCustomData[*Route, *processor.FromKernel[*kernel.MapStreamIndices]]

type Route struct {
	Locker xsync.Mutex

	// read only:
	Path string

	// access only when Locker is locked:
	Node                 *NodeRouting
	Publishers           Publishers
	PublishersChangeChan chan struct{}
}

func newRoute(
	ctx context.Context,
	path string,
	errCh chan<- avpipeline.ErrNode,
	onOpen func(context.Context, *Route),
	onClosed func(context.Context, *Route),
) *Route {
	r := &Route{
		Path:                 path,
		PublishersChangeChan: make(chan struct{}),
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

func (r *Route) getPublishersChangeChan(
	ctx context.Context,
) <-chan struct{} {
	return xsync.DoR1(ctx, &r.Locker, func() <-chan struct{} {
		return r.PublishersChangeChan
	})
}

func (r *Route) StreamIndexAssign(
	ctx context.Context,
	input types.InputPacketOrFrameUnion,
) (typing.Optional[int], error) {
	return typing.Opt(input.GetStreamIndex()), nil
}

func (r *Route) GetPublishers(
	ctx context.Context,
) Publishers {
	return xsync.DoR1(ctx, &r.Locker, func() Publishers {
		return r.Publishers
	})
}

func (r *Route) addPublisherIfNoPublishers(
	ctx context.Context,
	publisher Publisher,
) error {
	return xsync.DoR1(ctx, &r.Locker, func() error {
		if len(r.Publishers) > 0 {
			return fmt.Errorf("there are already %d publishers on this route", len(r.Publishers))
		}
		_ = r.addPublisherNoLock(ctx, publisher)
		return nil
	})
}

func (r *Route) addPublisherNoLock(
	_ context.Context,
	publisher Publisher,
) Publishers {
	if slices.Contains(r.Publishers, publisher) {
		// already added
		return r.Publishers
	}
	r.Publishers = append(r.Publishers, publisher)
	var ch chan<- struct{}
	ch, r.PublishersChangeChan = r.PublishersChangeChan, make(chan struct{})
	close(ch)
	return r.Publishers
}

func (r *Route) removePublisher(
	ctx context.Context,
	publisher Publisher,
) (Publishers, error) {
	return xsync.DoA2R2(ctx, &r.Locker, r.removePublisherNoLock, ctx, publisher)
}

func (r *Route) removePublisherNoLock(
	ctx context.Context,
	publisher Publisher,
) (Publishers, error) {
	for idx, candidate := range r.Publishers {
		if publisher == candidate {
			r.Publishers = slices.Delete(r.Publishers, idx, idx+1)
			var ch chan<- struct{}
			ch, r.PublishersChangeChan = r.PublishersChangeChan, make(chan struct{})
			close(ch)
			return r.Publishers, nil
		}
	}

	return nil, fmt.Errorf("the publisher is not found in the list of route's publishers")
}

func (r *Route) WaitForPublisher(
	ctx context.Context,
) (Publishers, error) {
	for {
		ch := r.getPublishersChangeChan(ctx)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ch:
			publishers := r.GetPublishers(ctx)
			if len(publishers) > 0 {
				return publishers, nil
			}
		}
	}
}
