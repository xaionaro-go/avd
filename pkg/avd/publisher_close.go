// publisher_close.go provides logic for closing publishers and their associated routes.

package avd

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avd/pkg/avd/types"
	"github.com/xaionaro-go/avpipeline"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/node"
	"github.com/xaionaro-go/avpipeline/processor"
)

func PublisherClose(
	ctx context.Context,
	publisher Publisher,
	onEndAction types.OnEndAction,
) (_err error) {
	var errs []error
	route := publisher.GetOutputRoute(ctx)

	logger.Debugf(ctx, "PublisherClose(ctx, '%s', '%s')", route, onEndAction)
	defer func() { logger.Debugf(ctx, "/PublisherClose(ctx, '%s', '%s'): %v", route, onEndAction, _err) }()

	switch onEndAction {
	default:
		logger.Errorf(ctx, "unknown 'OnEndAction': '%s'; defaulting to '%s'", onEndAction, types.OnEndActionCloseConsumers)
		fallthrough
	case types.OnEndActionCloseConsumers:
		if route == nil {
			break
		}
		err := avpipeline.Traverse(
			ctx,
			func(ctx context.Context, parent node.Abstract, item reflect.Type, node node.Abstract) error {
				procI := node.GetProcessor()
				proc, ok := procI.(*processor.FromKernel[*kernel.Output])
				if !ok {
					logger.Debugf(ctx, "processor type: %T", procI)
					return nil
				}
				logger.Debugf(ctx, "closing %s", node)
				err := proc.Close(ctx)
				if err != nil {
					errs = append(errs, fmt.Errorf("unable to close %s: %w", node, err))
				}
				logger.Debugf(ctx, "/closing %s: %v", node, err)
				return nil
			},
			route.Node,
		)
		if err != nil {
			errs = append(errs, fmt.Errorf("unable to traverse the output pipeline of route '%s': %w", route, err))
		}
		if err := route.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("unable to close the route '%s': %w", route, err))
		}
	case types.OnEndActionWaitForNewPublisher:
	}
	return errors.Join(errs...)
}
