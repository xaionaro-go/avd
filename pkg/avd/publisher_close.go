package avd

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avd/pkg/avd/types"
	"github.com/xaionaro-go/avpipeline"
	"github.com/xaionaro-go/avpipeline/node"
)

func PublisherClose(
	ctx context.Context,
	publisher Publisher,
	onEndAction types.OnEndAction,
) error {
	route := publisher.GetOutputRoute(ctx)
	var errs []error
	if _, err := route.RemovePublisher(ctx, publisher); err != nil {
		errs = append(errs, fmt.Errorf("unable to remove myself as a publisher at '%s': %w", route.Path, err))
	}

	logger.Debugf(ctx, "'OnEndAction' is '%s'", onEndAction)
	switch onEndAction {
	default:
		logger.Errorf(ctx, "unknown 'OnEndAction': '%s'; defaulting to '%s'", onEndAction, types.OnEndActionCloseConsumers)
		fallthrough
	case types.OnEndActionCloseConsumers:
		err := avpipeline.Traverse(
			ctx,
			func(ctx context.Context, parent node.Abstract, item reflect.Type, node node.Abstract) error {
				proc := node.GetProcessor()
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
