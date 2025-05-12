package configapplier

import (
	"context"
	"fmt"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avd/pkg/avd"
	"github.com/xaionaro-go/avd/pkg/config"
	"github.com/xaionaro-go/avpipeline/router"
	"github.com/xaionaro-go/observability"
	"github.com/xaionaro-go/secret"
)

func ApplyConfig(
	ctx context.Context,
	cfg config.Config,
	srv *avd.Server,
) error {
	for _, port := range cfg.Ports {
		protocol, err := port.ProtocolHandler.Protocol()
		if err != nil {
			return fmt.Errorf("unable to identify which protocol to use on '%s': %w", port.Address, err)
		}

		_, err = srv.Listen(ctx, port.Address, protocol, port.Mode, port.ListenOptions()...)
		if err != nil {
			return fmt.Errorf("unable to listen '%s' with the %s-%s handler: %w", port.Address, protocol, port.Mode, err)
		}
	}

	for path := range cfg.Endpoints {
		_, err := srv.Router.GetRoute(ctx, path, router.GetRouteModeCreateIfNotFound)
		if err != nil {
			return fmt.Errorf("unable to create route '%s': %w", path, err)
		}
	}

	for path, endpoint := range cfg.Endpoints {
		for idx, fwd := range endpoint.Forwardings {
			idx, fwd := idx, fwd
			observability.Go(ctx, func() {
				switch {
				case fwd.Destination.Route != "":
					_, err := srv.AddRouteForwardingLocal(
						ctx, path, fwd.Destination.Route, fwd.Recoding,
					)
					if err != nil {
						logger.Errorf(ctx, "unable to create forwarding from '%s' to a local stream '%s': %v", path, fwd.Destination.Route, err)
						return
					}
				case fwd.Destination.URL != "":
					_, err := srv.AddRouteForwardingToRemote(
						ctx,
						path,
						fwd.Destination.URL, secret.New(""),
						fwd.Recoding,
					)
					if err != nil {
						logger.Errorf(ctx, "unable to create forwarding from '%s' to a remote destination '%s': %v", path, fwd.Destination.URL, err)
						return
					}
				default:
					logger.Debugf(ctx, "skipped forwarding #%d: no destination", idx)
				}
			})
		}
	}

	return nil
}
