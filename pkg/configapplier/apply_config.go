package configapplier

import (
	"context"
	"fmt"
	"net"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avd/pkg/avd"
	"github.com/xaionaro-go/avd/pkg/config"
	"github.com/xaionaro-go/secret"
)

func ApplyConfig(
	ctx context.Context,
	cfg config.Config,
	srv *avd.Server,
) error {
	for _, port := range cfg.Ports {
		proto, host, err := port.Address.Parse(ctx)
		if err != nil {
			return fmt.Errorf("unable to parse the port string '%s': %w", port.Address, err)
		}
		logger.Debugf(ctx, "parsed: transport='%s', host='%s' (orig='%s')", proto, host, port.Address)
		listener, err := net.Listen(proto, host)
		if err != nil {
			return fmt.Errorf("unable to start listening on '%s': %w", port.Address, err)
		}

		protocol, err := port.ProtocolHandler.Protocol()
		if err != nil {
			return fmt.Errorf("unable to identify which protocol to use on '%s': %w", port.Address, err)
		}

		_, err = srv.Listen(ctx, listener, protocol, port.Mode, port.ListenOptions()...)
		if err != nil {
			return fmt.Errorf("unable to listen '%s' with the RTMP-%s handler: %w", listener.Addr(), port.Mode, err)
		}
	}

	for path := range cfg.Endpoints {
		_, err := srv.Router.GetRoute(ctx, path, avd.GetRouteModeCreate)
		if err != nil {
			return fmt.Errorf("unable to create route '%s': %w", path, err)
		}
	}

	for path, endpoint := range cfg.Endpoints {
		for idx, fwd := range endpoint.Forwardings {
			switch {
			case fwd.Destination.Route != "":
				_, err := srv.AddRouteForwardingLocal(
					ctx,
					path, avd.GetRouteModeFailIfNotFound,
					fwd.Destination.Route, avd.GetRouteModeFailIfNotFound,
					fwd.Recoding,
				)
				if err != nil {
					return fmt.Errorf("unable to create forwarding from '%s' to a local stream '%s': %w", path, fwd.Destination.Route, err)
				}
			case fwd.Destination.URL != "":
				_, err := srv.AddRouteForwardingToRemote(
					ctx,
					path,
					fwd.Destination.URL, secret.New(""),
					avd.GetRouteModeFailIfNotFound,
					fwd.Recoding,
				)
				if err != nil {
					return fmt.Errorf("unable to create forwarding from '%s' to a remote destination '%s': %w", path, fwd.Destination.URL, err)
				}
			default:
				logger.Debugf(ctx, "skipped forwarding #%d: no destination", idx)
			}
		}
	}

	return nil
}
