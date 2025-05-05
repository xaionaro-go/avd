package configapplier

import (
	"context"
	"fmt"
	"net"
	"net/url"

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
	for idx, port := range cfg.Ports {
		urlParsed, err := url.Parse(port.Address)
		if err != nil {
			return fmt.Errorf("unable to parse the port URL '%s' of port #%d: %w", port.Address, idx, err)
		}
		listener, err := net.Listen(urlParsed.Scheme, urlParsed.Host)
		if err != nil {
			return fmt.Errorf("unable to start listening on %s: %w", urlParsed, err)
		}
		switch {
		case port.RTMP != nil:
			switch port.RTMP.Mode {
			case config.RTMPModePublishers:
				_, err := srv.ListenRTMPPublisher(ctx, listener)
				if err != nil {
					return fmt.Errorf("unable to listen %s with the RTMP-publishers handler: %w", listener.Addr(), err)
				}
			default:
				return fmt.Errorf("the support of RTMP port mode '%s' is not implemented", port.RTMP.Mode)
			}
		default:
			return fmt.Errorf("unknown/missing port type in port #%d (%s)", idx, port.Address)
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
			case fwd.Destination.URL != "":
				_, err := srv.AddForwardingToRemote(
					ctx,
					path,
					fwd.Destination.URL, secret.New(""),
					avd.GetRouteModeWaitForPublisher,
					fwd.Recoding,
				)
				if err != nil {
					return fmt.Errorf("unable to create forwarding from '%s' to a remote destination '%s': %w", path, fwd.Destination.URL, err)
				}
			case fwd.Destination.Route != "":
				_, err := srv.AddForwardingLocal(
					ctx,
					path, avd.GetRouteModeWaitForPublisher,
					fwd.Destination.Route, avd.GetRouteModeFailIfNotFound,
					fwd.Recoding,
				)
				if err != nil {
					return fmt.Errorf("unable to create forwarding from '%s' to a local stream '%s': %w", path, fwd.Destination.Route, err)
				}
			default:
				logger.Debugf(ctx, "skipped forwarding #%d: no destination", idx)
			}
		}
	}

	return nil
}
