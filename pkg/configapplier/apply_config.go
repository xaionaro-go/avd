// Package configapplier applies the configuration to the AVD server.
package configapplier

import (
	"context"
	"fmt"
	"net"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avd/pkg/avd"
	"github.com/xaionaro-go/avd/pkg/avd/types"
	"github.com/xaionaro-go/avd/pkg/config"
	grpcserver "github.com/xaionaro-go/avd/pkg/management/grpc/server"
	"github.com/xaionaro-go/avpipeline/router"
	"github.com/xaionaro-go/observability"
	"github.com/xaionaro-go/secret"
)

func getListener(ctx context.Context, addr avd.PortAddress) (_ret net.Listener, _err error) {
	logger.Debugf(ctx, "getListener")
	defer func() { logger.Debugf(ctx, "/getListener: %v %v", _ret, _err) }()

	proto, hostPort, err := addr.Parse(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to parse service port '%s': %w", addr, err)
	}
	logger.Debugf(ctx, "transport='%s', hostport='%s' (orig='%s')", proto, hostPort, addr)
	listener, err := net.Listen(proto, hostPort)
	if err != nil {
		return nil, fmt.Errorf("unable to start listening on '%s': %w", hostPort, err)
	}
	return listener, nil
}

func initServicePortManagementGRPC(
	ctx context.Context,
	srv *avd.Server,
	port config.ServicePortConfig,
) (_err error) {
	logger.Debugf(ctx, "initServicePortManagementGRPC")
	defer func() { logger.Debugf(ctx, "/initServicePortManagementGRPC: %v", _err) }()
	listener, err := getListener(ctx, port.Address)
	if err != nil {
		return err
	}
	observability.Go(ctx, func(ctx context.Context) {
		defer listener.Close()
		logger.Infof(ctx, "management gRPC server started on '%s'", port.Address)
		err := grpcserver.New(srv, listener).ServeContext(ctx)
		if err != nil {
			logger.Errorf(ctx, "failed to serve gRPC server on '%s': %v", port.Address, err)
		}
	})
	return nil
}

func initServicePortManagement(
	ctx context.Context,
	srv *avd.Server,
	port config.ServicePortConfig,
) (_err error) {
	logger.Debugf(ctx, "initServicePortManagement")
	defer func() { logger.Debugf(ctx, "/initServicePortManagement: %v", _err) }()

	mgmt := port.Service.Management
	switch mgmt.ServiceProtocol {
	case types.ServiceProtocolGRPC:
		return initServicePortManagementGRPC(ctx, srv, port)
	default:
		return fmt.Errorf("service port '%s' is configured for an unsupported management service protocol '%s'", port.Address, mgmt.ServiceProtocol)
	}
}

func initServicePort(
	ctx context.Context,
	srv *avd.Server,
	port config.ServicePortConfig,
) (_err error) {
	logger.Debugf(ctx, "initServicePort")
	defer func() { logger.Debugf(ctx, "/initServicePort: %v", _err) }()

	switch {
	case port.Service.Management != nil:
		return initServicePortManagement(ctx, srv, port)
	default:
		return fmt.Errorf("service port '%s' is not configured for any known service", port.Address)
	}
}

func ApplyConfig(
	ctx context.Context,
	cfg config.Config,
	srv *avd.Server,
) error {
	logger.Debugf(ctx, "configuring the command handler...")
	commandHandler := newCommandHandler[avd.RouteCustomData](ctx, cfg)
	srv.Router.OnRoutePublisherAdded = commandHandler.OnRoutePublisherAdded
	srv.Router.OnRoutePublisherRemoved = commandHandler.OnRoutePublisherRemoved

	logger.Debugf(ctx, "configuring service listening ports...")
	for _, port := range cfg.ServicePorts {
		if err := initServicePort(ctx, srv, port); err != nil {
			return fmt.Errorf("unable to initialize service port '%s': %w", port.Address, err)
		}
	}

	logger.Debugf(ctx, "configuring streaming listening ports...")
	for _, port := range cfg.StreamingPorts {
		protocol, err := port.ProtocolHandler.Protocol()
		if err != nil {
			return fmt.Errorf("unable to identify which protocol to use on '%s': %w", port.Address, err)
		}

		_, err = srv.Listen(ctx, port.Address, protocol, port.Mode, port.ListenOptions()...)
		if err != nil {
			return fmt.Errorf("unable to listen '%s' with the %s-%s handler: %w", port.Address, protocol, port.Mode, err)
		}
	}

	logger.Debugf(ctx, "configuring the endpoints...")
	for path, endpoint := range cfg.Endpoints {
		_, err := srv.Router.GetRoute(ctx, path, router.GetRouteModeCreatePersistentIfNotFound)
		if err != nil {
			return fmt.Errorf("unable get-or-create route '%s': %w", path, err)
		}
		for idx, fwd := range endpoint.Forwardings {
			idx, fwd := idx, fwd
			observability.Go(ctx, func(ctx context.Context) {
				switch {
				case fwd.Destination.Local != nil:
					_, err := srv.AddRouteForwardingLocal(
						ctx,
						path,
						fwd.Destination.Local.Route,
						router.PublishMode(fwd.Destination.Local.PublishMode),
						fwd.Transcoding,
					)
					if err != nil {
						logger.Errorf(
							ctx,
							"unable to create forwarding from '%s' to a local stream '%s': %v",
							path,
							fwd.Destination.Local.Route,
							fwd.Destination.Local.PublishMode,
							err,
						)
						return
					}
				case fwd.Destination.URL != nil:
					_, err := srv.AddRouteForwardingToRemote(
						ctx,
						path,
						*fwd.Destination.URL, secret.New(""),
						fwd.Transcoding,
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
