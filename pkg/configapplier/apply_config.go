// apply_config.go provides functions to apply configuration to the AVD server.

// Package configapplier applies the configuration to the AVD server.
package configapplier

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avd/pkg/avd"
	"github.com/xaionaro-go/avd/pkg/avd/types"
	"github.com/xaionaro-go/avd/pkg/config"
	grpcserver "github.com/xaionaro-go/avd/pkg/management/grpc/server"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/router"
	"github.com/xaionaro-go/observability"
	"github.com/xaionaro-go/secret"
)

// forwardingSetupInitialDeadline is the window ApplyConfig waits after
// spawning all per-forwarding goroutines to collect any synchronous
// configuration-level errors (parse URL, resolve transcoder, check the
// destination). Errors that surface later (for example, after the
// wait-for-publisher blocking call returns) still go to logs but no
// longer block ApplyConfig from returning.
const forwardingSetupInitialDeadline = 2 * time.Second

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
	// errChan collects immediate setup errors from the per-forwarding
	// goroutines. It is sized to the maximum possible number of senders
	// so the sends are non-blocking, and each goroutine sends exactly
	// once (nil on success or after the blocking wait-for-publisher
	// returns).
	forwardingCount := 0
	for _, endpoint := range cfg.Endpoints {
		forwardingCount += len(endpoint.Forwardings)
	}
	errChan := make(chan error, forwardingCount)
	for path, endpoint := range cfg.Endpoints {
		_, err := srv.Router.GetRoute(ctx, path, router.GetRouteModeCreatePersistentIfNotFound)
		if err != nil {
			return fmt.Errorf("unable get-or-create route '%s': %w", path, err)
		}
		for idx, fwd := range endpoint.Forwardings {
			idx, fwd := idx, fwd
			observability.Go(ctx, func(ctx context.Context) {
				var sendErr error
				defer func() { errChan <- sendErr }()
				blurFactory, blurControl := newPrivacyBlurFactory(fwd.PrivacyBlur)
				deblemishFactory, deblemishControl := newDeblemishFactory(fwd.Deblemish)

				// When privacy blur has face detection, deblemish is
				// redundant on blurred faces. Wrap the deblemish factory
				// with a runtime guard that skips it whenever the privacy
				// blur control is enabled — so toggling privacy blur via
				// avcli automatically suppresses deblemish.
				if deblemishFactory != nil && blurControl != nil && fwd.PrivacyBlur != nil && fwd.PrivacyBlur.Faces {
					deblemishFactory = wrapWithSkipGuard(deblemishFactory, &blurControl.Enabled)
				}

				filterKernelFactory := composeFilterKernelFactories(blurFactory, deblemishFactory)
				if filterKernelFactory != nil && fwd.Transcoding == nil {
					logger.Warnf(ctx, "forwarding #%d: filter kernels require transcoding to be enabled; filters will be ignored", idx)
					filterKernelFactory = nil
					blurControl = nil
					deblemishControl = nil
				}
				if blurControl != nil {
					srv.RegisterPrivacyBlurControl(ctx, avd.PrivacyBlurControlKey{
						RoutePath:       router.RoutePath(path),
						ForwardingIndex: idx,
					}, blurControl)
				}
				if deblemishControl != nil {
					srv.RegisterDeblemishControl(ctx, avd.DeblemishControlKey{
						RoutePath:       router.RoutePath(path),
						ForwardingIndex: idx,
					}, deblemishControl)
				}

				switch {
				case fwd.Destination.Local != nil:
					routeForwarding, err := srv.AddRouteForwardingLocal(
						ctx,
						path,
						fwd.Destination.Local.Route,
						router.PublishMode(fwd.Destination.Local.PublishMode),
						fwd.Transcoding,
						filterKernelFactory,
					)
					if err != nil {
						logger.Errorf(
							ctx,
							"unable to create forwarding from '%s' to a local stream '%s' (mode %v): %v",
							path,
							fwd.Destination.Local.Route,
							fwd.Destination.Local.PublishMode,
							err,
						)
						sendErr = fmt.Errorf("forwarding '%s' -> local '%s' (mode %v): %w", path, fwd.Destination.Local.Route, fwd.Destination.Local.PublishMode, err)
						return
					}
					if fwd.OnDemand {
						idleTimeout := time.Duration(fwd.EffectiveIdleTimeoutSec()) * time.Second
						if err := wireOnDemandLocalForwarding(ctx, routeForwarding, idleTimeout); err != nil {
							logger.Errorf(ctx, "unable to wire on-demand local forwarding '%s' -> '%s': %v", path, fwd.Destination.Local.Route, err)
							sendErr = fmt.Errorf("on-demand wiring '%s' -> local '%s': %w", path, fwd.Destination.Local.Route, err)
							return
						}
					}
				case fwd.Destination.URL != nil:
					_, err := srv.AddRouteForwardingToRemote(
						ctx,
						path,
						*fwd.Destination.URL, secret.New(""),
						fwd.Transcoding,
						filterKernelFactory,
						kernel.OutputConfig{
							WaitForOutputStreams: &kernel.OutputConfigWaitForOutputStreams{
								MinStreamsVideo:    fwd.WaitUntil.VideoTrackCount,
								MinStreamsAudio:    fwd.WaitUntil.AudioTrackCount,
								MinStreamsSubtitle: fwd.WaitUntil.SubtitleTrackCount,
								MinStreamsData:     fwd.WaitUntil.DataTrackCount,
							},
						},
					)
					if err != nil {
						logger.Errorf(ctx, "unable to create forwarding from '%s' to a remote destination '%s': %v", path, fwd.Destination.URL, err)
						sendErr = fmt.Errorf("forwarding '%s' -> remote '%s': %w", path, *fwd.Destination.URL, err)
						return
					}
					if fwd.OnDemand {
						// Remote destinations do not have a router-level
						// consumer set: the remote endpoint is effectively
						// always the sole consumer. There is no hook point
						// to gate activation on, so we log and treat the
						// forwarding as eager. A future RPC-triggered
						// activation path could revisit this.
						logger.Warnf(ctx, "on_demand=true is not supported for remote forwardings ('%s' -> '%s'); forwarding will run eagerly", path, *fwd.Destination.URL)
					}
				default:
					logger.Debugf(ctx, "skipped forwarding #%d: no destination", idx)
				}
			})
		}
	}

	// Collect any forwarding-setup errors that surface within the initial
	// deadline. This does not catch every failure mode (setup calls that
	// block on GetRouteModeWaitForPublisher may surface errors much later,
	// and those continue to be logged), but it does turn immediate
	// config-level failures — parse URL, resolve transcoder, unknown
	// destination route — into a returned error instead of a silent drop.
	deadline := time.NewTimer(forwardingSetupInitialDeadline)
	defer deadline.Stop()
	var errs []error
	collected := 0
collectLoop:
	for collected < forwardingCount {
		select {
		case err := <-errChan:
			collected++
			if err != nil {
				errs = append(errs, err)
			}
		case <-deadline.C:
			break collectLoop
		case <-ctx.Done():
			errs = append(errs, ctx.Err())
			break collectLoop
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("one or more forwardings failed to set up: %w", errors.Join(errs...))
	}

	return nil
}
