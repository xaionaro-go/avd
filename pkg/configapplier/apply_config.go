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

// forwardingSetupInitialDeadline is the window wireEndpoint waits
// after spawning all per-forwarding goroutines to collect any
// synchronous configuration-level errors (parse URL, resolve
// transcoder, check the destination). Errors that surface later (for
// example, after the wait-for-publisher blocking call returns) still
// go to logs but no longer block the wiring call from returning.
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

// wireEndpoint wires a single endpoint at path on srv. Used by the
// resolver's lazy-materialization path (one endpoint at a time, each
// call gets its own deadline). Internally calls
// spawnEndpointForwardings to fan out per-forwarding goroutines and
// then blocks for forwardingSetupInitialDeadline collecting any
// synchronous setup errors. Errors that surface after the deadline
// (e.g. wait-for-publisher returning much later) continue to be
// logged but no longer block the wiring call.
//
// For startup-time eager wiring (cfg.Endpoints), ApplyConfig drives
// all endpoints in parallel with a single shared deadline — see the
// applyLiteralEndpoints helper.
func wireEndpoint(
	ctx context.Context,
	srv *avd.Server,
	path types.RoutePath,
	endpoint config.EndpointConfig,
) error {
	errChan := make(chan error, len(endpoint.Forwardings))
	if err := spawnEndpointForwardings(ctx, srv, path, endpoint, errChan); err != nil {
		return err
	}
	if len(endpoint.Forwardings) == 0 {
		return nil
	}
	return collectForwardingErrors(ctx, errChan, len(endpoint.Forwardings), forwardingSetupInitialDeadline)
}

// spawnEndpointForwardings creates the persistent route at path and
// spawns one goroutine per forwarding, each sending exactly once on
// errChan. errChan must have capacity ≥ the total number of expected
// senders so the per-forwarding goroutines never block on send. Used
// by both the eager and lazy wiring paths.
func spawnEndpointForwardings(
	ctx context.Context,
	srv *avd.Server,
	path types.RoutePath,
	endpoint config.EndpointConfig,
	errChan chan<- error,
) error {
	if _, err := srv.Router.GetRoute(ctx, path, router.GetRouteModeCreatePersistentIfNotFound); err != nil {
		return fmt.Errorf("unable get-or-create route '%s': %w", path, err)
	}
	for idx, fwd := range endpoint.Forwardings {
		idx, fwd := idx, fwd
		observability.Go(ctx, func(ctx context.Context) {
			errChan <- wireForwarding(ctx, srv, path, idx, fwd)
		})
	}
	return nil
}

// collectForwardingErrors waits up to deadline for expected results
// to arrive on errChan, joining any non-nil errors. If the deadline
// (or ctx) trips before all expected senders have reported, the call
// returns the errors collected so far. Late errors continue to be
// reported via the per-forwarding logging in wireForwarding.
func collectForwardingErrors(
	ctx context.Context,
	errChan <-chan error,
	expected int,
	deadline time.Duration,
) error {
	timer := time.NewTimer(deadline)
	defer timer.Stop()
	var errs []error
	collected := 0
	for collected < expected {
		select {
		case err := <-errChan:
			collected++
			if err != nil {
				errs = append(errs, err)
			}
		case <-timer.C:
			collected = expected
		case <-ctx.Done():
			errs = append(errs, ctx.Err())
			collected = expected
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("one or more forwardings failed to set up: %w", errors.Join(errs...))
	}
	return nil
}

// wireForwarding sets up a single forwarding's filters and
// destination chain. It returns the per-forwarding setup error or
// nil; longer-running failures (after AddRouteForwardingToRemote's
// internal wait-for-publisher returns) only surface in the logs.
func wireForwarding(
	ctx context.Context,
	srv *avd.Server,
	path types.RoutePath,
	idx int,
	fwd config.ForwardConfig,
) error {
	blurFactory, blurFilter := newPrivacyBlurFactory(fwd.PrivacyBlur)
	deblemishFactory, deblemishFilter := newDeblemishFactory(fwd.Deblemish)
	whisperFactory, whisperFilter := newWhisperFactory(fwd.Whisper)

	// When privacy blur has face detection, deblemish is redundant
	// on blurred faces. Wrap the deblemish factory with a runtime
	// guard that skips it whenever the privacy blur filter is
	// enabled — so toggling privacy blur via avcli automatically
	// suppresses deblemish.
	if deblemishFactory != nil && blurFilter != nil && fwd.PrivacyBlur != nil && fwd.PrivacyBlur.Faces {
		deblemishFactory = wrapWithSkipGuard(deblemishFactory, &blurFilter.Enabled)
	}

	filterKernelFactory := composeFilterKernelFactories(blurFactory, deblemishFactory, whisperFactory)
	if filterKernelFactory != nil && fwd.Transcoding == nil {
		logger.Warnf(ctx, "forwarding #%d: filter kernels require transcoding to be enabled; filters will be ignored", idx)
		filterKernelFactory = nil
		blurFilter = nil
		deblemishFilter = nil
		whisperFilter = nil
	}
	// Defer the per-forwarding filter registrations until after the
	// underlying AddRouteForwarding* call returns nil — on failure the
	// registry must stay clean so a subsequent ListFilterControls /
	// Get*State call cannot resolve to an orphan handle. This mirrors
	// the AVSync pattern (registered only after AddRouteForwardingToRemote
	// succeeds, see newAVSyncForRemoteForwarding).
	registerFilters := func() {
		if blurFilter != nil {
			srv.RegisterPrivacyBlurFilter(ctx, avd.PrivacyBlurFilterKey{
				RoutePath:       router.RoutePath(path),
				ForwardingIndex: idx,
			}, blurFilter)
		}
		if deblemishFilter != nil {
			srv.RegisterDeblemishFilter(ctx, avd.DeblemishFilterKey{
				RoutePath:       router.RoutePath(path),
				ForwardingIndex: idx,
			}, deblemishFilter)
		}
		if whisperFilter != nil {
			srv.RegisterWhisperFilter(ctx, avd.WhisperFilterKey{
				RoutePath:       router.RoutePath(path),
				ForwardingIndex: idx,
			}, whisperFilter)
		}
	}

	switch {
	case fwd.Destination.Local != nil:
		// Local forwardings reuse the local route's existing
		// publisher chain rather than constructing a per-forwarding
		// kernel.Output, so there is no chain->Output edge to
		// attach an AVSync PushTo Condition to. Skip AVSync wiring
		// here; only remote forwardings get an AVSync.
		logger.Debugf(ctx, "local forwarding '%s' #%d: AVSync wiring skipped (no kernel.Output edge)", path, idx)
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
			return fmt.Errorf("forwarding '%s' -> local '%s' (mode %v): %w", path, fwd.Destination.Local.Route, fwd.Destination.Local.PublishMode, err)
		}
		// Defer registerFilters until AFTER wireOnDemandLocalForwarding
		// has succeeded so an on-demand wiring failure does not leak
		// orphan filter handles into the registry. Mirrors the remote
		// branch's "register only after the destination is fully
		// wired" pattern (newAVSyncForRemoteForwarding registers the
		// AVSync after AddRouteForwardingToRemote returns nil).
		if fwd.OnDemand {
			idleTimeout := time.Duration(fwd.EffectiveIdleTimeoutSec()) * time.Second
			if err := wireOnDemandLocalForwarding(ctx, routeForwarding, idleTimeout); err != nil {
				logger.Errorf(ctx, "unable to wire on-demand local forwarding '%s' -> '%s': %v", path, fwd.Destination.Local.Route, err)
				return fmt.Errorf("on-demand wiring '%s' -> local '%s': %w", path, fwd.Destination.Local.Route, err)
			}
		}
		registerFilters()
		return nil
	case fwd.Destination.URL != nil:
		// Construct an AVSync per-forwarding and attach it as a
		// PushTo Condition on the chain->Output edge for
		// observability + per-mediatype offset application.
		// Register the AVSync only after AddRouteForwardingToRemote
		// succeeds — on failure the registry must stay clean (no
		// leak).
		avSync, avSyncConditions := newAVSyncForRemoteForwarding(ctx)
		_, err := srv.AddRouteForwardingToRemote(
			ctx,
			path,
			*fwd.Destination.URL, secret.New(""),
			fwd.Transcoding,
			filterKernelFactory,
			avSyncConditions,
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
			return fmt.Errorf("forwarding '%s' -> remote '%s': %w", path, *fwd.Destination.URL, err)
		}
		srv.RegisterAVSyncFilter(ctx, avd.AVSyncFilterKey{
			RoutePath:       router.RoutePath(path),
			ForwardingIndex: idx,
		}, avSync)
		registerFilters()
		if fwd.OnDemand {
			// Remote destinations do not have a router-level
			// consumer set: the remote endpoint is effectively
			// always the sole consumer. There is no hook point to
			// gate activation on, so we log and treat the
			// forwarding as eager. A future RPC-triggered
			// activation path could revisit this.
			logger.Warnf(ctx, "on_demand=true is not supported for remote forwardings ('%s' -> '%s'); forwarding will run eagerly", path, *fwd.Destination.URL)
		}
		return nil
	default:
		logger.Debugf(ctx, "skipped forwarding #%d: no destination", idx)
		return nil
	}
}

func ApplyConfig(
	ctx context.Context,
	cfg config.Config,
	srv *avd.Server,
) error {
	logger.Debugf(ctx, "configuring the command handler...")
	commandHandler := newCommandHandler[avd.RouteCustomData](ctx, cfg)
	wirer := newServerWirer(srv, commandHandler)
	resolver := NewEndpointResolver(wirer, cfg.Endpoints, cfg.Patterns)
	commandHandler.Resolver = resolver
	srv.Router.OnRoutePublisherAdded = commandHandler.OnRoutePublisherAdded
	srv.Router.OnRoutePublisherRemoved = commandHandler.OnRoutePublisherRemoved
	srv.Router.OnRouteConsumerAdded = commandHandler.OnRouteConsumerAdded
	srv.Router.OnRouteConsumerRemoved = commandHandler.OnRouteConsumerRemoved
	srv.EndpointResolver = resolver

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
	if err := applyLiteralEndpoints(ctx, srv, cfg.Endpoints); err != nil {
		return err
	}
	return nil
}

// applyLiteralEndpoints wires every literal endpoint in cfg.Endpoints
// in parallel and blocks for forwardingSetupInitialDeadline collecting
// synchronous setup errors. The pre-iteration-2 startup behaviour was
// "one global deadline covering all forwardings"; the iteration-2
// regression had each endpoint take its own deadline serially (N×
// deadline worst case). This restores the original semantics.
//
// errChan is sized for every forwarding the config declares, but only
// successfully-spawned endpoints contribute senders — a
// spawnEndpointForwardings failure (e.g., GetRoute returns an error)
// means that endpoint's per-forwarding goroutines never start, so its
// expected senders must be subtracted from collectForwardingErrors'
// `expected` count. Without that subtraction the wait blocks the
// whole forwardingSetupInitialDeadline waiting for senders that will
// never arrive.
func applyLiteralEndpoints(
	ctx context.Context,
	srv *avd.Server,
	endpoints map[types.RoutePath]config.EndpointConfig,
) error {
	totalForwardings := 0
	for _, endpoint := range endpoints {
		totalForwardings += len(endpoint.Forwardings)
	}
	errChan := make(chan error, totalForwardings)
	var spawnErrs []error
	expected := 0
	for path, endpoint := range endpoints {
		if err := spawnEndpointForwardings(ctx, srv, path, endpoint, errChan); err != nil {
			spawnErrs = append(spawnErrs, err)
			continue
		}
		expected += len(endpoint.Forwardings)
	}
	if err := collectForwardingErrors(ctx, errChan, expected, forwardingSetupInitialDeadline); err != nil {
		spawnErrs = append(spawnErrs, err)
	}
	if len(spawnErrs) > 0 {
		return fmt.Errorf("one or more endpoints failed to wire: %w", errors.Join(spawnErrs...))
	}
	return nil
}
