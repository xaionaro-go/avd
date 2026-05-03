// endpoint_resolver_integration_test.go drives a real publisher
// disconnect/reconnect cycle through the full router + commandHandler +
// EndpointResolver seam. The pure unit test in endpoint_resolver_test.go
// only exercises the resolver in isolation; this file additionally
// wires the resolver into the router callback path the way ApplyConfig
// does in production, so any future refactor that re-introduces an
// eviction step at a different layer (e.g. router GC, commandHandler)
// would surface as a failing test rather than a silent regression.
//
// The test is deterministic: it drives Route.AddPublisher /
// Route.RemovePublisher synchronously, asserts the resolver state
// after each step, and shuts the router down via t.Cleanup. The
// cleanup hook waits for IsServing to clear and then calls
// rtr.Close, whose r.WaitGroup.Wait drains the route Serve
// goroutines' defer chain (onRouteClosed → RemoveRoute). No
// time.Sleep, no real network.

package configapplier

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xaionaro-go/avd/pkg/avd/types"
	"github.com/xaionaro-go/avd/pkg/config"
	"github.com/xaionaro-go/avpipeline/node"
	"github.com/xaionaro-go/avpipeline/router"
)

// integrationPublisher is a minimal router.Publisher that returns nil
// for the input/output node accessors. It is sufficient for the router
// to fire its OnRoutePublisherAdded / OnRoutePublisherRemoved
// callbacks, which is the only behaviour this test relies on.
type integrationPublisher struct {
	name string
	mu   sync.Mutex
}

func (p *integrationPublisher) String() string {
	return p.name
}

func (p *integrationPublisher) Close(_ context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return nil
}

func (p *integrationPublisher) GetInputNode(_ context.Context) node.Abstract {
	return nil
}

func (p *integrationPublisher) GetOutputRoute(_ context.Context) *router.Route[types.RouteCustomData] {
	return nil
}

func (p *integrationPublisher) GetPublishMode(_ context.Context) router.PublishMode {
	return router.PublishModeExclusiveTakeover
}

// TestEndpointResolver_PublisherReconnectViaRouterCallbacks drives a
// real router publisher add → remove → add cycle through the
// commandHandler-wired resolver and asserts the cache + refcount
// progression that the fix guarantees.
//
// Failure mode this test guards against: a future refactor that
// re-introduces eviction at the router or commandHandler layer (e.g.
// "GC unreferenced routes") would silently regress this contract
// because the unit test only sees the resolver layer. Here, the
// eviction would surface as wireCalls==2 on reconnect or cache-len==0
// after release.
func TestEndpointResolver_PublisherReconnectViaRouterCallbacks(t *testing.T) {
	ctx := context.Background()

	rtr := router.New[types.RouteCustomData](ctx)
	t.Cleanup(func() {
		// Mirror avpipeline's newTestRouter cleanup carefully: the
		// route's Serve goroutine has a defer that calls
		// onRouteClosed → RemoveRoute. If we ALSO call RemoveRoute
		// from here while the Serve defer is in flight we hit a
		// double-close panic in router.removeRouteLocked (the close
		// races against rtr.Close's own close(RoutesChangedChan)).
		// So:
		// (1) cancel route contexts to nudge Serve toward exit,
		// (2) poll under r.Locker until RoutesByPath drains — the
		//     drain happens inside removeRouteLocked, which is what
		//     onRouteClosed eventually calls; once the map is empty,
		//     no live Serve defer can still race rtr.Close,
		// (3) only then close the router.
		// Do NOT call RemoveRoute ourselves.
		//
		// Aspect-I L-7 replaced the previous time.Sleep(200ms) with
		// the bounded poll-loop above — the sleep was a flake (it
		// "happened to be enough" rather than synchronizing on the
		// observable property the cleanup actually needs).
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		rtr.Locker.Do(cleanupCtx, func() {
			for _, route := range rtr.RoutesByPath {
				route.CancelFunc()
			}
		})
		drainDeadline := time.After(2 * time.Second)
		for {
			var pathCount int
			rtr.Locker.Do(cleanupCtx, func() { pathCount = len(rtr.RoutesByPath) })
			if pathCount == 0 {
				break
			}
			select {
			case <-drainDeadline:
				t.Logf("route drain timed out with %d routes still mapped; proceeding with Close", pathCount)
				goto closeRouter
			case <-time.After(5 * time.Millisecond):
			}
		}
	closeRouter:
		_ = rtr.Close(cleanupCtx)
	})

	var wireCalls int
	var wireMu sync.Mutex
	wirer := &fakeWirer{
		wireFn: func(_ context.Context, _ types.RoutePath, _ config.EndpointConfig) error {
			wireMu.Lock()
			defer wireMu.Unlock()
			wireCalls++
			return nil
		},
	}
	resolver := NewEndpointResolver(wirer, nil, []config.EndpointPattern{
		makePattern(t, `^cam/(?P<id>\d+)$`, `{}`),
	})

	hdlr := newCommandHandler[types.RouteCustomData](ctx, config.Config{})
	hdlr.Resolver = resolver
	rtr.OnRoutePublisherAdded = hdlr.OnRoutePublisherAdded
	rtr.OnRoutePublisherRemoved = hdlr.OnRoutePublisherRemoved

	const path = types.RoutePath("cam/42")
	route, err := rtr.GetRoute(ctx, path, router.GetRouteModeCreateTemporary)
	require.NoError(t, err)
	require.NotNil(t, route)

	pub1 := &integrationPublisher{name: "pub1"}

	// Step 1 — first arrival. Router invokes OnRoutePublisherAdded
	// synchronously inside AddPublisher (after releasing the lock),
	// which calls Resolver.Acquire; we observe the cache populated
	// and the refcount==1.
	_, err = route.AddPublisher(ctx, pub1)
	require.NoError(t, err)
	wireMu.Lock()
	assert.Equal(t, 1, wireCalls, "first publisher add must wire exactly once")
	wireMu.Unlock()
	assert.Equal(t, 1, resolverCacheLen(resolver), "first add must populate the cache")
	assert.Equal(t, 1, resolver.EntryRefcount(ctx, path), "refcount must be 1 after first add")

	// Step 2 — disconnect. Router invokes OnRoutePublisherRemoved,
	// which calls Resolver.Release; refcount drops to zero but the
	// cache entry MUST persist. wireCalls must NOT grow during
	// release.
	_, err = route.RemovePublisher(ctx, pub1)
	require.NoError(t, err)
	wireMu.Lock()
	assert.Equal(t, 1, wireCalls, "publisher remove must not trigger any wiring")
	wireMu.Unlock()
	assert.Equal(t, 1, resolverCacheLen(resolver), "cache entry must persist after publisher remove")
	assert.Equal(t, 0, resolver.EntryRefcount(ctx, path), "refcount must drop to zero after publisher remove")

	// Step 3 — reconnect. Router invokes OnRoutePublisherAdded
	// again; the resolver must reuse the cached entry (Once already
	// ran), so wireCalls must STILL be 1 and refcount bumps to 1.
	pub2 := &integrationPublisher{name: "pub2"}
	_, err = route.AddPublisher(ctx, pub2)
	require.NoError(t, err)
	wireMu.Lock()
	assert.Equal(t, 1, wireCalls, "reconnect must NOT re-invoke the wirer (cached route is reused)")
	wireMu.Unlock()
	assert.Equal(t, 1, resolverCacheLen(resolver), "cache must still hold exactly one entry")
	assert.Equal(t, 1, resolver.EntryRefcount(ctx, path), "refcount must be 1 after reconnect")

	// Drain — explicit remove so the cleanup hook doesn't race the
	// router shutdown with a live publisher.
	_, err = route.RemovePublisher(ctx, pub2)
	require.NoError(t, err)
	assert.Equal(t, 0, resolver.EntryRefcount(ctx, path), "refcount must be 0 after final remove")
}

// TestEndpointResolver_WireCtxSurvivesPublisherDisconnect pins the
// Bug-1 fix at the integration boundary: the wirer is invoked from
// commandHandler.OnRoutePublisherAdded, which receives the publisher's
// request ctx via Route.AddPublisher → onRoutePublisherAdded. Pre-fix,
// the wirer (and therefore the wired forwarder) inherited that ctx
// directly; cancelling the publisher's ctx at disconnect cancelled the
// forwarder, while sync.Once short-circuited subsequent reconnects from
// re-wiring. The fix detaches Done() at the resolver's renderAndWire
// boundary (xcontext.DetachDone) so the wirer's ctx is unbound from any
// individual publisher's lifetime.
//
// This test asserts the contract at the layer where the bug actually
// manifested: drive a real Route.AddPublisher with a per-publisher
// cancellable ctx, capture the wirer ctx, cancel the publisher ctx, and
// verify the wirer ctx Done() does not fire.
func TestEndpointResolver_WireCtxSurvivesPublisherDisconnect(t *testing.T) {
	ctx := context.Background()

	rtr := router.New[types.RouteCustomData](ctx)
	t.Cleanup(func() {
		// Same shutdown sequence as
		// TestEndpointResolver_PublisherReconnectViaRouterCallbacks:
		// cancel route ctxs, then poll under r.Locker until
		// RoutesByPath drains (the drain happens inside
		// removeRouteLocked, which is what onRouteClosed eventually
		// calls), then close the router. Aspect-I L-7 deduplication
		// note: kept inline (instead of a shared helper) because
		// extracting would force a *router.Router[types.RouteCustomData]
		// generic helper and the two cleanups are short.
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		rtr.Locker.Do(cleanupCtx, func() {
			for _, route := range rtr.RoutesByPath {
				route.CancelFunc()
			}
		})
		drainDeadline := time.After(2 * time.Second)
		for {
			var pathCount int
			rtr.Locker.Do(cleanupCtx, func() { pathCount = len(rtr.RoutesByPath) })
			if pathCount == 0 {
				break
			}
			select {
			case <-drainDeadline:
				t.Logf("route drain timed out with %d routes still mapped; proceeding with Close", pathCount)
				goto closeRouter
			case <-time.After(5 * time.Millisecond):
			}
		}
	closeRouter:
		_ = rtr.Close(cleanupCtx)
	})

	var capturedCtx context.Context
	var capturedCtxMu sync.Mutex
	wirer := &fakeWirer{
		wireFn: func(ctx context.Context, _ types.RoutePath, _ config.EndpointConfig) error {
			capturedCtxMu.Lock()
			defer capturedCtxMu.Unlock()
			capturedCtx = ctx
			return nil
		},
	}
	resolver := NewEndpointResolver(wirer, nil, []config.EndpointPattern{
		makePattern(t, `^cam/(?P<id>\d+)$`, `{}`),
	})

	hdlr := newCommandHandler[types.RouteCustomData](ctx, config.Config{})
	hdlr.Resolver = resolver
	rtr.OnRoutePublisherAdded = hdlr.OnRoutePublisherAdded
	rtr.OnRoutePublisherRemoved = hdlr.OnRoutePublisherRemoved

	const path = types.RoutePath("cam/42")
	route, err := rtr.GetRoute(ctx, path, router.GetRouteModeCreateTemporary)
	require.NoError(t, err)
	require.NotNil(t, route)

	// Per-publisher cancellable ctx mirrors how request handlers
	// (rtmp publisher path) flow ctx into Route.AddPublisher in prod.
	pubCtx, pubCancel := context.WithCancel(ctx)
	pub := &integrationPublisher{name: "pub1"}
	_, err = route.AddPublisher(pubCtx, pub)
	require.NoError(t, err)

	capturedCtxMu.Lock()
	wired := capturedCtx
	capturedCtxMu.Unlock()
	require.NotNil(t, wired, "wirer must have been invoked on first publisher arrival")

	// Cancel the publisher's ctx — simulating a phone disconnect or
	// rtmp connection close. The wirer's ctx (which the production
	// forwarder uses for its watcher goroutine and RouteForwarding's
	// CancelFunc parent) must NOT fire.
	pubCancel()

	select {
	case <-wired.Done():
		t.Fatal("Bug-1 regression: wirer ctx.Done() fired when publisher ctx was cancelled — forwarder would die on disconnect and never restart on reconnect")
	default:
	}
	require.NoError(t, wired.Err(), "wirer ctx must not surface publisher cancellation as its Err")

	// Cleanup the publisher to avoid racing the t.Cleanup teardown.
	_, err = route.RemovePublisher(ctx, pub)
	require.NoError(t, err)
}
