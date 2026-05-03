// endpoint_resolver_test.go covers the regex-pattern endpoint resolver:
// per-path sync.Once wiring, refcount accounting, render/wire failure
// semantics, declaration-order pattern matching, literal-shadow
// avoidance, deterministic concurrent-arrivals, and persistence of
// materialized routes across publisher reconnect cycles.

package configapplier

import (
	"context"
	"fmt"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"text/template"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xaionaro-go/avd/pkg/avd/types"
	"github.com/xaionaro-go/avd/pkg/config"
)

// makePattern builds an EndpointPattern from a raw regex and an inline
// template body. Test-only helper to keep test setup terse. Uses the
// same [[ ... ]] delimiters as production pattern bodies (see
// pkg/config/endpoint_pattern.go) so the test renders match
// production behaviour exactly.
func makePattern(t *testing.T, raw, body string) config.EndpointPattern {
	t.Helper()
	re, err := regexp.Compile(raw)
	require.NoError(t, err)
	tmpl, err := template.New("test:" + raw).
		Delims(config.EndpointPatternBodyLeftDelim, config.EndpointPatternBodyRightDelim).
		Parse(body)
	require.NoError(t, err)
	return config.EndpointPattern{Raw: raw, Regexp: re, Template: tmpl}
}

// fakeWirer is the test-side endpointWirer. Wire defaults to no-op
// success; tests override wireFn as needed. Pattern-materialized
// routes are persistent (no Release-driven eviction — see Release
// godoc), so the wirer interface intentionally has no removal
// counterpart; persistence is now asserted by the cache-and-refcount
// observations in the tests below rather than by counting wirer
// removal calls.
type fakeWirer struct {
	wireFn func(context.Context, types.RoutePath, config.EndpointConfig) error
}

func newFakeWirer() *fakeWirer {
	return &fakeWirer{
		wireFn: func(context.Context, types.RoutePath, config.EndpointConfig) error { return nil },
	}
}

func (f *fakeWirer) Wire(ctx context.Context, p types.RoutePath, ep config.EndpointConfig) error {
	return f.wireFn(ctx, p, ep)
}

// resolverCacheLen reads the resolver's cache size under its lock.
// Whitebox helper kept inside the same package so production code does
// not need to expose a cache-length accessor.
func resolverCacheLen(r *EndpointResolver) int {
	if r == nil {
		return 0
	}
	var n int
	r.locker.Do(context.Background(), func() {
		n = len(r.cache)
	})
	return n
}

func TestEndpointResolver_Acquire_NoMatchReturnsFalse(t *testing.T) {
	ctx := context.Background()
	r := NewEndpointResolver(newFakeWirer(), nil, []config.EndpointPattern{
		makePattern(t, `^cam/(?P<id>[0-9]+)$`, `forwardings: []`),
	})

	matched, err := r.EnsureWired(ctx, types.RoutePath("does-not-match"))
	require.NoError(t, err)
	assert.False(t, matched, "EnsureWired must return matched=false for a non-matching path")
}

func TestEndpointResolver_Acquire_LiteralIsNoOp(t *testing.T) {
	// Literal lookup wins over patterns: if the path was wired
	// eagerly via cfg.Endpoints, the resolver does not redo work.
	// Acquire on such a path returns nil (success) and does NOT
	// register the path in the resolver's cache.
	ctx := context.Background()
	r := NewEndpointResolver(newFakeWirer(), nil, nil) // no patterns at all
	err := r.Acquire(ctx, types.RoutePath("eager_literal"))
	require.NoError(t, err)
	assert.Equal(t, 0, resolverCacheLen(r), "literal/no-match Acquire must not populate the cache")
}

func TestEndpointResolver_Acquire_LiteralShadowsPattern(t *testing.T) {
	// Both a literal endpoint at "cam/42" AND a pattern matching
	// "^cam/.*$" are configured. A publisher arriving at "cam/42"
	// must NOT be materialized through the resolver — the literal
	// wins, the cache stays empty, and Release does not evict the
	// literal route. Verifies the regression described in REJECT-3.
	ctx := context.Background()
	wirer := newFakeWirer()
	wireCount := atomic.Int64{}
	wirer.wireFn = func(context.Context, types.RoutePath, config.EndpointConfig) error {
		wireCount.Add(1)
		return nil
	}

	literals := map[types.RoutePath]config.EndpointConfig{
		"cam/42": {},
	}
	r := NewEndpointResolver(wirer, literals, []config.EndpointPattern{
		makePattern(t, `^cam/.*$`, `forwardings: []`),
	})

	matched, err := r.EnsureWired(ctx, types.RoutePath("cam/42"))
	require.NoError(t, err)
	assert.False(t, matched, "literal-shadowed path must not match a pattern")
	assert.Equal(t, int64(0), wireCount.Load(), "literal-shadowed path must not invoke the wirer")
	assert.Equal(t, 0, resolverCacheLen(r), "literal-shadowed path must not populate the cache")

	// A non-literal path that matches the same pattern must still wire.
	matched, err = r.EnsureWired(ctx, types.RoutePath("cam/99"))
	require.NoError(t, err)
	assert.True(t, matched, "non-literal matching path must materialize")
	assert.Equal(t, int64(1), wireCount.Load(), "wirer must run for non-literal pattern match")
	assert.Equal(t, 1, resolverCacheLen(r), "cache must have the non-literal entry")
}

func TestEndpointResolver_Acquire_MatchingPattern_WiresOnce(t *testing.T) {
	ctx := context.Background()
	var wireCount atomic.Int64
	wirer := newFakeWirer()
	wirer.wireFn = func(context.Context, types.RoutePath, config.EndpointConfig) error {
		wireCount.Add(1)
		return nil
	}
	r := NewEndpointResolver(wirer, nil, []config.EndpointPattern{
		makePattern(t, `^cam/(?P<id>[0-9]+)$`, `{}`),
	})

	require.NoError(t, r.Acquire(ctx, types.RoutePath("cam/42")))
	require.NoError(t, r.Acquire(ctx, types.RoutePath("cam/42")))
	require.NoError(t, r.Acquire(ctx, types.RoutePath("cam/42")))
	assert.Equal(t, int64(1), wireCount.Load(), "wireEndpoint must run exactly once for the same path")
}

func TestEndpointResolver_Acquire_ConcurrentArrivals_WiresOnce(t *testing.T) {
	// Deterministic gate: all 16 goroutines start, the one inside
	// Once.Do blocks on the gate while the rest queue up; we then
	// close the gate, the wire returns, the rest skip via Once.
	// After the dust settles we assert wire-count==1 AND refcount==16
	// AND that releasing 16 times leaves the cache populated (pattern
	// routes mirror literal-endpoint persistence — see Release in
	// endpoint_resolver.go). Persistence is observed via the cache-len
	// + refcount + wireCount triple; the wirer interface has no
	// removal method to count.
	ctx := context.Background()
	const goroutines = 16

	gate := make(chan struct{})
	var wireCount atomic.Int64
	wirer := newFakeWirer()
	wirer.wireFn = func(context.Context, types.RoutePath, config.EndpointConfig) error {
		<-gate
		wireCount.Add(1)
		return nil
	}

	r := NewEndpointResolver(wirer, nil, []config.EndpointPattern{
		makePattern(t, `^cam/(?P<id>[0-9]+)$`, `{}`),
	})

	// Track when each goroutine has started so we can let them all
	// race for the lock before unblocking the wire.
	started := make(chan struct{}, goroutines)
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			started <- struct{}{}
			err := r.Acquire(ctx, types.RoutePath("cam/9"))
			assert.NoError(t, err)
		}()
	}
	for i := 0; i < goroutines; i++ {
		<-started
	}
	close(gate)
	wg.Wait()

	assert.Equal(t, int64(1), wireCount.Load(), "wireEndpoint must run exactly once under concurrent Acquire")
	assert.Equal(t, goroutines, r.EntryRefcount(ctx, "cam/9"), "all 16 acquires must bump the refcount")
	assert.Equal(t, 1, resolverCacheLen(r), "exactly one cache entry expected")

	// Release all 16 references. Per the new contract the route is
	// retained: the cache entry remains and the refcount drops to zero.
	// wireCount must NOT grow during release (no re-wiring on the
	// drain path).
	for i := 0; i < goroutines; i++ {
		r.Release(ctx, "cam/9")
	}
	assert.Equal(t, int64(1), wireCount.Load(), "Release must NOT trigger any further wire calls")
	assert.Equal(t, 1, resolverCacheLen(r), "cache entry must persist after final Release")
	assert.Equal(t, 0, r.EntryRefcount(ctx, "cam/9"), "refcount must drop to zero after final Release")
}

// TestEndpointResolver_RefcountAccountingPersistsRoute verifies that
// pattern-materialized routes are retained for the lifetime of the
// resolver to mirror literal-endpoint persistence. The refcount still
// tracks outstanding holders, but it does not drive eviction.
func TestEndpointResolver_RefcountAccountingPersistsRoute(t *testing.T) {
	ctx := context.Background()
	var wireCount atomic.Int64
	wirer := newFakeWirer()
	wirer.wireFn = func(context.Context, types.RoutePath, config.EndpointConfig) error {
		wireCount.Add(1)
		return nil
	}
	r := NewEndpointResolver(wirer, nil, []config.EndpointPattern{
		makePattern(t, `^cam/(?P<id>[0-9]+)$`, `{}`),
	})

	const n = 5
	for i := 0; i < n; i++ {
		require.NoError(t, r.Acquire(ctx, types.RoutePath("cam/7")))
	}
	assert.Equal(t, int64(1), wireCount.Load(), "wireFn must run exactly once across n acquires")
	assert.Equal(t, n, r.EntryRefcount(ctx, "cam/7"), "refcount must equal n acquires")

	for i := 0; i < n; i++ {
		r.Release(ctx, types.RoutePath("cam/7"))
	}
	assert.Equal(t, int64(1), wireCount.Load(), "Release must not trigger any further wire calls (route is persistent)")
	assert.Equal(t, 1, resolverCacheLen(r), "cache entry must persist after refcount reaches zero")
	assert.Equal(t, 0, r.EntryRefcount(ctx, "cam/7"), "refcount must reach zero after matching releases")

	// Extra Release must not underflow the refcount.
	r.Release(ctx, types.RoutePath("cam/7"))
	assert.Equal(t, 0, r.EntryRefcount(ctx, "cam/7"), "extra Release must clamp refcount at zero")
	assert.Equal(t, 1, resolverCacheLen(r), "extra Release must keep the cache entry")
}

// TestEndpointResolver_PublisherReconnectKeepsRoute reproduces the
// reconnect bug: a publisher arrives, departs, and re-arrives at the
// same regex-pattern path. The materialized route MUST survive the
// disconnect so that the persistent forwarders attached to the route
// node continue running and the re-Acquire reuses the same node
// without re-invoking the wirer (which previously raced the forwarder
// auto-restart waiter and produced duplicate forwarders).
func TestEndpointResolver_PublisherReconnectKeepsRoute(t *testing.T) {
	ctx := context.Background()
	var wireCalls atomic.Int64
	wirer := newFakeWirer()
	wirer.wireFn = func(context.Context, types.RoutePath, config.EndpointConfig) error {
		wireCalls.Add(1)
		return nil
	}
	r := NewEndpointResolver(wirer, nil, []config.EndpointPattern{
		makePattern(t, `^cam/(?P<id>\d+)$`, `{}`),
	})

	// First publisher arrival: wire once, refcount==1.
	require.NoError(t, r.Acquire(ctx, types.RoutePath("cam/1")))
	assert.Equal(t, int64(1), wireCalls.Load(), "first Acquire must wire exactly once")
	assert.Equal(t, 1, resolverCacheLen(r), "first Acquire must populate the cache")
	assert.Equal(t, 1, r.EntryRefcount(ctx, "cam/1"), "refcount must be 1 after first Acquire")

	// Publisher disconnect: refcount drops to zero, but the cache
	// entry and the underlying route MUST persist. wireCalls must
	// NOT grow during Release.
	r.Release(ctx, types.RoutePath("cam/1"))
	assert.Equal(t, int64(1), wireCalls.Load(), "Release must not trigger further wiring")
	assert.Equal(t, 1, resolverCacheLen(r), "cache entry must remain after Release")
	assert.Equal(t, 0, r.EntryRefcount(ctx, "cam/1"), "refcount must be 0 after Release")

	// Publisher reconnect: re-Acquire the same path. The cached
	// entry must be reused — wirer must NOT be invoked again.
	require.NoError(t, r.Acquire(ctx, types.RoutePath("cam/1")))
	assert.Equal(t, int64(1), wireCalls.Load(), "second Acquire must NOT re-invoke the wirer (route is cached)")
	assert.Equal(t, 1, r.EntryRefcount(ctx, "cam/1"), "refcount must be 1 after reconnect Acquire")
}

func TestEndpointResolver_WireFailure_NotCached(t *testing.T) {
	ctx := context.Background()
	wireCalls := atomic.Int64{}
	failOnce := atomic.Bool{}
	wirer := newFakeWirer()
	wirer.wireFn = func(context.Context, types.RoutePath, config.EndpointConfig) error {
		wireCalls.Add(1)
		if failOnce.CompareAndSwap(false, true) {
			return fmt.Errorf("synthetic wire failure")
		}
		return nil
	}
	r := NewEndpointResolver(wirer, nil, []config.EndpointPattern{
		makePattern(t, `^cam/(?P<id>[0-9]+)$`, `{}`),
	})

	matched, err := r.EnsureWired(ctx, types.RoutePath("cam/3"))
	require.True(t, matched)
	require.Error(t, err, "first call must surface wire failure")
	assert.Equal(t, 0, resolverCacheLen(r), "failed wiring must not be cached")

	matched, err = r.EnsureWired(ctx, types.RoutePath("cam/3"))
	require.True(t, matched)
	require.NoError(t, err, "retry must succeed and cache")
	assert.Equal(t, int64(2), wireCalls.Load(), "wireFn must run twice (failed once, then retry)")
}

func TestEndpointResolver_RenderError_NotCached(t *testing.T) {
	// [[ .NoSuchField.Sub ]] errors at template execution time
	// because the data struct has no such field. Render failure must
	// surface as an error and must NOT be cached so a subsequent call
	// retries.
	ctx := context.Background()
	r := NewEndpointResolver(newFakeWirer(), nil, []config.EndpointPattern{
		makePattern(t, `^cam/(?P<id>[0-9]+)$`, `forwardings: [{destination: {url: "[[ .NoSuchField.Sub ]]"}}]`),
	})

	matched, err := r.EnsureWired(ctx, types.RoutePath("cam/9"))
	assert.True(t, matched)
	assert.Error(t, err, "template-render failure must error")
	assert.Equal(t, 0, resolverCacheLen(r), "render failure must not be cached")
}

func TestEndpointResolver_PatternsTriedInDeclarationOrder(t *testing.T) {
	ctx := context.Background()
	winner := atomic.Value{}
	winner.Store("")

	// Two overlapping patterns; the first must win.
	patFirst := makePattern(t, `^cam/(?P<id>[0-9]+)$`, `forwardings: [{destination: {local: {route: "first"}}}]`)
	patSecond := makePattern(t, `^cam/(?P<id>[0-9]+)$`, `forwardings: [{destination: {local: {route: "second"}}}]`)
	wirer := newFakeWirer()
	wirer.wireFn = func(_ context.Context, _ types.RoutePath, ep config.EndpointConfig) error {
		require.Len(t, ep.Forwardings, 1)
		require.NotNil(t, ep.Forwardings[0].Destination.Local)
		winner.Store(string(ep.Forwardings[0].Destination.Local.Route))
		return nil
	}
	r := NewEndpointResolver(wirer, nil, []config.EndpointPattern{patFirst, patSecond})

	require.NoError(t, r.Acquire(ctx, types.RoutePath("cam/1")))
	assert.Equal(t, "first", winner.Load(), "first declared matching pattern must win")
}

func TestEndpointResolver_NamedCaptureRendered(t *testing.T) {
	ctx := context.Background()
	got := atomic.Value{}
	got.Store("")
	wirer := newFakeWirer()
	wirer.wireFn = func(_ context.Context, _ types.RoutePath, ep config.EndpointConfig) error {
		require.Len(t, ep.Forwardings, 1)
		require.NotNil(t, ep.Forwardings[0].Destination.Local)
		got.Store(string(ep.Forwardings[0].Destination.Local.Route))
		return nil
	}
	r := NewEndpointResolver(wirer, nil, []config.EndpointPattern{
		makePattern(t, `^cam/(?P<id>[0-9]+)$`, `forwardings: [{destination: {local: {route: "x-[[ .Match.id ]]"}}}]`),
	})

	require.NoError(t, r.Acquire(ctx, types.RoutePath("cam/123")))
	assert.Equal(t, "x-123", got.Load(), "named capture .Match.id must render into the route path")
}

// TestEndpointResolver_WireCtxSurvivesPublisherCancel pins the Bug-1 fix
// (regex-pattern forwarder context lifetime). The wirer must receive a
// context whose Done() channel is decoupled from the originating
// caller's ctx — even when the originating ctx is cancelled (e.g. the
// publisher disconnects after triggering the lazy materialization),
// the wired forwarder's lifetime ctx must NOT see the cancellation.
//
// Pre-fix behaviour: the wirer received the publisher's request ctx
// directly, so RouteForwarding.openLocked's context.WithCancel inherited
// the publisher's cancellation. The forwarder's watcher goroutine then
// took the <-ctx.Done() branch on disconnect and exited, while sync.Once
// short-circuited subsequent EnsureWired calls — production logs showed
// 64,586 "nowhere to push" drops because the route node lived but the
// forwarder did not.
//
// Falsification protocol: revert the xcontext.DetachDone(ctx) call in
// renderAndWire — this test must fail, observing wireCtxDone closed.
func TestEndpointResolver_WireCtxSurvivesPublisherCancel(t *testing.T) {
	parentCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wireCtx context.Context
	var wireCtxMu sync.Mutex
	wirer := &fakeWirer{
		wireFn: func(ctx context.Context, _ types.RoutePath, _ config.EndpointConfig) error {
			wireCtxMu.Lock()
			defer wireCtxMu.Unlock()
			wireCtx = ctx
			return nil
		},
	}
	r := NewEndpointResolver(wirer, nil, []config.EndpointPattern{
		makePattern(t, `^cam/(?P<id>\d+)$`, `{}`),
	})

	matched, err := r.EnsureWired(parentCtx, types.RoutePath("cam/7"))
	require.NoError(t, err)
	require.True(t, matched)

	wireCtxMu.Lock()
	captured := wireCtx
	wireCtxMu.Unlock()
	require.NotNil(t, captured, "wirer must have been invoked")

	// The wirer's ctx must NOT be triggered by parent cancellation —
	// this is the load-bearing assertion of the Bug-1 fix. Cancel
	// parent and verify wirer's ctx.Done() does not close.
	cancel()
	select {
	case <-captured.Done():
		t.Fatal("Bug-1 regression: wired ctx.Done() closed when parent ctx was cancelled — forwarder would die with originating publisher")
	default:
	}
	require.NoError(t, captured.Err(), "wired ctx must not surface parent cancellation as its Err")
}
