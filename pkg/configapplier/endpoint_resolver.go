// endpoint_resolver.go materializes regex-pattern endpoints on demand.
//
// EndpointResolver is consulted on the first publisher- or
// consumer-arrival for a route path that was not eagerly wired from
// cfg.Endpoints. It walks cfg.Patterns in declaration order, renders
// the first matching pattern's body via text/template (with named
// captures exposed as .Match.<name>), unmarshals the body as
// EndpointConfig, and runs the same wiring logic the eager-startup
// loop runs for literal endpoints. The result is cached by concrete
// RoutePath; a per-entry sync.Once guarantees exactly-once wiring
// under concurrent first-arrivals. A reference count is maintained
// for observability; entries are not evicted on refcount==0 (parity
// with literal endpoints). Explicit eviction is deferred.
//
// Literal endpoints (cfg.Endpoints) take precedence over patterns:
// the resolver short-circuits to "no match" for any path already
// present in the literals set, so a publisher arriving at a literal
// path never accidentally materializes a pattern on top of it (and
// never has the literal route evicted on Release).

package configapplier

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/goccy/go-yaml"
	"github.com/xaionaro-go/avd/pkg/avd/types"
	"github.com/xaionaro-go/avd/pkg/config"
	"github.com/xaionaro-go/xcontext"
	"github.com/xaionaro-go/xsync"
)

// endpointWirer is the resolver's view of the side effects it needs
// to materialize an endpoint at a concrete path. Production wires this
// through serverWirer (built in apply_config.go); tests inject a fake.
// Keeping this as the only wiring dependency means there is exactly one
// configuration path through the resolver, which the tests exercise
// verbatim. Pattern-materialized routes are persistent (mirror literal-
// endpoint semantics — see Release); a route-removal entry point will
// be re-introduced together with the explicit-eviction RPC (deferred).
type endpointWirer interface {
	Wire(ctx context.Context, path types.RoutePath, ep config.EndpointConfig) error
}

// EndpointResolver wires regex-pattern endpoints lazily on first arrival.
type EndpointResolver struct {
	Patterns []config.EndpointPattern

	wirer endpointWirer

	// literals holds the literal-endpoint paths declared in
	// cfg.Endpoints. findMatch short-circuits to "no match" for any
	// path in this set so a literal cannot be shadowed (and later
	// evicted) by an overlapping pattern. Read-only after construction.
	literals map[types.RoutePath]struct{}

	locker xsync.Mutex
	// cache is keyed by concrete RoutePath. Entries appear when a
	// pattern matches and are retained for the lifetime of the
	// resolver (mirroring literal-endpoint persistence — see Release).
	// Read/write only under locker.
	cache map[types.RoutePath]*resolvedEntry
}

// resolvedEntry is the cache entry for one concrete route path
// materialized from a regex pattern. Once guarantees exactly-once
// invocation of the wirer for that path; WireErr captures the outcome
// for waiters; Refcount is maintained for observability and is no
// longer used to drive eviction (see Release).
type resolvedEntry struct {
	Once    sync.Once
	WireErr error
	// Refcount is observability-only: every Acquire bumps it and
	// every Release decrements, but Release does NOT evict when the
	// count reaches zero. Kept on the struct because (a)
	// EntryRefcount() exposes it for tests asserting the
	// observability contract, and (b) it will be load-bearing again
	// once the explicit-eviction RPC lands.
	Refcount int
}

// NewEndpointResolver constructs a resolver that wires through w with
// the given patterns and literal-endpoint set. literals may be nil;
// patterns may be empty. The resolver does not retain a reference to
// the literals map (it copies the keys into an internal set), so the
// caller is free to mutate the original map afterwards.
func NewEndpointResolver(
	w endpointWirer,
	literals map[types.RoutePath]config.EndpointConfig,
	patterns []config.EndpointPattern,
) *EndpointResolver {
	litSet := make(map[types.RoutePath]struct{}, len(literals))
	for path := range literals {
		litSet[path] = struct{}{}
	}
	return &EndpointResolver{
		Patterns: patterns,
		wirer:    w,
		literals: litSet,
		cache:    map[types.RoutePath]*resolvedEntry{},
	}
}

// EnsureWired walks Patterns and, on the first match, materializes
// the endpoint at path. The returned matched=true with err==nil means
// the route is wired (this call or a previous one). matched=true with
// err!=nil means a pattern matched but rendering/unmarshaling/wiring
// failed; the failure is NOT cached so a subsequent call retries.
// matched=false means no pattern matched (or path is a literal
// endpoint already wired eagerly).
func (r *EndpointResolver) EnsureWired(
	ctx context.Context,
	path types.RoutePath,
) (matched bool, err error) {
	if r == nil {
		return false, nil
	}
	pat, captures, ok := r.findMatch(path)
	if !ok {
		return false, nil
	}
	entry := r.getOrCreateEntry(ctx, path)
	entry.Once.Do(func() {
		if wireErr := r.renderAndWire(ctx, path, pat, captures); wireErr != nil {
			entry.WireErr = wireErr
		}
	})
	if entry.WireErr != nil {
		// Failure path: drop the cache entry so next caller retries.
		r.dropFailedEntry(ctx, path, entry)
		return true, entry.WireErr
	}
	return true, nil
}

// Acquire ensures the path is wired (if it matches a pattern) and
// increments the refcount. Under the current Release contract pattern-
// materialized routes are persistent (Release never evicts), so once
// EnsureWired returns matched=true the cache entry is guaranteed to
// still exist when we take the lock to bump the refcount — there is
// no eviction race to handle here. If/when an explicit-eviction RPC
// lands this will need a re-acquire-on-evict loop again; add it
// together with the RPC, not preemptively.
func (r *EndpointResolver) Acquire(
	ctx context.Context,
	path types.RoutePath,
) error {
	if r == nil {
		return nil
	}
	matched, err := r.EnsureWired(ctx, path)
	if err != nil {
		return err
	}
	if !matched {
		return nil
	}
	r.locker.Do(ctx, func() {
		entry := r.cache[path]
		if entry == nil {
			// Unreachable under the current Release contract:
			// EnsureWired registered the entry and Release never
			// removes it. Crash loudly if the invariant breaks
			// (e.g. a future refactor re-introduces eviction
			// without updating this path).
			panic(fmt.Sprintf("endpoint resolver Acquire(%q): cache entry missing after EnsureWired (Release-contract invariant violated)", path))
		}
		entry.Refcount++
	})
	return nil
}

// Release decrements the refcount for path. The cache entry and the
// underlying route are intentionally retained even at refcount==0:
// pattern-materialized routes mirror literal-endpoint semantics so
// publisher disconnect/reconnect cycles reuse the same route node and
// the same persistent forwarders. Earlier versions evicted on
// disconnect, which raced the forwarder auto-restart and produced
// duplicate forwarders. Routes now persist for the resolver lifetime.
//
// Memory-growth caveat: configurations whose patterns capture a high-
// cardinality identifier (e.g. per-session-id paths) leak one cache
// entry + one route + one forwarder set per unique materialized path
// for the lifetime of the resolver. Explicit eviction is deferred
// (an EvictPatternRoute RPC). Current avd configs use codec×height-
// bounded patterns so the growth is bounded by config until then.
func (r *EndpointResolver) Release(
	ctx context.Context,
	path types.RoutePath,
) {
	if r == nil {
		return
	}
	r.locker.Do(ctx, func() {
		entry := r.cache[path]
		if entry == nil {
			return
		}
		if entry.Refcount > 0 {
			entry.Refcount--
		}
	})
}

// EntryRefcount reports the current refcount for path, or 0 if no
// cache entry exists. Exposed for tests that need to verify refcount
// behaviour without reaching into resolver internals.
func (r *EndpointResolver) EntryRefcount(
	ctx context.Context,
	path types.RoutePath,
) int {
	if r == nil {
		return 0
	}
	var n int
	r.locker.Do(ctx, func() {
		if e := r.cache[path]; e != nil {
			n = e.Refcount
		}
	})
	return n
}

// findMatch returns the first matching pattern (in declaration order)
// and the named-capture map. Paths declared in cfg.Endpoints
// (literals) are short-circuited to "no match" so a literal cannot
// be silently shadowed by a pattern. The returned map contains only
// captures with a non-empty name (unnamed groups are not exposed to
// the template).
func (r *EndpointResolver) findMatch(
	path types.RoutePath,
) (config.EndpointPattern, map[string]string, bool) {
	if _, isLiteral := r.literals[path]; isLiteral {
		return config.EndpointPattern{}, nil, false
	}
	pathStr := string(path)
	for _, pat := range r.Patterns {
		match := pat.Regexp.FindStringSubmatch(pathStr)
		if match == nil {
			continue
		}
		captures := map[string]string{}
		for i, name := range pat.Regexp.SubexpNames() {
			if name == "" {
				continue
			}
			captures[name] = match[i]
		}
		return pat, captures, true
	}
	return config.EndpointPattern{}, nil, false
}

// getOrCreateEntry returns the cache entry for path, creating a fresh
// one if none exists. Concurrent callers may receive the same entry;
// sync.Once gates the wiring side-effects. Takes the lock internally —
// no Locked suffix because the locking is private to the method.
func (r *EndpointResolver) getOrCreateEntry(
	ctx context.Context,
	path types.RoutePath,
) *resolvedEntry {
	var entry *resolvedEntry
	r.locker.Do(ctx, func() {
		entry = r.cache[path]
		if entry != nil {
			return
		}
		entry = &resolvedEntry{}
		r.cache[path] = entry
	})
	return entry
}

// dropFailedEntry removes entry from the cache iff it is still the
// entry registered at path. Concurrent retries may have already
// replaced it; in that case we leave the new entry alone.
func (r *EndpointResolver) dropFailedEntry(
	ctx context.Context,
	path types.RoutePath,
	entry *resolvedEntry,
) {
	r.locker.Do(ctx, func() {
		if r.cache[path] == entry {
			delete(r.cache, path)
		}
	})
}

// renderAndWire executes pat.Template with .Match=captures, unmarshals
// the rendered YAML as an EndpointConfig, and invokes the wirer. Any
// failure short-circuits and is reported back to the caller without
// caching the entry.
//
// The wirer is called with a Done-detached context (xcontext.DetachDone)
// so the wired forwarder's lifetime is decoupled from the originating
// request's context. EnsureWired is reachable from two ctx flavours:
//   - the consumer/publisher request handler ctx (lazy first-arrival),
//     which is cancelled on disconnect,
//   - the listener ctx (eager startup-time), which is the server-scoped
//     ctx and only cancels at server shutdown.
//
// In both flavours the wired route mirrors literal-endpoint semantics
// (persistent, see Release godoc): the forwarder must outlive any
// individual publisher/consumer disconnect. Without the detach, a regex-
// pattern forwarder created on a publisher's first arrival would inherit
// that publisher's ctx; on disconnect the watcher goroutine in
// avpipeline/router/route_forwarding.go takes the <-ctx.Done() branch
// and exits, while sync.Once short-circuits subsequent EnsureWired calls
// — a subsequent reconnect re-creates the route node but never re-wires
// the forwarder. Symptom: 64,586 "nowhere to push to a packet.Output"
// drops in production logs, only on regex-pattern endpoints (literal
// endpoints wired at startup with the server ctx survived).
//
// Belt log fields and tracing context still propagate through the
// detached ctx (xcontext.DetachDone preserves Value()) — only Done(),
// Deadline(), and Err() are detached.
//
// Dropping the deadline is intentional: persistent forwarders must not
// be torn down by a caller-imposed timeout — wiring is exactly-once via
// sync.Once and any deadline at the wire boundary would leak through
// to the forwarder's lifetime. Use RouteForwarding.Close() to stop a
// wired forwarder, never a parent-context deadline.
//
// xcontext.DetachDone is preferred over the equivalent context.WithoutCancel
// (Go 1.21+) because the xcontext dep is already present elsewhere in the
// module and the two have identical semantics for this use case.
func (r *EndpointResolver) renderAndWire(
	ctx context.Context,
	path types.RoutePath,
	pat config.EndpointPattern,
	captures map[string]string,
) error {
	var buf bytes.Buffer
	data := struct {
		Match map[string]string
	}{Match: captures}
	if err := pat.Template.Execute(&buf, data); err != nil {
		return fmt.Errorf("render endpoint template for %q (pattern %q): %w", path, pat.Raw, err)
	}
	logger.Tracef(ctx, "rendered endpoint body for path %q (pattern %q): %s", path, pat.Raw, buf.String())
	var ep config.EndpointConfig
	if err := yaml.Unmarshal(buf.Bytes(), &ep); err != nil {
		return fmt.Errorf("unmarshal rendered endpoint for %q (pattern %q): %w", path, pat.Raw, err)
	}
	if r.wirer == nil {
		return fmt.Errorf("endpoint resolver: wirer not set")
	}
	wireCtx := xcontext.DetachDone(ctx)
	if err := r.wirer.Wire(wireCtx, path, ep); err != nil {
		return fmt.Errorf("wire endpoint at %q (pattern %q): %w", path, pat.Raw, err)
	}
	return nil
}
