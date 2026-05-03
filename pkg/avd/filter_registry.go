// filter_registry.go provides a generic, mutex-guarded registry that
// backs the per-feature per-forwarding filter handle maps on *Server
// (privacy_blur, deblemish, whisper, av_sync).

package avd

import (
	"context"
	"fmt"

	"github.com/xaionaro-go/avpipeline/router"
	"github.com/xaionaro-go/xsync"
)

// filterRegistry is a generic, mutex-guarded map keyed by per-forwarding
// filter identifiers. featureName is embedded in the registry-miss
// error message so operators can grep server logs by feature.
type filterRegistry[K comparable, V any] struct {
	featureName string
	mu          xsync.Mutex
	filters     map[K]V
}

func newFilterRegistry[K comparable, V any](
	featureName string,
) filterRegistry[K, V] {
	return filterRegistry[K, V]{
		featureName: featureName,
		filters:     make(map[K]V),
	}
}

// Register stores filter under key. A repeat registration with the
// same key overwrites the prior entry. Per-feature *Server wrappers
// are responsible for any nil-value rejection (the generic registry
// does not constrain V to a nilable type).
func (r *filterRegistry[K, V]) Register(
	ctx context.Context,
	key K,
	filter V,
) {
	r.mu.Do(ctx, func() {
		r.filters[key] = filter
	})
}

// Get returns the filter registered under key, or an error naming the
// feature and the key when no entry exists.
func (r *filterRegistry[K, V]) Get(
	ctx context.Context,
	key K,
) (V, error) {
	var v V
	var err error
	r.mu.Do(ctx, func() {
		var ok bool
		v, ok = r.filters[key]
		if !ok {
			err = fmt.Errorf("no %s filter for key %v", r.featureName, key)
		}
	})
	return v, err
}

// Unregister removes the entry under key. A miss is silent: callers
// that iterate GetRegisteredKeys + Unregister cannot easily avoid
// races, and a miss-after-iteration is benign.
func (r *filterRegistry[K, V]) Unregister(
	ctx context.Context,
	key K,
) {
	r.mu.Do(ctx, func() {
		delete(r.filters, key)
	})
}

// GetRegisteredKeys returns a snapshot of the currently-registered
// keys. Order is unspecified.
func (r *filterRegistry[K, V]) GetRegisteredKeys(
	ctx context.Context,
) []K {
	var keys []K
	r.mu.Do(ctx, func() {
		for k := range r.filters {
			keys = append(keys, k)
		}
	})
	return keys
}

// UnregisterByPredicate removes every entry whose key matches pred and
// returns the count of removed entries. The walk and the deletes happen
// under a single lock so a concurrent Register cannot race with the
// iteration (the previous per-feature wrappers built a snapshot via
// GetRegisteredKeys then called Unregister per key, leaving a small
// window where a re-registration under the same key could be deleted
// after the snapshot decision; a single locked walk closes that window).
//
// Used by unregisterFiltersByRoutePath (and any future bulk-purge
// helper) to drop filter handles in one locked walk.
func (r *filterRegistry[K, V]) UnregisterByPredicate(
	ctx context.Context,
	pred func(K) bool,
) int {
	var removed int
	r.mu.Do(ctx, func() {
		for k := range r.filters {
			if pred(k) {
				delete(r.filters, k)
				removed++
			}
		}
	})
	return removed
}

// unregisterFiltersByRoutePath removes every entry whose key's
// RoutePath matches path and returns the count of removed entries.
// Free function (instead of a method on *filterRegistry) because Go
// generics cannot express "K constrained to ForwardingKey" on a method
// of an already-parameterised receiver, but the free form composes
// cleanly via type inference at the call site.
//
// Every per-feature UnregisterXxxFiltersByRoutePath wrapper now
// dispatches here; ForwardingKey aliases (PrivacyBlur/Deblemish/
// Whisper/AVSyncFilterKey) collapse to one canonical type so the
// extractor stays simple.
func unregisterFiltersByRoutePath[V any](
	ctx context.Context,
	r *filterRegistry[ForwardingKey, V],
	path router.RoutePath,
) int {
	return r.UnregisterByPredicate(ctx, func(k ForwardingKey) bool {
		return k.RoutePath == path
	})
}
