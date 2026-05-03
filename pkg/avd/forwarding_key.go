// forwarding_key.go provides the single canonical key type
// (ForwardingKey) used by every per-feature filter registry on the avd
// server. The four per-feature key types (PrivacyBlurFilterKey,
// DeblemishFilterKey, WhisperFilterKey, AVSyncFilterKey) were
// historically declared as four byte-identical structs that drifted
// independently; they are now type aliases of ForwardingKey so the
// fields, formatting, and comparability stay in lockstep.
//
// unregisterFiltersByRoutePath is the route-path-scoped purge used by
// every Unregister*FiltersByRoutePath wrapper to drop stale handles
// when the parent route disappears (OnRouteRemoved hook).

package avd

import (
	"fmt"

	"github.com/xaionaro-go/avpipeline/router"
)

// ForwardingKey identifies a forwarding's filter handle by the
// {RoutePath, ForwardingIndex} pair. avd's gRPC layer looks up the
// matching per-feature filter via this key.
type ForwardingKey struct {
	RoutePath       router.RoutePath
	ForwardingIndex int
}

func (k ForwardingKey) String() string {
	return fmt.Sprintf("route '%s' forwarding #%d", k.RoutePath, k.ForwardingIndex)
}
