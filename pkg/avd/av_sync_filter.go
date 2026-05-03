package avd

import (
	"context"
	"time"

	"github.com/asticode/go-astiav"
	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/router"
)

// AVSyncFilterKey is the canonical ForwardingKey used to address a
// forwarding's AVSync filter handle. avd's gRPC layer looks up the
// matching *kernel.AVSync via this key to drive
// GetDelta / SetOffset / AutoTune. Kept as an alias (instead of a
// distinct struct) so all four feature key types stay in lockstep —
// see forwarding_key.go.
type AVSyncFilterKey = ForwardingKey

// RegisterAVSyncFilter stores the given AVSync under the given key.
// nil filters are ignored. A repeat registration with the same key
// overwrites the prior entry.
func (s *Server) RegisterAVSyncFilter(
	ctx context.Context,
	key AVSyncFilterKey,
	filter *kernel.AVSync,
) {
	if filter == nil {
		return
	}
	s.AVSyncFilters.Register(ctx, key, filter)
}

// GetAVSyncFilter returns the registered AVSync for the given key, or
// an error describing the missing key.
func (s *Server) GetAVSyncFilter(
	ctx context.Context,
	key AVSyncFilterKey,
) (*kernel.AVSync, error) {
	return s.AVSyncFilters.Get(ctx, key)
}

// GetRegisteredAVSyncKeys returns the keys currently registered. The
// order is unspecified.
func (s *Server) GetRegisteredAVSyncKeys(
	ctx context.Context,
) []AVSyncFilterKey {
	return s.AVSyncFilters.GetRegisteredKeys(ctx)
}

// SetPTSShift writes the per-mediatype PTS+DTS offset on the AVSync
// registered under key. Returns the registry-miss error from
// GetAVSyncFilter when the key is unknown, and the kernel.AVSync's
// own error for unsupported media types.
func (s *Server) SetPTSShift(
	ctx context.Context,
	key AVSyncFilterKey,
	mediaType astiav.MediaType,
	shift time.Duration,
) error {
	filter, err := s.GetAVSyncFilter(ctx, key)
	if err != nil {
		return err
	}
	return filter.SetOffset(ctx, mediaType, shift)
}

// GetPTSShift reads the per-mediatype PTS+DTS offset from the AVSync
// registered under key.
func (s *Server) GetPTSShift(
	ctx context.Context,
	key AVSyncFilterKey,
	mediaType astiav.MediaType,
) (time.Duration, error) {
	filter, err := s.GetAVSyncFilter(ctx, key)
	if err != nil {
		return 0, err
	}
	return filter.GetOffset(ctx, mediaType)
}

// GetAVSyncDelta reads the audio-minus-video PTS delta and the
// observed flag from the AVSync registered under key. Returns the
// registry-miss error when the key is unknown.
func (s *Server) GetAVSyncDelta(
	ctx context.Context,
	key AVSyncFilterKey,
) (time.Duration, bool, error) {
	filter, err := s.GetAVSyncFilter(ctx, key)
	if err != nil {
		return 0, false, err
	}
	d, ok := filter.GetDelta(ctx)
	return d, ok, nil
}

// AutoTuneAVSync invokes one-shot auto-tune on the AVSync registered
// under key. Surfaces kernel sentinels (ErrAVSyncNotObserved /
// ErrAVSyncVideoLeadsAudio) as-is so callers can map them to
// transport-specific status codes.
func (s *Server) AutoTuneAVSync(
	ctx context.Context,
	key AVSyncFilterKey,
) (time.Duration, error) {
	filter, err := s.GetAVSyncFilter(ctx, key)
	if err != nil {
		return 0, err
	}
	return filter.AutoTune(ctx)
}

// UnregisterAVSyncFiltersByRoutePath removes every AVSync filter
// registered against the given route path. Called by OnRouteRemoved
// to drop stale per-forwarding handles when their parent route
// disappears.
func (s *Server) UnregisterAVSyncFiltersByRoutePath(
	ctx context.Context,
	path router.RoutePath,
) error {
	if removed := unregisterFiltersByRoutePath(ctx, &s.AVSyncFilters, path); removed > 0 {
		logger.Tracef(ctx, "UnregisterAVSyncFiltersByRoutePath('%s'): removed %d entries", path, removed)
	}
	return nil
}
