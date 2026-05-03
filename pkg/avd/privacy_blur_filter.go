package avd

import (
	"context"
	"math"
	"sync/atomic"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avpipeline/router"
)

// PrivacyBlurFilterKey is the canonical ForwardingKey used to address a
// forwarding's privacy blur filter handle. Kept as an alias (instead of
// a distinct struct) so all four feature key types stay in lockstep —
// see forwarding_key.go.
type PrivacyBlurFilterKey = ForwardingKey

// PrivacyBlurFilter holds the runtime-adjustable parameters for a privacy blur kernel.
// All fields are safe for concurrent access.
type PrivacyBlurFilter struct {
	Enabled           atomic.Bool
	BlurRadius        atomic.Uint64 // stores float64 bits via math.Float64bits/Float64frombits
	PixelateBlockSize atomic.Int64
}

func (c *PrivacyBlurFilter) SetBlurRadius(v float64) {
	c.BlurRadius.Store(math.Float64bits(v))
}

func (c *PrivacyBlurFilter) GetBlurRadius() float64 {
	return math.Float64frombits(c.BlurRadius.Load())
}

// RegisterPrivacyBlurFilter stores filter under key. nil filters are ignored.
// A repeat registration with the same key overwrites the prior entry.
func (s *Server) RegisterPrivacyBlurFilter(
	ctx context.Context,
	key PrivacyBlurFilterKey,
	filter *PrivacyBlurFilter,
) {
	if filter == nil {
		return
	}
	s.PrivacyBlurFilters.Register(ctx, key, filter)
}

// GetPrivacyBlurFilter returns the registered filter for key, or an error
// describing the missing key.
func (s *Server) GetPrivacyBlurFilter(
	ctx context.Context,
	key PrivacyBlurFilterKey,
) (*PrivacyBlurFilter, error) {
	return s.PrivacyBlurFilters.Get(ctx, key)
}

func (s *Server) SetPrivacyBlurState(
	ctx context.Context,
	key PrivacyBlurFilterKey,
	enabled *bool,
	blurRadius *float64,
	pixelateBlockSize *int64,
) error {
	filter, err := s.GetPrivacyBlurFilter(ctx, key)
	if err != nil {
		return err
	}
	if enabled != nil {
		filter.Enabled.Store(*enabled)
	}
	if blurRadius != nil {
		filter.SetBlurRadius(*blurRadius)
	}
	if pixelateBlockSize != nil {
		filter.PixelateBlockSize.Store(*pixelateBlockSize)
	}
	return nil
}

func (s *Server) GetPrivacyBlurState(
	ctx context.Context,
	key PrivacyBlurFilterKey,
) (enabled bool, blurRadius float64, pixelateBlockSize int64, err error) {
	filter, err := s.GetPrivacyBlurFilter(ctx, key)
	if err != nil {
		return false, 0, 0, err
	}
	return filter.Enabled.Load(), filter.GetBlurRadius(), filter.PixelateBlockSize.Load(), nil
}

func (s *Server) GetRegisteredPrivacyBlurKeys(
	ctx context.Context,
) []PrivacyBlurFilterKey {
	return s.PrivacyBlurFilters.GetRegisteredKeys(ctx)
}

// UnregisterPrivacyBlurFiltersByRoutePath removes every privacy blur
// filter registered against the given route path. Called by
// OnRouteRemoved to drop stale per-forwarding handles when their
// parent route disappears.
func (s *Server) UnregisterPrivacyBlurFiltersByRoutePath(
	ctx context.Context,
	path router.RoutePath,
) error {
	if removed := unregisterFiltersByRoutePath(ctx, &s.PrivacyBlurFilters, path); removed > 0 {
		logger.Tracef(ctx, "UnregisterPrivacyBlurFiltersByRoutePath('%s'): removed %d entries", path, removed)
	}
	return nil
}
