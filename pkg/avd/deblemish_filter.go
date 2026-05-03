package avd

import (
	"context"
	"math"
	"sync/atomic"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avpipeline/kernel/deblemish"
	"github.com/xaionaro-go/avpipeline/router"
	"github.com/xaionaro-go/xsync"
)

// DeblemishFilterKey is the canonical ForwardingKey used to address a
// forwarding's deblemish filter handle. Kept as an alias (instead of a
// distinct struct) so all four feature key types stay in lockstep —
// see forwarding_key.go.
type DeblemishFilterKey = ForwardingKey

// DeblemishFilter holds the runtime-adjustable parameters for a deblemish kernel.
// All fields are safe for concurrent access. When a live kernel exists,
// parameter changes propagate immediately via the kernel's own atomic fields.
type DeblemishFilter struct {
	Enabled  atomic.Bool
	SigmaS   atomic.Uint64 // stores float64 bits via math.Float64bits/Float64frombits
	SigmaR   atomic.Uint64
	Diameter atomic.Int64

	kernelMu xsync.Mutex
	kernel   *deblemish.Deblemish
}

func (c *DeblemishFilter) SetSigmaS(
	ctx context.Context,
	v float64,
) {
	c.SigmaS.Store(math.Float64bits(v))
	c.kernelMu.Do(ctx, func() {
		if c.kernel != nil {
			c.kernel.SigmaS.Store(v)
		}
	})
}

func (c *DeblemishFilter) GetSigmaS() float64 {
	return math.Float64frombits(c.SigmaS.Load())
}

func (c *DeblemishFilter) SetSigmaR(
	ctx context.Context,
	v float64,
) {
	c.SigmaR.Store(math.Float64bits(v))
	c.kernelMu.Do(ctx, func() {
		if c.kernel != nil {
			c.kernel.SigmaR.Store(v)
		}
	})
}

func (c *DeblemishFilter) GetSigmaR() float64 {
	return math.Float64frombits(c.SigmaR.Load())
}

func (c *DeblemishFilter) SetDiameter(
	ctx context.Context,
	v int64,
) {
	c.Diameter.Store(v)
	c.kernelMu.Do(ctx, func() {
		if c.kernel != nil {
			c.kernel.Diameter.Store(v)
		}
	})
}

func (c *DeblemishFilter) SetEnabled(v bool) {
	c.Enabled.Store(v)
}

// SetKernel registers the live kernel so that parameter changes
// propagate to it. Called by the factory each time a kernel is created.
func (c *DeblemishFilter) SetKernel(
	ctx context.Context,
	k *deblemish.Deblemish,
) {
	c.kernelMu.Do(ctx, func() {
		c.kernel = k
	})
}

// RegisterDeblemishFilter stores filter under key. nil filters are ignored.
// A repeat registration with the same key overwrites the prior entry.
func (s *Server) RegisterDeblemishFilter(
	ctx context.Context,
	key DeblemishFilterKey,
	filter *DeblemishFilter,
) {
	if filter == nil {
		return
	}
	s.DeblemishFilters.Register(ctx, key, filter)
}

// GetDeblemishFilter returns the registered filter for key, or an error
// describing the missing key.
func (s *Server) GetDeblemishFilter(
	ctx context.Context,
	key DeblemishFilterKey,
) (*DeblemishFilter, error) {
	return s.DeblemishFilters.Get(ctx, key)
}

func (s *Server) SetDeblemishState(
	ctx context.Context,
	key DeblemishFilterKey,
	enabled *bool,
	sigmaS *float64,
	sigmaR *float64,
	diameter *int64,
) error {
	filter, err := s.GetDeblemishFilter(ctx, key)
	if err != nil {
		return err
	}
	if enabled != nil {
		filter.SetEnabled(*enabled)
	}
	if sigmaS != nil {
		filter.SetSigmaS(ctx, *sigmaS)
	}
	if sigmaR != nil {
		filter.SetSigmaR(ctx, *sigmaR)
	}
	if diameter != nil {
		filter.SetDiameter(ctx, *diameter)
	}
	return nil
}

func (s *Server) GetDeblemishState(
	ctx context.Context,
	key DeblemishFilterKey,
) (enabled bool, sigmaS float64, sigmaR float64, diameter int64, err error) {
	filter, err := s.GetDeblemishFilter(ctx, key)
	if err != nil {
		return false, 0, 0, 0, err
	}
	return filter.Enabled.Load(), filter.GetSigmaS(), filter.GetSigmaR(), filter.Diameter.Load(), nil
}

func (s *Server) GetRegisteredDeblemishKeys(
	ctx context.Context,
) []DeblemishFilterKey {
	return s.DeblemishFilters.GetRegisteredKeys(ctx)
}

// UnregisterDeblemishFiltersByRoutePath removes every deblemish
// filter registered against the given route path. Called by
// OnRouteRemoved to drop stale per-forwarding handles when their
// parent route disappears.
func (s *Server) UnregisterDeblemishFiltersByRoutePath(
	ctx context.Context,
	path router.RoutePath,
) error {
	if removed := unregisterFiltersByRoutePath(ctx, &s.DeblemishFilters, path); removed > 0 {
		logger.Tracef(ctx, "UnregisterDeblemishFiltersByRoutePath('%s'): removed %d entries", path, removed)
	}
	return nil
}
