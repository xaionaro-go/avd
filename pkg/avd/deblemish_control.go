package avd

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"

	"github.com/xaionaro-go/avpipeline/kernel/deblemish"
	"github.com/xaionaro-go/avpipeline/router"
	"github.com/xaionaro-go/xsync"
)

// DeblemishControlKey identifies a forwarding's deblemish control.
type DeblemishControlKey struct {
	RoutePath       router.RoutePath
	ForwardingIndex int
}

// DeblemishControl holds the runtime-adjustable parameters for a deblemish kernel.
// All fields are safe for concurrent access. When a live kernel exists,
// parameter changes propagate immediately via the kernel's own atomic fields.
type DeblemishControl struct {
	Enabled  atomic.Bool
	SigmaS   atomic.Uint64 // stores float64 bits via math.Float64bits/Float64frombits
	SigmaR   atomic.Uint64
	Diameter atomic.Int64

	kernelMu xsync.Mutex
	kernel   *deblemish.Deblemish
}

func (c *DeblemishControl) SetSigmaS(
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

func (c *DeblemishControl) GetSigmaS() float64 {
	return math.Float64frombits(c.SigmaS.Load())
}

func (c *DeblemishControl) SetSigmaR(
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

func (c *DeblemishControl) GetSigmaR() float64 {
	return math.Float64frombits(c.SigmaR.Load())
}

func (c *DeblemishControl) SetDiameter(
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

func (c *DeblemishControl) SetEnabled(v bool) {
	c.Enabled.Store(v)
}

// SetKernel registers the live kernel so that parameter changes
// propagate to it. Called by the factory each time a kernel is created.
func (c *DeblemishControl) SetKernel(
	ctx context.Context,
	k *deblemish.Deblemish,
) {
	c.kernelMu.Do(ctx, func() {
		c.kernel = k
	})
}

type deblemishRegistry struct {
	Locker   xsync.Mutex
	Controls map[DeblemishControlKey]*DeblemishControl
}

func (s *Server) RegisterDeblemishControl(
	ctx context.Context,
	key DeblemishControlKey,
	control *DeblemishControl,
) {
	if control == nil {
		return
	}
	s.DeblemishRegistry.Locker.Do(ctx, func() {
		s.DeblemishRegistry.Controls[key] = control
	})
}

func (s *Server) GetDeblemishControl(
	ctx context.Context,
	key DeblemishControlKey,
) (*DeblemishControl, error) {
	var ctrl *DeblemishControl
	var err error
	s.DeblemishRegistry.Locker.Do(ctx, func() {
		var ok bool
		ctrl, ok = s.DeblemishRegistry.Controls[key]
		if !ok {
			err = fmt.Errorf("no deblemish control for route '%s' forwarding #%d", key.RoutePath, key.ForwardingIndex)
		}
	})
	return ctrl, err
}

func (s *Server) SetDeblemishState(
	ctx context.Context,
	key DeblemishControlKey,
	enabled *bool,
	sigmaS *float64,
	sigmaR *float64,
	diameter *int64,
) error {
	ctrl, err := s.GetDeblemishControl(ctx, key)
	if err != nil {
		return err
	}
	if enabled != nil {
		ctrl.SetEnabled(*enabled)
	}
	if sigmaS != nil {
		ctrl.SetSigmaS(ctx, *sigmaS)
	}
	if sigmaR != nil {
		ctrl.SetSigmaR(ctx, *sigmaR)
	}
	if diameter != nil {
		ctrl.SetDiameter(ctx, *diameter)
	}
	return nil
}

func (s *Server) GetDeblemishState(
	ctx context.Context,
	key DeblemishControlKey,
) (enabled bool, sigmaS float64, sigmaR float64, diameter int64, err error) {
	ctrl, err := s.GetDeblemishControl(ctx, key)
	if err != nil {
		return false, 0, 0, 0, err
	}
	return ctrl.Enabled.Load(), ctrl.GetSigmaS(), ctrl.GetSigmaR(), ctrl.Diameter.Load(), nil
}

func (s *Server) GetRegisteredDeblemishKeys(
	ctx context.Context,
) []DeblemishControlKey {
	var keys []DeblemishControlKey
	s.DeblemishRegistry.Locker.Do(ctx, func() {
		for k := range s.DeblemishRegistry.Controls {
			keys = append(keys, k)
		}
	})
	return keys
}
