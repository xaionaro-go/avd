package avd

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"

	"github.com/xaionaro-go/avpipeline/router"
	"github.com/xaionaro-go/xsync"
)

// PrivacyBlurControlKey identifies a forwarding's privacy blur control.
type PrivacyBlurControlKey struct {
	RoutePath       router.RoutePath
	ForwardingIndex int
}

// PrivacyBlurControl holds the runtime-adjustable parameters for a privacy blur kernel.
// All fields are safe for concurrent access.
type PrivacyBlurControl struct {
	Enabled           atomic.Bool
	BlurRadius        atomic.Uint64 // stores float64 bits via math.Float64bits/Float64frombits
	PixelateBlockSize atomic.Int64
}

func (c *PrivacyBlurControl) SetBlurRadius(v float64) {
	c.BlurRadius.Store(math.Float64bits(v))
}

func (c *PrivacyBlurControl) GetBlurRadius() float64 {
	return math.Float64frombits(c.BlurRadius.Load())
}

type privacyBlurRegistry struct {
	Locker   xsync.Mutex
	Controls map[PrivacyBlurControlKey]*PrivacyBlurControl
}

func (s *Server) RegisterPrivacyBlurControl(
	ctx context.Context,
	key PrivacyBlurControlKey,
	control *PrivacyBlurControl,
) {
	if control == nil {
		return
	}
	s.PrivacyBlurRegistry.Locker.Do(ctx, func() {
		s.PrivacyBlurRegistry.Controls[key] = control
	})
}

func (s *Server) GetPrivacyBlurControl(
	ctx context.Context,
	key PrivacyBlurControlKey,
) (*PrivacyBlurControl, error) {
	var ctrl *PrivacyBlurControl
	var err error
	s.PrivacyBlurRegistry.Locker.Do(ctx, func() {
		var ok bool
		ctrl, ok = s.PrivacyBlurRegistry.Controls[key]
		if !ok {
			err = fmt.Errorf("no privacy blur control for route '%s' forwarding #%d", key.RoutePath, key.ForwardingIndex)
		}
	})
	return ctrl, err
}

func (s *Server) SetPrivacyBlurState(
	ctx context.Context,
	key PrivacyBlurControlKey,
	enabled *bool,
	blurRadius *float64,
	pixelateBlockSize *int64,
) error {
	ctrl, err := s.GetPrivacyBlurControl(ctx, key)
	if err != nil {
		return err
	}
	if enabled != nil {
		ctrl.Enabled.Store(*enabled)
	}
	if blurRadius != nil {
		ctrl.SetBlurRadius(*blurRadius)
	}
	if pixelateBlockSize != nil {
		ctrl.PixelateBlockSize.Store(*pixelateBlockSize)
	}
	return nil
}

func (s *Server) GetPrivacyBlurState(
	ctx context.Context,
	key PrivacyBlurControlKey,
) (enabled bool, blurRadius float64, pixelateBlockSize int64, err error) {
	ctrl, err := s.GetPrivacyBlurControl(ctx, key)
	if err != nil {
		return false, 0, 0, err
	}
	return ctrl.Enabled.Load(), ctrl.GetBlurRadius(), ctrl.PixelateBlockSize.Load(), nil
}
