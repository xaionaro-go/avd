package avd

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/xaionaro-go/avpipeline/router"
	"github.com/xaionaro-go/xsync"
)

// WhisperControlKey identifies a forwarding's whisper control.
type WhisperControlKey struct {
	RoutePath       router.RoutePath
	ForwardingIndex int
}

// WhisperControl holds the runtime-adjustable parameters for a whisper kernel.
// All fields are safe for concurrent access.
type WhisperControl struct {
	Enabled  atomic.Bool
	Language atomic.Value // stores string
	Model    atomic.Value // stores string
}

func (c *WhisperControl) SetLanguage(v string) {
	c.Language.Store(v)
}

func (c *WhisperControl) GetLanguage() string {
	v, _ := c.Language.Load().(string)
	return v
}

func (c *WhisperControl) SetModel(v string) {
	c.Model.Store(v)
}

func (c *WhisperControl) GetModel() string {
	v, _ := c.Model.Load().(string)
	return v
}

type whisperRegistry struct {
	Locker   xsync.Mutex
	Controls map[WhisperControlKey]*WhisperControl
}

func (s *Server) RegisterWhisperControl(
	ctx context.Context,
	key WhisperControlKey,
	control *WhisperControl,
) {
	if control == nil {
		return
	}
	s.WhisperRegistry.Locker.Do(ctx, func() {
		s.WhisperRegistry.Controls[key] = control
	})
}

func (s *Server) GetWhisperControl(
	ctx context.Context,
	key WhisperControlKey,
) (*WhisperControl, error) {
	var ctrl *WhisperControl
	var err error
	s.WhisperRegistry.Locker.Do(ctx, func() {
		var ok bool
		ctrl, ok = s.WhisperRegistry.Controls[key]
		if !ok {
			err = fmt.Errorf("no whisper control for route '%s' forwarding #%d", key.RoutePath, key.ForwardingIndex)
		}
	})
	return ctrl, err
}

func (s *Server) SetWhisperState(
	ctx context.Context,
	key WhisperControlKey,
	enabled *bool,
	language *string,
	model *string,
) error {
	ctrl, err := s.GetWhisperControl(ctx, key)
	if err != nil {
		return err
	}
	if enabled != nil {
		ctrl.Enabled.Store(*enabled)
	}
	if language != nil {
		ctrl.SetLanguage(*language)
	}
	if model != nil {
		ctrl.SetModel(*model)
	}
	return nil
}

func (s *Server) GetWhisperState(
	ctx context.Context,
	key WhisperControlKey,
) (enabled bool, language string, model string, err error) {
	ctrl, err := s.GetWhisperControl(ctx, key)
	if err != nil {
		return false, "", "", err
	}
	return ctrl.Enabled.Load(), ctrl.GetLanguage(), ctrl.GetModel(), nil
}

func (s *Server) GetRegisteredWhisperKeys(
	ctx context.Context,
) []WhisperControlKey {
	var keys []WhisperControlKey
	s.WhisperRegistry.Locker.Do(ctx, func() {
		for k := range s.WhisperRegistry.Controls {
			keys = append(keys, k)
		}
	})
	return keys
}
