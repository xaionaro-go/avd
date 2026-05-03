package avd

import (
	"context"
	"sync/atomic"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avpipeline/router"
)

// WhisperFilterKey is the canonical ForwardingKey used to address a
// forwarding's whisper filter handle. Kept as an alias (instead of a
// distinct struct) so all four feature key types stay in lockstep —
// see forwarding_key.go.
type WhisperFilterKey = ForwardingKey

// WhisperFilter holds the runtime-adjustable parameters for a whisper kernel.
// All fields are safe for concurrent access.
type WhisperFilter struct {
	Enabled  atomic.Bool
	Language atomic.Value // stores string
	Model    atomic.Value // stores string
}

func (c *WhisperFilter) SetLanguage(v string) {
	c.Language.Store(v)
}

func (c *WhisperFilter) GetLanguage() string {
	v, _ := c.Language.Load().(string)
	return v
}

func (c *WhisperFilter) SetModel(v string) {
	c.Model.Store(v)
}

func (c *WhisperFilter) GetModel() string {
	v, _ := c.Model.Load().(string)
	return v
}

// RegisterWhisperFilter stores filter under key. nil filters are ignored.
// A repeat registration with the same key overwrites the prior entry.
func (s *Server) RegisterWhisperFilter(
	ctx context.Context,
	key WhisperFilterKey,
	filter *WhisperFilter,
) {
	if filter == nil {
		return
	}
	s.WhisperFilters.Register(ctx, key, filter)
}

// GetWhisperFilter returns the registered filter for key, or an error
// describing the missing key.
func (s *Server) GetWhisperFilter(
	ctx context.Context,
	key WhisperFilterKey,
) (*WhisperFilter, error) {
	return s.WhisperFilters.Get(ctx, key)
}

func (s *Server) SetWhisperState(
	ctx context.Context,
	key WhisperFilterKey,
	enabled *bool,
	language *string,
	model *string,
) error {
	filter, err := s.GetWhisperFilter(ctx, key)
	if err != nil {
		return err
	}
	if enabled != nil {
		filter.Enabled.Store(*enabled)
	}
	if language != nil {
		filter.SetLanguage(*language)
	}
	if model != nil {
		filter.SetModel(*model)
	}
	return nil
}

func (s *Server) GetWhisperState(
	ctx context.Context,
	key WhisperFilterKey,
) (enabled bool, language string, model string, err error) {
	filter, err := s.GetWhisperFilter(ctx, key)
	if err != nil {
		return false, "", "", err
	}
	return filter.Enabled.Load(), filter.GetLanguage(), filter.GetModel(), nil
}

func (s *Server) GetRegisteredWhisperKeys(
	ctx context.Context,
) []WhisperFilterKey {
	return s.WhisperFilters.GetRegisteredKeys(ctx)
}

// UnregisterWhisperFiltersByRoutePath removes every whisper filter
// registered against the given route path. Called by OnRouteRemoved
// to drop stale per-forwarding handles when their parent route
// disappears (e.g., config reload removing the route).
func (s *Server) UnregisterWhisperFiltersByRoutePath(
	ctx context.Context,
	path router.RoutePath,
) error {
	if removed := unregisterFiltersByRoutePath(ctx, &s.WhisperFilters, path); removed > 0 {
		logger.Tracef(ctx, "UnregisterWhisperFiltersByRoutePath('%s'): removed %d entries", path, removed)
	}
	return nil
}
