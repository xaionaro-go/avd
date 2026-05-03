package configapplier

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xaionaro-go/avd/pkg/config"
)

func TestNewDeblemishFactory_NilConfig(t *testing.T) {
	factory, filter := newDeblemishFactory(nil)
	assert.Nil(t, factory, "nil config should produce nil factory")
	assert.Nil(t, filter, "nil config should produce nil filter")
}

func TestNewDeblemishFactory_DisabledPassthrough(t *testing.T) {
	cfg := &config.DeblemishConfig{Enabled: false, SigmaS: 10.0, SigmaR: 0.1}
	factory, filter := newDeblemishFactory(cfg)
	// Passthrough: factory and filter created so avcli can enable at runtime.
	require.NotNil(t, factory, "disabled config should still produce a factory for runtime enable")
	require.NotNil(t, filter, "disabled config should still produce a filter for runtime enable")
	assert.False(t, filter.Enabled.Load(), "Enabled should be false in passthrough mode")
	assert.Equal(t, 10.0, filter.GetSigmaS(), "SigmaS should be set from config")
	assert.Equal(t, 0.1, filter.GetSigmaR(), "SigmaR should be set from config")
}

func TestNewDeblemishFactory_Enabled(t *testing.T) {
	cfg := &config.DeblemishConfig{
		Enabled:  true,
		SigmaS:   15.0,
		SigmaR:   0.2,
		Diameter: 5,
	}
	factory, filter := newDeblemishFactory(cfg)
	require.NotNil(t, factory, "enabled config should produce a factory")
	require.NotNil(t, filter, "enabled config should produce a filter")
	assert.True(t, filter.Enabled.Load(), "Enabled should be true")
	assert.Equal(t, 15.0, filter.GetSigmaS())
	assert.Equal(t, 0.2, filter.GetSigmaR())
	assert.Equal(t, int64(5), filter.Diameter.Load())
}
