//go:build !with_cv
// +build !with_cv

package configapplier

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xaionaro-go/avd/pkg/config"
)

func TestNewPrivacyBlurFactory_NilConfig(t *testing.T) {
	factory, filter := newPrivacyBlurFactory(nil)
	assert.Nil(t, factory, "nil config should produce nil factory")
	assert.Nil(t, filter, "nil config should produce nil filter")
}

func TestNewPrivacyBlurFactory_DisabledConfig(t *testing.T) {
	cfg := &config.PrivacyBlurConfig{Enabled: false}
	factory, filter := newPrivacyBlurFactory(cfg)
	// Passthrough mode: factory and filter are created so avcli can enable at runtime.
	require.NotNil(t, factory, "disabled config should still produce a factory for runtime enable")
	require.NotNil(t, filter, "disabled config should still produce a filter for runtime enable")
	assert.False(t, filter.Enabled.Load(), "Enabled should be false")

	// The factory should return an error when called (no CV support).
	_, err := factory(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "with_cv")
}

func TestNewPrivacyBlurFactory_EnabledWithoutCV(t *testing.T) {
	cfg := &config.PrivacyBlurConfig{
		Enabled: true,
		Faces:   true,
	}
	factory, filter := newPrivacyBlurFactory(cfg)
	require.NotNil(t, factory, "enabled config should produce a factory even without with_cv")
	require.NotNil(t, filter, "enabled config should produce a filter")
	assert.True(t, filter.Enabled.Load(), "Enabled should be true")

	// The factory should return an error when called (no CV support).
	_, err := factory(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "with_cv")
}
