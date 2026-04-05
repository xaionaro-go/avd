package config

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestForwardConfig_OnDemandDefaults(t *testing.T) {
	var cfg ForwardConfig
	require.NoError(t, yaml.Unmarshal([]byte("{}"), &cfg))
	assert.False(t, cfg.OnDemand, "on_demand must default to false")
	assert.Equal(t, uint(0), cfg.IdleTimeoutSec, "idle_timeout_sec must default to 0 (sentinel)")
}

func TestForwardConfig_EffectiveIdleTimeoutSec(t *testing.T) {
	t.Run("explicit", func(t *testing.T) {
		cfg := ForwardConfig{OnDemand: true, IdleTimeoutSec: 60}
		assert.Equal(t, uint(60), cfg.EffectiveIdleTimeoutSec())
	})
	t.Run("fallback", func(t *testing.T) {
		cfg := ForwardConfig{OnDemand: true}
		assert.Equal(t, uint(DefaultIdleTimeoutSec), cfg.EffectiveIdleTimeoutSec())
	})
}

func TestForwardConfig_OnDemandYAMLRoundTrip(t *testing.T) {
	cfg := ForwardConfig{
		OnDemand:       true,
		IdleTimeoutSec: 45,
	}
	data, err := yaml.Marshal(cfg)
	require.NoError(t, err)

	var got ForwardConfig
	require.NoError(t, yaml.Unmarshal(data, &got))
	assert.True(t, got.OnDemand)
	assert.Equal(t, uint(45), got.IdleTimeoutSec)
}

func TestForwardConfig_OnDemandOmittedWhenFalse(t *testing.T) {
	cfg := ForwardConfig{}
	data, err := yaml.Marshal(cfg)
	require.NoError(t, err)
	assert.NotContains(t, string(data), "on_demand",
		"on_demand should be omitted when false")
	assert.NotContains(t, string(data), "idle_timeout_sec",
		"idle_timeout_sec should be omitted when zero")
}

func TestForwardConfig_OnDemandYAMLParse(t *testing.T) {
	yamlData := `
destination:
  url: "rtmp://example.com/live/stream"
on_demand: true
idle_timeout_sec: 10
`
	var cfg ForwardConfig
	require.NoError(t, yaml.Unmarshal([]byte(yamlData), &cfg))
	assert.True(t, cfg.OnDemand)
	assert.Equal(t, uint(10), cfg.IdleTimeoutSec)
}

func TestForwardConfig_OnDemandInFullConfig(t *testing.T) {
	yamlData := `
endpoints:
  /low_res_preview:
    forwardings:
      - destination:
          local:
            route: "/preview_out"
        on_demand: true
        idle_timeout_sec: 30
`
	var buf bytes.Buffer
	buf.WriteString(yamlData)
	var cfg Config
	_, err := cfg.ReadFrom(&buf)
	require.NoError(t, err)

	ep, ok := cfg.Endpoints["/low_res_preview"]
	require.True(t, ok)
	require.Len(t, ep.Forwardings, 1)
	assert.True(t, ep.Forwardings[0].OnDemand)
	assert.Equal(t, uint(30), ep.Forwardings[0].IdleTimeoutSec)
}
