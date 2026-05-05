// config_test.go provides tests for the configuration.

package config

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xaionaro-go/avd/pkg/avd/types"
)

func TestConfigWriteRead(t *testing.T) {
	cfg := Default()

	var b bytes.Buffer
	_, err := cfg.WriteTo(&b)
	require.NoError(t, err)
	yamlOrig := b.String()

	var dup Config
	_, err = dup.ReadFrom(&b)
	require.NoError(t, err)

	var b2 bytes.Buffer
	_, err = dup.WriteTo(&b2)
	require.NoError(t, err)
	yamlDup := b2.String()

	require.YAMLEq(t, yamlOrig, yamlDup)
}

func TestConfigReadCommandRestartDefaultsToNever(t *testing.T) {
	var cfg Config

	_, err := cfg.Read([]byte(`
endpoints:
  cam/audio:
    on_publisher_removed:
      command: ["true"]
`))
	require.NoError(t, err)

	endpoint := cfg.Endpoints[types.RoutePath("cam/audio")]
	require.NotNil(t, endpoint.OnPublisherRemoved)
	require.Equal(t, RestartPolicyNever, endpoint.OnPublisherRemoved.Restart)
}

func TestCommandZeroValueRestartIsNever(t *testing.T) {
	var cmd Command

	require.Equal(t, RestartPolicyNever, cmd.Restart)
	require.Equal(t, "never", cmd.Restart.String())
}

func TestConfigReadCommandRestartExplicitAlways(t *testing.T) {
	var cfg Config

	_, err := cfg.Read([]byte(`
endpoints:
  cam/audio:
    on_publisher_removed:
      command: ["true"]
      restart: always
`))
	require.NoError(t, err)

	endpoint := cfg.Endpoints[types.RoutePath("cam/audio")]
	require.NotNil(t, endpoint.OnPublisherRemoved)
	require.Equal(t, RestartPolicyAlways, endpoint.OnPublisherRemoved.Restart)
}
