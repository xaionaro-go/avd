// config_test.go provides tests for the configuration.

package config

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
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
