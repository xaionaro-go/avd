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

	var dup Config
	_, err = dup.ReadFrom(&b)
	require.NoError(t, err)

	require.Equal(t, cfg, dup)
}
