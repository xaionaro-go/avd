package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseNvidiaSMIUsedMemoryMiB(t *testing.T) {
	output := []byte("123, 128\n456, 4097 MiB\n123, 256 MiB\n")

	usedMiB, found, err := parseNvidiaSMIUsedMemoryMiB(output, 123)

	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(384), usedMiB)
}

func TestParseNvidiaSMIUsedMemoryMiBNoMatch(t *testing.T) {
	output := []byte("123, 128\n")

	usedMiB, found, err := parseNvidiaSMIUsedMemoryMiB(output, 456)

	require.NoError(t, err)
	require.False(t, found)
	require.Equal(t, uint64(0), usedMiB)
}

func TestParseNvidiaSMIUsedMemoryMiBRejectsMalformedLine(t *testing.T) {
	output := []byte("not csv\n")

	_, _, err := parseNvidiaSMIUsedMemoryMiB(output, 456)

	require.Error(t, err)
}
