package avd

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsLocalUDPPortBound(t *testing.T) {
	t.Run("ipv4", func(t *testing.T) {
		// 1. Find a free UDP port
		addr, err := net.ResolveUDPAddr("udp4", "127.0.0.1:0")
		require.NoError(t, err)

		l, err := net.ListenUDP("udp4", addr)
		require.NoError(t, err)

		actualAddr := l.LocalAddr().(*net.UDPAddr)
		port := uint16(actualAddr.Port)

		// Since we are already listening, it should be bound
		isBound, err := isLocalUDPPortBound(port)
		require.NoError(t, err)
		require.True(t, isBound, "Expected port %d to be bound", port)

		// 2. Close it and check again
		err = l.Close()
		require.NoError(t, err)

		isBound, err = isLocalUDPPortBound(port)
		require.NoError(t, err)
		require.False(t, isBound, "Expected port %d to be NOT bound after closing", port)
	})

	t.Run("ipv6", func(t *testing.T) {
		// 1. Find a free UDP port
		addr, err := net.ResolveUDPAddr("udp6", "[::1]:0")
		if err != nil {
			t.Skipf("IPv6 not available: %v", err)
		}

		l, err := net.ListenUDP("udp6", addr)
		if err != nil {
			t.Skipf("IPv6 not available: %v", err)
		}

		actualAddr := l.LocalAddr().(*net.UDPAddr)
		port := uint16(actualAddr.Port)

		// Since we are already listening, it should be bound
		isBound, err := isLocalUDPPortBound(port)
		require.NoError(t, err)
		require.True(t, isBound, "Expected port %d to be bound", port)

		// 2. Close it and check again
		err = l.Close()
		require.NoError(t, err)

		isBound, err = isLocalUDPPortBound(port)
		require.NoError(t, err)
		require.False(t, isBound, "Expected port %d to be NOT bound after closing", port)
	})
}
