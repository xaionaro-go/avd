// connection_proxied_test.go tests for proxied connections.

package avd

import (
	"net"
	"testing"

	"github.com/facebookincubator/go-belt"
	"github.com/stretchr/testify/require"
	"github.com/xaionaro-go/avpipeline/router"
)

func TestConnectionProxied(t *testing.T) {
	ctx := ctx()
	defer belt.Flush(ctx)
	t.Run("Input", func(t *testing.T) {
		testConnectionProxiedInputOrOutput(t, PortModePublishers)
	})
	t.Run("Output", func(t *testing.T) {
		testConnectionProxiedInputOrOutput(t, PortModeConsumers)
	})
}

func testConnectionProxiedInputOrOutput(
	t *testing.T,
	portMode PortMode,
) {
	ctx := ctx()
	defer belt.Flush(ctx)

	for _, proto := range SupportedProtocols() {
		if proto == ProtocolRTSP && portMode == PortModeConsumers {
			// TODO: not supported yet; but would be nice to fix
			continue
		}
		t.Run(proto.String(), func(t *testing.T) {
			defer belt.Flush(ctx)

			myEnd, mockConn := net.Pipe()
			defer myEnd.Close()

			c, err := newConnectionProxied(
				ctx,
				&ListeningPortProxied{
					Server: &Server{
						Router: router.New[RouteCustomData](ctx),
					},
					Mode:        portMode,
					Protocol:    proto,
					Connections: map[net.Addr]*ConnectionProxied{},
				},
				mockConn,
			)
			require.NoError(t, err)
			require.NotNil(t, c)
		})
	}
}

// TestConnectionProxiedGetRoutePathAfterClose guards against BUG-006: a late
// GetRoutePath/GetURLPath call after closeLocked() has nilled c.Port must not
// panic. The connection can be retained by loggers or scheduled goroutines
// after close, so these getters must degrade gracefully.
func TestConnectionProxiedGetRoutePathAfterClose(t *testing.T) {
	t.Run("nil receiver", func(t *testing.T) {
		var c *ConnectionProxied
		require.NotPanics(t, func() { _ = c.GetRoutePath() })
		require.NotPanics(t, func() { _ = c.GetURLPath() })
		require.Equal(t, RoutePath(""), c.GetRoutePath())
		require.Equal(t, "", c.GetURLPath())
	})

	t.Run("nil port", func(t *testing.T) {
		// Equivalent to the state after closeLocked() calls c.SetPort(nil).
		c := &ConnectionProxied{}
		require.NotPanics(t, func() { _ = c.GetRoutePath() })
		require.NotPanics(t, func() { _ = c.GetURLPath() })
		require.Equal(t, RoutePath(""), c.GetRoutePath())
		require.Equal(t, "", c.GetURLPath())
	})

	t.Run("after closeLocked", func(t *testing.T) {
		ctx := ctx()
		defer belt.Flush(ctx)

		myEnd, mockConn := net.Pipe()
		defer myEnd.Close()

		c := &ConnectionProxied{
			InitFinished: make(chan struct{}),
		}
		c.SetProxyConn(mockConn)
		c.SetPort(&ListeningPortProxied{
			Server: &Server{
				Router: router.New[RouteCustomData](ctx),
			},
			Mode:        PortModePublishers,
			Protocol:    ProtocolRTMP,
			Connections: map[net.Addr]*ConnectionProxied{},
		})
		require.NotNil(t, c.GetPort())

		require.NoError(t, c.Close(ctx))
		require.Nil(t, c.GetPort())

		require.NotPanics(t, func() { _ = c.GetRoutePath() })
		require.NotPanics(t, func() { _ = c.GetURLPath() })
		require.Equal(t, RoutePath(""), c.GetRoutePath())
		require.Equal(t, "", c.GetURLPath())
	})
}

// TestConnectionProxiedGetURLPathUnknownProtocol guards against NEW-BUG-4:
// GetURLPath used to panic on an unrecognized Protocol while GetRoutePath
// returned gracefully. The asymmetry meant that a logged connection whose
// protocol is not wired for URL-path formatting (e.g. ProtocolMPEGTS, or a
// zero-valued UndefinedProtocol surviving partial initialization) would
// crash the process. Both getters must degrade to an empty string.
func TestConnectionProxiedGetURLPathUnknownProtocol(t *testing.T) {
	ctx := ctx()
	defer belt.Flush(ctx)

	c := &ConnectionProxied{}
	c.SetPort(&ListeningPortProxied{
		Server: &Server{
			Router: router.New[RouteCustomData](ctx),
		},
		Mode:        PortModePublishers,
		Protocol:    UndefinedProtocol,
		Connections: map[net.Addr]*ConnectionProxied{},
	})

	require.NotPanics(t, func() { _ = c.GetURLPath() })
	require.Equal(t, "", c.GetURLPath())
}
