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
