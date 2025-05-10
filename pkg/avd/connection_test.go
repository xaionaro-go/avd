package avd

import (
	"net"
	"testing"

	"github.com/facebookincubator/go-belt"
	"github.com/stretchr/testify/require"
)

func TestConnection(t *testing.T) {
	ctx := ctx()
	defer belt.Flush(ctx)
	t.Run("Input", func(t *testing.T) {
		testConnectionInputOrOutput[*NodeInput](t)
	})
	t.Run("Output", func(t *testing.T) {
		testConnectionInputOrOutput[*NodeOutput](t)
	})
}

func testConnectionInputOrOutput[N AbstractNodeIO](t *testing.T) {
	ctx := ctx()
	defer belt.Flush(ctx)

	for _, proto := range SupportedProtocols() {
		var zeroNodeValue N
		_, isOutput := any(zeroNodeValue).(*NodeOutput)
		if proto == ProtocolRTSP && isOutput {
			// this test case fails, something wrong in the avpipeline
			// TODO: fix
			continue
		}
		t.Run(proto.String(), func(t *testing.T) {
			myEnd, mockConn := net.Pipe()
			defer myEnd.Close()

			c, err := newConnection[N](
				ctx,
				&ListeningPort{
					Server: &Server{
						Router: newRouter(ctx),
					},
					Protocol:              proto,
					ConnectionsPublishers: map[net.Addr]*Connection[*NodeInput]{},
					ConnectionsConsumers:  map[net.Addr]*Connection[*NodeOutput]{},
				},
				mockConn,
			)
			require.NoError(t, err)
			require.NotNil(t, c)
		})
	}
}
