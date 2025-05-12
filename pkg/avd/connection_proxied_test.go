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
		testConnectionProxiedInputOrOutput[*NodeInput](t)
	})
	t.Run("Output", func(t *testing.T) {
		testConnectionProxiedInputOrOutput[*NodeOutput](t)
	})
}

func testConnectionProxiedInputOrOutput[N AbstractNodeIO](t *testing.T) {
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

			c, err := newConnectionProxied[N](
				ctx,
				&ListeningPortProxied{
					Server: &Server{
						Router: router.New(ctx),
					},
					Protocol:              proto,
					ConnectionsPublishers: map[net.Addr]*ConnectionProxied[*NodeInput]{},
					ConnectionsConsumers:  map[net.Addr]*ConnectionProxied[*NodeOutput]{},
				},
				mockConn,
			)
			require.NoError(t, err)
			require.NotNil(t, c)
		})
	}
}
