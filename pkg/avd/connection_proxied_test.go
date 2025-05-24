package avd

import (
	"context"
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
		testConnectionProxiedInputOrOutput(t, newConnectionProxiedPublisher)
	})
	t.Run("Output", func(t *testing.T) {
		testConnectionProxiedInputOrOutput(t, newConnectionProxiedConsumer)
	})
}

func testConnectionProxiedInputOrOutput[T any](
	t *testing.T,
	newConnectionProxied func(
		ctx context.Context,
		p *ListeningPortProxied,
		conn net.Conn,
	) (T, error),
) {
	ctx := ctx()
	defer belt.Flush(ctx)

	for _, proto := range SupportedProtocols() {
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
					Protocol:              proto,
					ConnectionsPublishers: map[net.Addr]*ConnectionProxiedPublisher{},
					ConnectionsConsumers:  map[net.Addr]*ConnectionProxiedConsumer{},
				},
				mockConn,
			)
			require.NoError(t, err)
			require.NotNil(t, c)
		})
	}
}
