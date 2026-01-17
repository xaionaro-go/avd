// srt_integration_test.go implements integration tests for SRT support.

package avd

import (
	"testing"
	"time"

	"github.com/facebookincubator/go-belt"
	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSRTIntegration(t *testing.T) {
	ctx := ctx()
	defer belt.Flush(ctx)

	s := NewServer(ctx)
	defer s.Close(ctx)

	// Listen for SRT publishers
	// "srt:127.0.0.1:0" will pick a random UDP port.
	port, err := s.Listen(ctx, "srt:127.0.0.1:0", ProtocolSRT, PortModePublishers)
	require.NoError(t, err)
	defer port.Close(ctx)

	portProxied := port.(*ListeningPortProxied)
	routePath := RoutePath("test-srt-integration")
	url, err := portProxied.GetURLForRoute(ctx, string(routePath))
	require.NoError(t, err)

	logger.Infof(ctx, "Pushing to %s", url.String())

	// Start pushing the test file
	// Pushing to the server.
	pushTestFileTo(ctx, t, url.String())

	// Wait untill the connection is established and then some frames are processed.
	var connections []Connection
	assert.Eventually(t, func() bool {
		connections = port.GetConnections(ctx)
		return len(connections) > 0
	}, 10*time.Second, 100*time.Millisecond, "no connections found on the port")

	var totalProcessedFrames uint64
	for _, conn := range connections {
		cp := conn.(*ConnectionProxied)
		// We wait for InitFinished to make sure everything is initialized.
		select {
		case <-cp.InitFinished:
		case <-time.After(5 * time.Second):
			t.Errorf("timeout waiting for InitFinished on connection %s", cp.String())
			continue
		}

		handler := cp.GetHandler()
		if handler == nil {
			logger.Warnf(ctx, "handler is nil for connection %s", cp.String())
			continue
		}
		publisher, ok := handler.(*ConnectionProxiedHandlerPublisher)
		if !ok {
			logger.Warnf(ctx, "handler is not a publisher for connection %s", cp.String())
			continue
		}
		node := publisher.GetNodeTyped()
		if node == nil {
			logger.Warnf(ctx, "node is nil for publisher %s", cp.String())
			continue
		}
		counters := node.GetProcessor().CountersPtr()
		count := counters.Processed.Frames.TotalCount() + counters.Processed.Packets.TotalCount() +
			counters.Generated.Frames.TotalCount() + counters.Generated.Packets.TotalCount()
		logger.Infof(ctx, "Connection %s processed %d frames/packets", cp.String(), count)
		totalProcessedFrames += count
	}

	logger.Infof(ctx, "Total processed frames: %d", totalProcessedFrames)
	require.Greater(t, totalProcessedFrames, uint64(0), "no frames were processed by avd")
}
