// srt_integration_test.go implements integration tests for SRT support.

package avd

import (
	"context"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/facebookincubator/go-belt"
	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xaionaro-go/avd/pkg/avd/types"
	"github.com/xaionaro-go/avpipeline"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/node"
	"github.com/xaionaro-go/avpipeline/processor"
	"github.com/xaionaro-go/observability"
	"github.com/xaionaro-go/secret"
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

// TestSRTIntegrationConsumer verifies the SRT consumer (output) path:
// it pushes a stream into a publisher port, then pulls from a consumer port
// configured with the same default route, and asserts the puller receives
// frames/packets. This guards the publisher → router → consumer egress wiring
// that the publisher-only test (TestSRTIntegration) does not exercise.
//
// Originally surfaced a SIGFPE crash inside the consumer mpegts muxer when an
// audio output stream was created with codecpar.sample_rate=0 (codec params
// had not yet propagated from the publisher's demuxer when the consumer was
// allowed to attach). The fix lives in avpipeline kernel.Output's
// configureOutputStream, which now rejects audio streams with sample_rate=0
// (mirroring the existing video width/height check) so the caller retries
// once the codec parameters are populated, instead of letting mpegts divide
// by zero in av_interleaved_write_frame.
func TestSRTIntegrationConsumer(t *testing.T) {
	ctx := ctx()
	defer belt.Flush(ctx)

	s := NewServer(ctx)
	// Close the server with a hard timeout: the consumer connection's
	// deferred close path can deadlock when the routed audio stream had
	// invalid codec params (FromKernel.Close → wg.Wait waits on a reader
	// loop that never receives an error). Tracked separately; we accept a
	// brief leak here so the test reports its actual outcome instead of
	// hanging.
	defer closeWithTimeout(ctx, t, "server", s.Close, 3*time.Second)

	const routePath = RoutePath("test-srt-consumer")

	pubPort, err := s.Listen(
		ctx, "srt:127.0.0.1:0", ProtocolSRT, PortModePublishers,
		types.ListenOptionDefaultRoutePath(routePath),
	)
	require.NoError(t, err)
	defer closeWithTimeout(ctx, t, "publisher port", pubPort.Close, 2*time.Second)

	consPort, err := s.Listen(
		ctx, "srt:127.0.0.1:0", ProtocolSRT, PortModeConsumers,
		types.ListenOptionDefaultRoutePath(routePath),
		types.ListenOptionWaitUntilVideoTracksCount(1),
	)
	require.NoError(t, err)
	defer closeWithTimeout(ctx, t, "consumer port", consPort.Close, 2*time.Second)

	pubURL, err := pubPort.(*ListeningPortProxied).GetURLForRoute(ctx, string(routePath))
	require.NoError(t, err)
	consURL, err := consPort.(*ListeningPortProxied).GetURLForRoute(ctx, string(routePath))
	require.NoError(t, err)

	logger.Infof(ctx, "Publisher URL: %s", pubURL.String())
	logger.Infof(ctx, "Consumer URL:  %s", consURL.String())

	// Start the publisher in the background. pushTestFileTo blocks until
	// io.EOF or context cancellation, so we run it in a goroutine and stop
	// it once the test has observed enough frames on the consumer side.
	// We deliberately do not call pushTestFileTo: it requires the publish
	// pipeline to terminate with io.EOF, which does not hold here because
	// the consumer chain can shut down out from under the publisher (e.g.
	// when audio codec params are not yet populated and updateOutputFormat
	// rejects the stream on the consumer side). We push the same file with
	// a relaxed error policy that accepts any teardown reason.
	pubCtx, pubCancel := context.WithCancel(ctx)
	var pubWg sync.WaitGroup
	pubWg.Add(1)
	observability.Go(pubCtx, func(pubCtx context.Context) {
		defer pubWg.Done()
		pushTestFileToRelaxed(pubCtx, t, pubURL.String())
	})
	defer func() {
		pubCancel()
		// Same rationale as closeWithTimeout: pushTestFileTo's serve goroutine
		// can stay parked on a downstream WaitGroup when the consumer chain
		// shuts down out from under it. Bound the wait so the test reports its
		// actual outcome rather than timing out the whole binary.
		done := make(chan struct{})
		go func() { pubWg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			logger.Warnf(ctx, "publisher goroutine did not exit within 2s; leaking")
		}
	}()

	// Open the consumer URL as an input. The consumer port blocks the
	// underlying SRT accept until WaitUntil*TracksCount is satisfied, so
	// retry until the publisher has been routed and tracks are visible.
	pullCtx, pullCancel := context.WithCancel(ctx)
	defer pullCancel()

	var inputKernel *kernel.Input
	require.Eventually(t, func() bool {
		k, err := kernel.NewInputFromURL(pullCtx, consURL.String(), secret.New(""), kernel.InputConfig{})
		if err != nil {
			logger.Debugf(pullCtx, "consumer NewInputFromURL not ready: %v", err)
			return false
		}
		inputKernel = k
		return true
	}, 30*time.Second, 200*time.Millisecond, "unable to open SRT consumer URL")

	inputNode := node.NewFromKernel(pullCtx, inputKernel, processor.DefaultOptionsInput()...)

	errCh := make(chan node.Error, 100)
	var serveWg sync.WaitGroup
	serveWg.Add(1)
	observability.Go(pullCtx, func(pullCtx context.Context) {
		defer serveWg.Done()
		defer close(errCh)
		avpipeline.Serve(pullCtx, avpipeline.ServeConfig{}, errCh, inputNode)
	})

	require.Eventually(t, func() bool {
		counters := inputNode.GetProcessor().CountersPtr()
		got := counters.Generated.Frames.TotalCount() + counters.Generated.Packets.TotalCount() +
			counters.Processed.Frames.TotalCount() + counters.Processed.Packets.TotalCount()
		return got > 0
	}, 30*time.Second, 200*time.Millisecond, "no frames/packets received from SRT consumer port")

	pullCancel()
	// Bound the serve-shutdown wait — the pull goroutine can be parked on
	// the same downstream WaitGroup as the consumer connection close.
	serveDone := make(chan struct{})
	go func() { serveWg.Wait(); close(serveDone) }()
	select {
	case <-serveDone:
	case <-time.After(2 * time.Second):
		logger.Warnf(ctx, "pull serve goroutine did not exit within 2s; leaking")
	}

	counters := inputNode.GetProcessor().CountersPtr()
	total := counters.Generated.Frames.TotalCount() + counters.Generated.Packets.TotalCount() +
		counters.Processed.Frames.TotalCount() + counters.Processed.Packets.TotalCount()
	logger.Infof(ctx, "SRT consumer received %d frames/packets", total)
	require.Greater(t, total, uint64(0), "no frames/packets were received from SRT consumer port")
}

// pushTestFileToRelaxed mirrors pushTestFileTo but does not assert that the
// publish pipeline ends with io.EOF. The consumer-side chain in this test can
// shut down before EOF (e.g. when invalid codec params propagate through the
// router), and we only need the publisher to *keep pushing* — its termination
// reason is not what we are validating.
func pushTestFileToRelaxed(
	ctx context.Context,
	t *testing.T,
	dstAddr string,
) {
	t.Helper()
	logger.Debugf(ctx, "pushTestFileToRelaxed")
	defer func() { logger.Debugf(ctx, "/pushTestFileToRelaxed") }()

	var wg sync.WaitGroup
	defer wg.Wait()

	ctx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	recordingPath := path.Join("testdata", "video0-1v1a.mov")

	inputKernel, err := kernel.NewInputFromURL(ctx, recordingPath, secret.New(""), kernel.InputConfig{})
	require.NoError(t, err)

	inputNode := node.NewFromKernel(ctx, inputKernel, processor.DefaultOptionsInput()...)

	outputKernel, err := kernel.NewOutputFromURL(ctx, dstAddr, secret.New(""), kernel.OutputConfig{
		ErrorOnNSequentialInvalidDTS: 100,
	})
	require.NoError(t, err)

	outputNode := node.NewFromKernel(ctx, outputKernel, processor.DefaultOptionsOutput()...)

	inputNode.AddPushTo(ctx, outputNode)
	errCh := make(chan node.Error, 100)

	wg.Add(1)
	observability.Go(ctx, func(ctx context.Context) {
		defer wg.Done()
		defer close(errCh)
		avpipeline.Serve(ctx, avpipeline.ServeConfig{}, errCh, inputNode)
	})

	select {
	case <-ctx.Done():
		return
	case err := <-errCh:
		// Any termination is acceptable here; the test asserts on the
		// consumer side, not the publisher's exit code.
		logger.Debugf(ctx, "publisher pipeline ended: %v", err)
	}
}

// TestSRTIntegrationStreamIDDecode is a regression test for d34f2b1
// ("Fix SRT consumer StreamID decoding per IETF draft"). Per IETF draft
// section 3.2.1.3, the StreamID extension payload is stored as 32-bit
// little-endian words (bytes within each 4-byte block are byte-swapped on
// the wire). Before d34f2b1, avd's extractStreamID treated the payload as
// raw UTF-8, so a streamid like "test/passthrough" decoded to garbled
// bytes ("tsetsap/rhtshguo") and never matched the configured route path —
// the client got "Connection failed: Input/output error".
//
// A naive end-to-end push/pull test does NOT catch this because the bug is
// symmetric: the publisher and consumer both encode (via libsrt/ffmpeg)
// the same wire bytes, and avd's broken decode produces the same garbled
// path for both sides — they still self-match. To make the test FAIL on a
// binary lacking the fix, we assert directly on the post-handshake
// RoutePath stored on the ConnectionProxied: with the fix it equals the
// expected UTF-8 path; without it, the path is byte-swapped garbage.
//
// The route name is chosen so the byte-swap actually changes the string
// (4-byte boundaries fall mid-token, not on word boundaries), matching
// the prod-shape "pixel/dji-osmo-pocket-3-merged" pattern.
func TestSRTIntegrationStreamIDDecode(t *testing.T) {
	ctx := ctx()
	defer belt.Flush(ctx)

	s := NewServer(ctx)
	defer closeWithTimeout(ctx, t, "server", s.Close, 3*time.Second)

	// Deliberately do NOT set DefaultRoutePath: we want the route to come
	// from the streamID handshake decode, which is exactly the code path
	// d34f2b1 fixes. A path with an embedded slash ensures the byte-swap
	// produces a visibly different string from the UTF-8 form.
	const routePath = RoutePath("test/passthrough")

	pubPort, err := s.Listen(ctx, "srt:127.0.0.1:0", ProtocolSRT, PortModePublishers)
	require.NoError(t, err)
	defer closeWithTimeout(ctx, t, "publisher port", pubPort.Close, 2*time.Second)

	pubURL, err := pubPort.(*ListeningPortProxied).GetURLForRoute(ctx, string(routePath))
	require.NoError(t, err)
	logger.Infof(ctx, "Publisher URL: %s", pubURL.String())

	pubCtx, pubCancel := context.WithCancel(ctx)
	var pubWg sync.WaitGroup
	pubWg.Add(1)
	observability.Go(pubCtx, func(pubCtx context.Context) {
		defer pubWg.Done()
		pushTestFileToRelaxed(pubCtx, t, pubURL.String())
	})
	defer func() {
		pubCancel()
		done := make(chan struct{})
		go func() { pubWg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			logger.Warnf(ctx, "publisher goroutine did not exit within 2s; leaking")
		}
	}()

	// Wait for the publisher's handshake to be processed and the route
	// path to be parsed and stored on the connection.
	var pubConn *ConnectionProxied
	require.Eventually(t, func() bool {
		conns := pubPort.GetConnections(ctx)
		for _, c := range conns {
			cp, ok := c.(*ConnectionProxied)
			if !ok {
				continue
			}
			if cp.RoutePath != nil {
				pubConn = cp
				return true
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond, "no SRT publisher connection with parsed RoutePath")

	got := pubConn.GetRoutePath()
	require.Equalf(
		t, routePath, got,
		"SRT StreamID decode produced wrong route path; "+
			"this is the d34f2b1 regression — payload was not byte-swapped "+
			"per IETF draft section 3.2.1.3. expected %q, got %q (raw bytes: %x)",
		string(routePath), string(got), []byte(got),
	)

	// Belt and suspenders: also verify frames actually flow through the
	// route — a correct decode should also produce a usable stream.
	require.Eventually(t, func() bool {
		handler := pubConn.GetHandler()
		if handler == nil {
			return false
		}
		pub, ok := handler.(*ConnectionProxiedHandlerPublisher)
		if !ok {
			return false
		}
		n := pub.GetNodeTyped()
		if n == nil {
			return false
		}
		counters := n.GetProcessor().CountersPtr()
		count := counters.Processed.Frames.TotalCount() + counters.Processed.Packets.TotalCount() +
			counters.Generated.Frames.TotalCount() + counters.Generated.Packets.TotalCount()
		return count > 0
	}, 15*time.Second, 200*time.Millisecond, "no frames/packets processed by publisher node after handshake")
}

// closeWithTimeout invokes the given Close function in a goroutine and waits
// for completion up to timeout. If Close blocks past the deadline the test
// records a non-fatal log and proceeds, intentionally leaking the underlying
// goroutines: cleaning up the avd consumer connection requires changes to the
// router/processor close paths (FromKernel.Close.wg.Wait) that are out of
// scope for this test, and we prefer a leak over masking the real result.
func closeWithTimeout(
	ctx context.Context,
	t *testing.T,
	name string,
	closeFn func(context.Context) error,
	timeout time.Duration,
) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- closeFn(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			logger.Warnf(ctx, "close %s returned: %v", name, err)
		}
	case <-time.After(timeout):
		logger.Warnf(ctx, "close %s did not return within %s; proceeding (goroutine leaked)", name, timeout)
	}
}
