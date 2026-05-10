// connection_proxied_rtmp_test.go covers the RTMP route-path snooper.
//
// The bug fixed here: when an ffmpeg client opens
// rtmp://host/<segments...>/<name>, ffmpeg's RTMP client splits the URL at
// the LAST '/' and emits app=<segments...> in the connect AMF and the
// stream name only later in publish/play AMF. The original snooper only
// read the connect AMF, producing a truncated route path. This test
// exercises the new behavior where the snooper combines the connect
// "app" field with the stream-name argument from publish/play.
//
// We test the snooper by directly constructing AMF byte streams that
// reproduce what the avd negotiate loop would observe on the wire and
// feeding them into tryExtractRouteString in sequence — the same way
// the negotiate loop would. The negotiate loop itself is exercised in
// integration tests; this is a focused unit test of the parser.

package avd

import (
	"encoding/binary"
	"math"
	"net"
	"testing"

	"github.com/facebookincubator/go-belt"
	"github.com/stretchr/testify/require"
	"github.com/xaionaro-go/avpipeline/router"
)

// amf0String returns an AMF0 string-typed value (type byte 0x02).
func amf0String(s string) []byte {
	out := []byte{0x02}
	out = binary.BigEndian.AppendUint16(out, uint16(len(s)))
	return append(out, []byte(s)...)
}

// amf0Number returns an AMF0 number-typed value (type byte 0x00).
func amf0Number(v float64) []byte {
	out := []byte{0x00}
	return binary.BigEndian.AppendUint64(out, math.Float64bits(v))
}

// amf0Null returns an AMF0 null-typed value (type byte 0x05).
func amf0Null() []byte { return []byte{0x05} }

// amf0Object returns an AMF0 object payload from a literal slice of
// (key, value-bytes) pairs. Pairs preserve order to match real
// ffmpeg-emitted packets.
func amf0Object(kv []amfKV) []byte {
	out := []byte{0x03}
	for _, p := range kv {
		out = binary.BigEndian.AppendUint16(out, uint16(len(p.Key)))
		out = append(out, []byte(p.Key)...)
		out = append(out, p.Value...)
	}
	out = append(out, 0x00, 0x00, 0x09)
	return out
}

type amfKV struct {
	Key   string
	Value []byte
}

// rtmpChunk wraps an AMF0-command payload in an RTMP chunk-format-0
// header on chunk-stream-id 3, message stream id 0 (or the supplied
// streamID), message type 0x14 (AMF0 command). Payloads larger than
// the default chunk size (128) are split with continuation chunks
// (fmt=3 csid=3, single header byte 0xc3). Real ffmpeg uses csid=3
// for the AMF command stream.
func rtmpChunk(streamID uint32, payload []byte) []byte {
	const chunkSize = 128
	csid := byte(3)
	header := []byte{
		0x00 | csid,      // fmt=0, csid=3
		0x00, 0x00, 0x00, // timestamp
		byte(len(payload) >> 16), byte(len(payload) >> 8), byte(len(payload)),
		0x14, // type: AMF0 command
	}
	var sid [4]byte
	binary.LittleEndian.PutUint32(sid[:], streamID)
	header = append(header, sid[:]...)

	if len(payload) <= chunkSize {
		return append(header, payload...)
	}
	out := append(append([]byte{}, header...), payload[:chunkSize]...)
	rest := payload[chunkSize:]
	for len(rest) > 0 {
		out = append(out, 0xc3) // fmt=3, csid=3 continuation
		n := chunkSize
		if len(rest) < n {
			n = len(rest)
		}
		out = append(out, rest[:n]...)
		rest = rest[n:]
	}
	return out
}

// connectChunk builds a realistic RTMP connect command chunk where
// the connect-object specifies the given app (and a tcUrl as ffmpeg
// would). The encoding matches the layout produced by
// `ffmpeg ... rtmp://127.0.0.1:PORT/<app>` (object encoding 0).
func connectChunk(app string) []byte {
	payload := append(amf0String("connect"), amf0Number(1)...)
	payload = append(payload, amf0Object([]amfKV{
		{Key: "app", Value: amf0String(app)},
		{Key: "type", Value: amf0String("nonprivate")},
		{Key: "flashVer", Value: amf0String("FMLE/3.0 (compatible; Lavf62.3.100)")},
		{Key: "tcUrl", Value: amf0String("rtmp://127.0.0.1:0/" + app)},
	})...)
	return rtmpChunk(0, payload)
}

// publishChunk builds a realistic RTMP publish command chunk for the
// given stream name on message stream id 1 (the value libav assigns
// after createStream).
func publishChunk(streamName string) []byte {
	payload := append(amf0String("publish"), amf0Number(5)...)
	payload = append(payload, amf0Null()...)
	payload = append(payload, amf0String(streamName)...)
	payload = append(payload, amf0String("live")...)
	return rtmpChunk(1, payload)
}

// playChunk builds an RTMP play command chunk for the given stream
// name on message stream id 1.
func playChunk(streamName string) []byte {
	payload := append(amf0String("play"), amf0Number(4)...)
	payload = append(payload, amf0Null()...)
	payload = append(payload, amf0String(streamName)...)
	return rtmpChunk(1, payload)
}

// newRTMPSnooperConn returns a ConnectionProxied wired with just enough
// state to exercise the RTMP route-path snooper. The conn is a no-op
// pipe so tryExtractRouteString does not touch the network.
func newRTMPSnooperConn(
	t *testing.T,
	mode PortMode,
) *ConnectionProxied {
	t.Helper()
	myEnd, mockConn := net.Pipe()
	t.Cleanup(func() { _ = myEnd.Close() })
	c := &ConnectionProxied{
		ProtocolProperties: make(map[string]any),
		InitFinished:       make(chan struct{}),
	}
	c.SetProxyConn(mockConn)
	c.SetPort(&ListeningPortProxied{
		Server: &Server{
			Router: router.New[RouteCustomData](ctx()),
		},
		Mode:        mode,
		Protocol:    ProtocolRTMP,
		Connections: map[net.Addr]*ConnectionProxied{},
	})
	return c
}

// TestRTMPRouteParseConnectPlusPublish exercises the bug from prod:
// the consumer URL rtmp://host/pixel/dji-osmo-pocket-3-merged is split
// by ffmpeg into app=pixel and stream name dji-osmo-pocket-3-merged.
// The snooper must combine both into the full route path. The connect
// chunk alone must NOT yield a final route path — that would lock in
// the truncated path 'pixel' before the stream name has arrived.
func TestRTMPRouteParseConnectPlusPublish(t *testing.T) {
	ctx := ctx()
	defer belt.Flush(ctx)
	c := newRTMPSnooperConn(t, PortModePublishers)

	// Step 1: connect chunk arrives. Snooper must remember the app
	// but not yet finalize the route path.
	rp, err := c.tryExtractRouteString(ctx, connectChunk("pixel"))
	require.NoError(t, err)
	require.Nil(t, rp, "connect alone must not finalize route path; stream name is still en route")

	// Step 2: publish chunk arrives. Snooper merges app + streamName.
	rp, err = c.tryExtractRouteString(ctx, publishChunk("dji-osmo-pocket-3-merged"))
	require.NoError(t, err)
	require.NotNil(t, rp)
	require.Equal(t, RoutePath("pixel/dji-osmo-pocket-3-merged"), *rp)
}

// TestRTMPRouteParsePublisherConnectFullApp covers RTMP publishers
// that place the entire route path in the connect 'app' field. The
// publisher route must be available before publish so the proxy can
// attach the backend that completes the RTMP open handshake.
func TestRTMPRouteParsePublisherConnectFullApp(t *testing.T) {
	ctx := ctx()
	defer belt.Flush(ctx)
	c := newRTMPSnooperConn(t, PortModePublishers)

	rp, err := c.tryExtractRouteString(ctx, connectChunk("pixel/source-av1-1080"))
	require.NoError(t, err)
	require.NotNil(t, rp)
	require.Equal(t, RoutePath("pixel/source-av1-1080"), *rp)
}

// TestRTMPRouteParseConnectPlusPlay covers the consumer side: a
// consumer pulling rtmp://host/pixel/source-merged emits
// connect(app=pixel) then play(streamName=source-merged).
// Same merge logic as publish.
func TestRTMPRouteParseConnectPlusPlay(t *testing.T) {
	ctx := ctx()
	defer belt.Flush(ctx)
	c := newRTMPSnooperConn(t, PortModeConsumers)

	rp, err := c.tryExtractRouteString(ctx, connectChunk("pixel"))
	require.NoError(t, err)
	require.Nil(t, rp)

	rp, err = c.tryExtractRouteString(ctx, playChunk("source-merged"))
	require.NoError(t, err)
	require.NotNil(t, rp)
	require.Equal(t, RoutePath("pixel/source-merged"), *rp)
}

// TestRTMPRouteParseStreamNameRedundant covers the case where a
// publisher emits a stream name that is already a suffix of (or equal
// to the tail of) app. We must not double-append. E.g. some clients
// emit publish(streamName=app) for compatibility.
func TestRTMPRouteParseStreamNameRedundant(t *testing.T) {
	require.Equal(t, "pixel/source-av1-1080", rtmpMergeAppAndStream("pixel/source-av1-1080", "source-av1-1080"))
	require.Equal(t, "pixel/source-av1-1080", rtmpMergeAppAndStream("pixel/source-av1-1080", "pixel/source-av1-1080"))
}
