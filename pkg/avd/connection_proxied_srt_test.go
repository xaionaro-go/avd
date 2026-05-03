package avd

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTryExtractRouteStringSRT(t *testing.T) {
	ctx := context.Background()
	c := &ConnectionProxied{}

	t.Run("too short", func(t *testing.T) {
		res, err := c.tryExtractRouteStringSRT(ctx, []byte{0x80, 0x00})
		require.NoError(t, err)
		require.Nil(t, res)
	})

	t.Run("not srt", func(t *testing.T) {
		res, err := c.tryExtractRouteStringSRT(ctx, make([]byte, 64))
		require.NoError(t, err)
		require.Nil(t, res)
	})

	t.Run("no extensions", func(t *testing.T) {
		msg := make([]byte, 64)
		msg[0] = 0x80
		// Extension field at 22:24 is 0
		res, err := c.tryExtractRouteStringSRT(ctx, msg)
		require.NoError(t, err)
		require.Nil(t, res)
	})

	t.Run("with streamid", func(t *testing.T) {
		// Per SRT IETF draft 3.2.1.3, StreamID payload is stored as
		// 32-bit little-endian words: bytes within each 4-byte block
		// are reversed on the wire compared to the UTF-8 string.
		streamID := "/test-route"
		msg := buildSRTHandshakeWithStreamID(streamID)

		res, err := c.tryExtractRouteStringSRT(ctx, msg)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, RoutePath("test-route"), *res)
	})

	t.Run("with streamid no slash", func(t *testing.T) {
		streamID := "test-route"
		msg := buildSRTHandshakeWithStreamID(streamID)

		res, err := c.tryExtractRouteStringSRT(ctx, msg)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, RoutePath("test-route"), *res)
	})

	t.Run("with streamid path with slash", func(t *testing.T) {
		// Regression: a routePath like "test/passthrough" used to
		// decode to garbled bytes (e.g. "tsetsap/rhtshguo") because
		// the StreamID payload was treated as raw bytes instead of
		// 32-bit little-endian words.
		streamID := "/test/passthrough"
		msg := buildSRTHandshakeWithStreamID(streamID)

		res, err := c.tryExtractRouteStringSRT(ctx, msg)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, RoutePath("test/passthrough"), *res)
	})
}

// buildSRTHandshakeWithStreamID builds a minimal SRT CONCLUSION handshake
// packet carrying the given UTF-8 streamID encoded per IETF draft section
// 3.2.1.3 (32-bit little-endian words, zero-padded to a 4-byte boundary).
func buildSRTHandshakeWithStreamID(streamID string) []byte {
	paddedLen := (len(streamID) + 3) &^ 3
	extLenWords := uint16(paddedLen / 4)

	msg := make([]byte, 64+4+paddedLen)
	msg[0] = 0x80
	binary.BigEndian.PutUint16(msg[22:24], 0x0001) // Extensions bit
	binary.BigEndian.PutUint32(msg[36:40], 1)      // CONCLUSION
	binary.BigEndian.PutUint16(msg[64:66], 5)      // SID
	binary.BigEndian.PutUint16(msg[66:68], extLenWords)

	// Encode payload as little-endian 32-bit words (byte-reversed
	// within each 4-byte block) to match the on-the-wire format
	// produced by libsrt and ffmpeg. The byte-swap is its own
	// inverse, so we reuse streamIDPayloadDecode for encoding too.
	raw := make([]byte, paddedLen)
	copy(raw, streamID)
	payload, err := streamIDPayloadDecode(raw)
	if err != nil {
		panic(err)
	}
	copy(msg[68:], payload)
	return msg
}
