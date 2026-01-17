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
		msg := make([]byte, 80)
		msg[0] = 0x80
		binary.BigEndian.PutUint16(msg[22:24], 0x0001) // Extensions bit
		binary.BigEndian.PutUint32(msg[36:40], 1)      // CONCLUSION

		// Extension Type 5 (SID), Length 2 words (8 bytes)
		pos := 64
		binary.BigEndian.PutUint16(msg[pos:pos+2], 5)
		binary.BigEndian.PutUint16(msg[pos+2:pos+4], 2)
		copy(msg[pos+4:], "/test-route\x00\x00\x00\x00\x00") // This is longer than 2 words, but let's adjust

		// Re-calculate to match exactly
		streamID := "/test-route"
		paddedLen := (len(streamID) + 3) &^ 3
		extLenWords := uint16(paddedLen / 4)
		msg = make([]byte, 64+4+paddedLen)
		msg[0] = 0x80
		binary.BigEndian.PutUint16(msg[22:24], 0x0001)
		binary.BigEndian.PutUint32(msg[36:40], 1)
		binary.BigEndian.PutUint16(msg[64:66], 5)
		binary.BigEndian.PutUint16(msg[66:68], extLenWords)
		copy(msg[68:], streamID)

		res, err := c.tryExtractRouteStringSRT(ctx, msg)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, RoutePath("test-route"), *res)
	})

	t.Run("with streamid no slash", func(t *testing.T) {
		streamID := "test-route"
		paddedLen := (len(streamID) + 3) &^ 3
		extLenWords := uint16(paddedLen / 4)
		msg := make([]byte, 64+4+paddedLen)
		msg[0] = 0x80
		binary.BigEndian.PutUint16(msg[22:24], 0x0001)
		binary.BigEndian.PutUint32(msg[36:40], 1)
		binary.BigEndian.PutUint16(msg[64:66], 5)
		binary.BigEndian.PutUint16(msg[66:68], extLenWords)
		copy(msg[68:], streamID)

		res, err := c.tryExtractRouteStringSRT(ctx, msg)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, RoutePath("test-route"), *res)
	})
}
