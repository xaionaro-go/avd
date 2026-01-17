package avd

import (
	"context"
	"encoding/binary"
	"strings"

	"github.com/facebookincubator/go-belt/tool/logger"
)

// SRT (Secure Reliable Transport) Protocol References:
// - IETF Specification (Draft): https://datatracker.ietf.org/doc/html/draft-sharabayko-srt
// - Technical Overview (PDF): https://github.com/Haivision/srt/files/2489142/SRT_Protocol_TechnicalOverview_DRAFT_2018-10-17.pdf
// - Latest Working Copy: https://haivision.github.io/srt-rfc/draft-sharabayko-srt.html
//
// SRT Packet structure:
// Data Packet: Bit 0 is 0.
// Control Packet: Bit 0 is 1. (0x80 in the first byte)
//
// SRT Handshake Control Packet (Big Endian Example):
// 80 00 00 00  (Control Type: Handshake=0, Subtype: 0)
// 00 00 00 00  (Reserved)
// 00 00 00 00  (Timestamp)
// 00 00 00 00  (Destination Socket ID)
// 00 00 00 05  (SRT Version)
// 00 00 00 01  (Encryption Flags / Extension Flags: 0x01 = HS_EXT_BIT)
// ...
// Extension (at offset 64):
// 00 05 00 03  (Type: StreamID=5, Length: 3 words = 12 bytes)
// 2f 73 72 74  (/srt)
// 2d 69 6e 70  (-inp)
// 75 74 00 00  (ut\0\0)
//
// Little Endian (Word Swapped - common in some implementations) Example:
// 00 00 00 80  (instead of 80 00 00 00)
// 00 00 00 00
// 00 00 00 00
// ...

const (
	// srtPacketTypeHandshake is the combination of Control Bit (1),
	// Control Type (Handshake=0), and Subtype (0).
	srtPacketTypeHandshake = 0x8000

	// srtPacketSubTypeHandshake is the subtype for handshake packets.
	srtPacketSubTypeHandshake = 0x0000

	// srtHandshakeExtensionBit indicates that extensions are present in the handshake.
	// It is bit 0 of the extension field in the handshake packet.
	srtHandshakeExtensionBit = uint16(0x01)

	// srtExtensionTypeStreamID is the extension type for StreamID (SRT_CMD_SID).
	srtExtensionTypeStreamID = uint16(5)

	// srtHandshakeHeaderSize is the minimum size of an SRT handshake packet before extensions.
	srtHandshakeHeaderSize = 64
)

func (c *ConnectionProxied) onInitFinishedSRT(
	ctx context.Context,
) {
}

func (c *ConnectionProxied) tryExtractRouteStringSRT(
	ctx context.Context,
	msg []byte,
) (*RoutePath, error) {
	if len(msg) < srtHandshakeHeaderSize {
		return nil, nil
	}

	if !c.isHandshakePacket(msg) {
		return nil, nil
	}

	streamID := c.extractStreamID(ctx, msg)
	if streamID == "" {
		return nil, nil
	}

	routePath := strings.TrimPrefix(streamID, "/")
	return ptr(RoutePath(routePath)), nil
}

func (c *ConnectionProxied) isHandshakePacket(msg []byte) bool {
	// Control Type 0 (Handshake), Subtype 0
	return binary.BigEndian.Uint16(msg[0:2]) == srtPacketTypeHandshake &&
		binary.BigEndian.Uint16(msg[2:4]) == srtPacketSubTypeHandshake
}

func (c *ConnectionProxied) extractStreamID(ctx context.Context, msg []byte) string {
	extensionField := binary.BigEndian.Uint16(msg[22:24])
	if extensionField&srtHandshakeExtensionBit == 0 {
		return ""
	}

	pos := srtHandshakeHeaderSize
	for pos+4 <= len(msg) {
		extType := binary.BigEndian.Uint16(msg[pos : pos+2])
		extLenWords := binary.BigEndian.Uint16(msg[pos+2 : pos+4])
		pos += 4
		extLen := int(extLenWords) * 4

		if pos+extLen > len(msg) {
			logger.Errorf(ctx, "SRT extension length exceeds packet size: %d > %d", pos+extLen, len(msg))
			break
		}

		if extType == srtExtensionTypeStreamID {
			payload := msg[pos : pos+extLen]
			return strings.TrimRight(string(payload), "\x00")
		}
		pos += extLen
	}
	return ""
}

func (c *ConnectionProxied) correctMessageSRT(
	_ context.Context,
	msg []byte,
) ([]byte, error) {
	return msg, nil
}
