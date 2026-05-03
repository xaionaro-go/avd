// connection_proxied_srt.go participates in the proxied flow that lets a SINGLE
// listening UDP port fan out SRT publishers and consumers across many remote
// 5-tuples. Per-tuple, avd creates a ConnectionProxied that demuxes UDP
// datagrams to a per-client libav AVFormatContext. The proxied design exists
// SPECIFICALLY because libav's SRT (libsrt) binds one caller per AVFormatContext
// and cannot demultiplex by streamid on its own. Do not route SRT through the
// direct-dispatch path even if the consumer handshake here looks broken — fix
// the bidirectional pump (libav listener replies must reach the external SRT
// caller) instead.

package avd

import (
	"context"
	"encoding/binary"
	"fmt"
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
			// Per the SRT IETF draft section 3.2.1.3, the StreamID
			// extension contents are stored as 32-bit little-endian
			// words. The header reader above uses big-endian for
			// extType/extLenWords (which the protocol also requires
			// in the surrounding header), but the StreamID payload
			// itself is byte-reversed within each 4-byte word and
			// must be unswapped before being interpreted as UTF-8.
			decoded, err := streamIDPayloadDecode(payload)
			if err != nil {
				logger.Errorf(ctx, "SRT StreamID payload decode failed: %v", err)
				return ""
			}
			return strings.TrimRight(string(decoded), "\x00")
		}
		pos += extLen
	}
	return ""
}

// streamIDPayloadDecode reverses bytes within each 4-byte word of an SRT
// StreamID extension payload, undoing the little-endian-word storage format
// mandated by the SRT IETF draft (section 3.2.1.3) so the result can be
// interpreted as a UTF-8 string. The transform is its own inverse, so this
// function is also used to encode a UTF-8 string into the wire format.
// The input length must be a multiple of 4 because StreamID extension
// lengths are expressed in 32-bit words; a non-multiple is reported as an
// error rather than silently truncated.
func streamIDPayloadDecode(payload []byte) ([]byte, error) {
	if len(payload)%4 != 0 {
		return nil, fmt.Errorf("SRT StreamID payload length %d is not a multiple of 4", len(payload))
	}
	out := make([]byte, len(payload))
	for i := 0; i < len(payload); i += 4 {
		out[i+0] = payload[i+3]
		out[i+1] = payload[i+2]
		out[i+2] = payload[i+1]
		out[i+3] = payload[i+0]
	}
	return out, nil
}

func (c *ConnectionProxied) correctMessageSRT(
	_ context.Context,
	msg []byte,
) ([]byte, error) {
	return msg, nil
}
