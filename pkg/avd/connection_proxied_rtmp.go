// connection_proxied_rtmp.go provides RTMP-specific logic for proxied connections.
//
// Scope of the AMF snooper (rtmpParseConnectApp / rtmpParseStreamName):
// these scanners are CHUNK-HEADER-BLIND. They walk the raw bytes returned
// by a single conn.Read() (see negotiate() in connection_proxied.go) and
// assume the AMF0 command body is contiguous in that buffer. RTMP's wire
// protocol splits a message larger than the negotiated chunk size (default
// 128 bytes) by inserting a 1-byte type-3 continuation header (0xc3 for
// csid=3) every chunkSize payload bytes. If such a continuation byte falls
// INSIDE the AMF0 type/length/value run that the scanner walks, parsing
// produces wrong keys/values or a "message too short" error.
//
// We accept this limitation because every production RTMP client we serve
// (libav-originated ffmpeg/ffstream) emits the connect+publish/play AMFs
// well under 128 payload bytes for our route-path lengths. The fuzz tests
// in connection_proxied_rtmp_fuzz_test.go pin this assumption: they
// reproduce the chunk-spanning failure mode against a synthetic non-libav
// client so a future protocol-extension regression is caught loudly.
//
// Removing the limitation requires either (a) a chunk reassembler that
// strips continuation bytes before scanning, or (b) replacing this
// snooper with a real AMF demuxer. Both are deferred - chunk reassembly
// is non-trivial state because chunkSize itself is renegotiable mid-
// stream via the SetChunkSize control message, and the snooper would
// have to track per-csid message-state across reads. The current
// libav-only audience does not justify that surface area.

package avd

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avcommon"
)

// Keys used inside ConnectionProxied.ProtocolProperties to carry
// snooper state across negotiate-loop reads. RTMP requires this
// because ffmpeg may split rtmp://host/<segments...>/<name> at the
// LAST '/': it emits app=<segments...> in the connect AMF and only
// emits the stream name later in the publish/play AMF, so the route
// path cannot always be decided from a single read.
const (
	rtmpPropPendingApp = "rtmp.pending_app"
)

func (c *ConnectionProxied) AVRTMPContext(ctx context.Context) *avcommon.RTMPContext {
	urlCtx := c.AVURLContext(ctx)
	if urlCtx == nil {
		logger.Errorf(ctx, "AVRTMPContext: urlCtx == nil")
		return nil
	}
	return avcommon.WrapRTMPContext(urlCtx.PrivData())
}

func (c *ConnectionProxied) onInitFinishedRTMP(
	ctx context.Context,
) {
	routePath := c.GetRoutePath()
	rtmpCtx := c.AVRTMPContext(ctx)
	if rtmpCtx == nil {
		logger.Errorf(ctx, "onInitFinishedRTMP: rtmpCtx == nil")
		return
	}
	logger.Debugf(ctx, "updating the app name: '%s' -> '%s'", rtmpCtx.App(), routePath)
	rtmpCtx.SetApp(string(routePath))
}

// tryExtractRouteStringRTMP snoops a single client→server message for
// either a connect AMF (which carries the 'app' segment of the path)
// or a publish/play AMF (which carries the stream name — the last
// path segment). Returns:
//
//   - (non-nil, nil) once both halves are known and the merged route
//     path is available. This can happen from a publisher connect AMF
//     when the app already carries the full slash-delimited route, or
//     later from a publish/play AMF after app + stream are merged.
//     The negotiate loop will then bind libav to this path and dispatch serve.
//   - (nil, nil) when the message contains a connect (app stashed for
//     a later read), or contains nothing relevant — the caller forwards
//     and keeps reading.
//   - (nil, err) on a malformed AMF header.
func (c *ConnectionProxied) tryExtractRouteStringRTMP(
	ctx context.Context,
	msg []byte,
) (*RoutePath, error) {
	logger.Tracef(ctx, "msg: %X", msg)

	// A single TCP read MAY carry both connect and a follow-up
	// command (rare in practice but legal); handle both halves in
	// order.
	if bytes.Contains(msg, rtmpConnectMagic) {
		app, err := rtmpParseConnectApp(ctx, msg)
		if err != nil {
			return nil, fmt.Errorf("unable to parse the 'app' from the 'connect' AMF: %w", err)
		}
		c.ProtocolProperties[rtmpPropPendingApp] = app
		logger.Debugf(ctx, "stashed pending RTMP app from connect: %q", app)
		if c.Mode() == PortModePublishers {
			if routePath, ok := rtmpRoutePathFromPublisherConnectApp(app); ok {
				logger.Debugf(ctx, "RTMP publisher route path resolved from connect app: app=%q -> %q", app, routePath)
				rp := RoutePath(routePath)
				return &rp, nil
			}
		}
	}

	streamName, ok := rtmpParseStreamName(ctx, msg)
	if !ok {
		return nil, nil
	}

	app, _ := c.ProtocolProperties[rtmpPropPendingApp].(string)
	merged := rtmpMergeAppAndStream(app, streamName)
	logger.Debugf(ctx, "RTMP route path resolved: app=%q stream=%q -> %q", app, streamName, merged)
	rp := RoutePath(merged)
	return &rp, nil
}

// rtmpMergeAppAndStream combines the app field from the connect AMF
// with the stream name from the publish/play AMF. ffmpeg URLs of the
// form rtmp://host/<segments...>/<name> arrive as
// app=<segments...>, streamName=<name>; the canonical route is the
// concatenation. Two compatibility cases require care:
//
//  1. Some publishers place the entire path in 'app' and use a trailing
//     slash on the URL. ffmpeg then emits an empty stream name, which
//     must not be appended.
//  2. Some clients re-emit the last segment of app as the stream name
//     (defensive duplication). Detect by trailing match and skip the
//     append in that case.
func rtmpMergeAppAndStream(app, streamName string) string {
	app = strings.TrimRight(app, "/")
	if streamName == "" {
		return app
	}
	if app == "" {
		return streamName
	}
	// Avoid double-appending when the publisher echoed the trailing
	// segment of app as the stream name.
	if app == streamName || strings.HasSuffix(app, "/"+streamName) {
		return app
	}
	return app + "/" + streamName
}

func rtmpRoutePathFromPublisherConnectApp(app string) (string, bool) {
	routePath := strings.Trim(app, "/")
	switch {
	case routePath == "":
		return "", false
	case !strings.Contains(routePath, "/"):
		return "", false
	default:
		return routePath, true
	}
}

func (c *ConnectionProxied) correctMessageRTMP(
	ctx context.Context,
	msg []byte,
) ([]byte, error) {
	return msg, nil
}

var (
	rtmpConnectMagic []byte
	rtmpPublishMagic = []byte{0x02, 0x00, 0x07, 'p', 'u', 'b', 'l', 'i', 's', 'h'}
	rtmpPlayMagic    = []byte{0x02, 0x00, 0x04, 'p', 'l', 'a', 'y'}
)

func init() {
	/* An example of a packet:
	00000000  03 00 00 00 00 00 91 14  00 00 00 00 02 00 07 63  |...............c|
	00000010  6f 6e 6e 65 63 74 00 3f  f0 00 00 00 00 00 00 03  |onnect.?........|
	00000020  00 03 61 70 70 02 00 07  74 65 73 74 41 70 70 00  |..app...testApp.|
	*/
	rtmpConnectMagic = append(rtmpConnectMagic, []byte{0x02, 0x00, 0x07}...)
	rtmpConnectMagic = append(rtmpConnectMagic, []byte("connect\000")...)
	rtmpConnectMagic = append(rtmpConnectMagic, []byte{0x3f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03}...)
}

// rtmpParseConnectApp scans a buffered RTMP message for the connect
// command's 'app' object property and returns its string value. The
// scan walks the AMF0 object that follows the connect command name +
// transaction-id-1.0, decoding key/value pairs until it finds "app".
//
// Non-string property values (numbers, booleans, null, undefined) that
// appear before "app" are skipped via rtmpSkipAMF0Value rather than
// erroring out — real ffmpeg connect AMFs frequently emit numeric
// properties (e.g. objectEncoding, audioCodecs, videoCodecs) and a
// strict string-only walker would short-circuit before reaching "app"
// when those properties precede it. Unsupported AMF0 types still cause
// a parse error so that the failure is loud rather than silent.
func rtmpParseConnectApp(
	ctx context.Context,
	msg []byte,
) (_ret string, _err error) {
	logger.Tracef(ctx, "rtmpParseConnectApp(ctx, %X)", msg)
	defer func() { logger.Tracef(ctx, "/rtmpParseConnectApp: %q %v", _ret, _err) }()

	connectMagicIdx := bytes.Index(msg, rtmpConnectMagic)
	if connectMagicIdx < 0 {
		return "", fmt.Errorf("internal error: the 'connect' magic was not found")
	}

	idx := connectMagicIdx + len(rtmpConnectMagic)
	for {
		if idx+2 > len(msg) {
			return "", fmt.Errorf("the message was too short: cannot get the length of the key: %d > %d", idx+2, len(msg))
		}
		keyLen := binary.BigEndian.Uint16(msg[idx:])
		// AMF0 object end-marker: 16-bit zero-length key followed by
		// the 0x09 type tag. Stop walking — "app" was not present in
		// the connect object (treat as a parse error so the negotiate
		// loop can fall back rather than wire a path of "").
		if keyLen == 0 && idx+3 <= len(msg) && msg[idx+2] == 0x09 {
			return "", fmt.Errorf("'app' property not found in connect AMF object")
		}
		idx += 2

		if idx+int(keyLen) > len(msg) {
			return "", fmt.Errorf("the message was too short: cannot get the key: %d > %d", idx+int(keyLen), len(msg))
		}
		key := string(msg[idx : idx+int(keyLen)])
		idx += int(keyLen)

		if idx+1 > len(msg) {
			return "", fmt.Errorf("the message was too short: cannot get the the value type: %d > %d (key: '%s')", idx+1, len(msg), key)
		}
		valueType := msg[idx]

		if valueType != 0x02 {
			// Non-string property: skip to next pair. Unsupported
			// types still surface as an error from rtmpSkipAMF0Value.
			next, ok := rtmpSkipAMF0Value(msg, idx)
			if !ok {
				return "", fmt.Errorf("unsupported or truncated AMF0 type 0x%02x for connect property %q", valueType, key)
			}
			logger.Debugf(ctx, "skipped non-string connect property %q (type 0x%02x)", key, valueType)
			idx = next
			continue
		}
		idx++

		if idx+2 > len(msg) {
			return "", fmt.Errorf("the message was too short: cannot get the length of the value: %d > %d (key: '%s')", idx+2, len(msg), key)
		}
		valueLen := binary.BigEndian.Uint16(msg[idx:])
		idx += 2

		if idx+int(valueLen) > len(msg) {
			return "", fmt.Errorf("the message was too short: cannot get the value: %d > %d (key: '%s')", idx+int(valueLen), len(msg), key)
		}
		value := msg[idx : idx+int(valueLen)]
		idx += int(valueLen)

		logger.Debugf(ctx, "'%s': '%s'", key, value)
		if key == "app" {
			return string(value), nil
		}
	}
}

// rtmpParseStreamName scans a buffered RTMP message for a publish or
// play AMF command and returns the stream-name argument (the third
// AMF argument after the command name + transaction-id + null).
//
// The scan ignores releaseStream and FCPublish — those carry the
// stream name as well, but they are emitted BEFORE createStream and
// would lock in a route path while libav still has not opened its
// underlying RTMP server context. Snooping publish/play (which the
// client sends only after createStream succeeds) keeps the timing
// aligned with libav's existing OnPostOpen / OnOpened callbacks.
//
// Returns (streamName, true) on success, ("", false) when no
// publish/play marker was found in msg.
func rtmpParseStreamName(
	ctx context.Context,
	msg []byte,
) (string, bool) {
	publishIdx := bytes.Index(msg, rtmpPublishMagic)
	playIdx := bytes.Index(msg, rtmpPlayMagic)
	// publish wins if both are present (they are mutually exclusive
	// in practice; this is defensive).
	idx := -1
	switch {
	case publishIdx >= 0:
		idx = publishIdx + len(rtmpPublishMagic)
	case playIdx >= 0:
		idx = playIdx + len(rtmpPlayMagic)
	default:
		return "", false
	}
	logger.Debugf(ctx, "rtmpParseStreamName: command found at %d", idx)

	// Layout after the command name string:
	//   AMF0 number (1 + 8 bytes) — transaction id
	//   AMF0 null   (1 byte)      — command-info object placeholder
	//   AMF0 string (1 + 2 + N)   — stream name (the value we want)
	//
	// We tolerate the command-info slot being either null (0x05) or
	// a small object/undefined value as some encoders emit; we walk
	// AMF0 type-tags conservatively until we land on the next string.
	if idx+9 > len(msg) {
		return "", false
	}
	if msg[idx] != 0x00 {
		return "", false
	}
	idx += 9 // type byte + 8 IEEE754 double

	// Skip a null/undefined/object placeholder. Only support common
	// shapes; bail out otherwise.
	for idx < len(msg) {
		switch msg[idx] {
		case 0x05, 0x06: // null, undefined
			idx++
		case 0x03: // object — walk to end-marker
			idx++
			ok := false
			for idx+3 <= len(msg) {
				keyLen := binary.BigEndian.Uint16(msg[idx:])
				if keyLen == 0 && msg[idx+2] == 0x09 {
					idx += 3
					ok = true
					break
				}
				idx += 2
				if idx+int(keyLen) > len(msg) {
					return "", false
				}
				idx += int(keyLen)
				skipped, ok2 := rtmpSkipAMF0Value(msg, idx)
				if !ok2 {
					return "", false
				}
				idx = skipped
			}
			if !ok {
				return "", false
			}
		case 0x02: // string — this is the stream name we want
			if idx+3 > len(msg) {
				return "", false
			}
			sLen := binary.BigEndian.Uint16(msg[idx+1:])
			if idx+3+int(sLen) > len(msg) {
				return "", false
			}
			return string(msg[idx+3 : idx+3+int(sLen)]), true
		default:
			return "", false
		}
	}
	return "", false
}

// rtmpSkipAMF0Value advances idx past a single AMF0 value starting at
// msg[idx]. Returns (newIdx, true) on success or (_, false) on a
// truncated or unsupported type. Only the AMF0 types we actually see
// inside RTMP command objects are supported.
func rtmpSkipAMF0Value(msg []byte, idx int) (int, bool) {
	if idx >= len(msg) {
		return 0, false
	}
	switch msg[idx] {
	case 0x00: // number
		if idx+9 > len(msg) {
			return 0, false
		}
		return idx + 9, true
	case 0x01: // boolean
		if idx+2 > len(msg) {
			return 0, false
		}
		return idx + 2, true
	case 0x02: // string
		if idx+3 > len(msg) {
			return 0, false
		}
		sLen := binary.BigEndian.Uint16(msg[idx+1:])
		if idx+3+int(sLen) > len(msg) {
			return 0, false
		}
		return idx + 3 + int(sLen), true
	case 0x05, 0x06: // null, undefined
		return idx + 1, true
	default:
		return 0, false
	}
}
