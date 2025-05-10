package avd

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avcommon"
)

func (c *Connection[N]) AVRTMPContext() *avcommon.RTMPContext {
	return avcommon.WrapRTMPContext(c.AVURLContext().PrivData())
}

func (c *Connection[N]) onInitFinishedRTMP(
	ctx context.Context,
) {
	routePath := c.GetRoutePath()
	rtmpCtx := c.AVRTMPContext()
	logger.Debugf(ctx, "updating the app name: '%s' -> '%s'", rtmpCtx.App(), routePath)
	rtmpCtx.SetApp(string(routePath))
}

func (c *Connection[N]) tryExtractRouteStringRTMP(
	ctx context.Context,
	msg []byte,
) (*RoutePath, error) {
	logger.Tracef(ctx, "msg: %X (suspect 'connect': %t)", msg, bytes.Contains(msg, []byte("connect")))
	if !bytes.Contains(msg, rtmpConnectMagic) {
		return nil, nil
	}

	routePath, err := rtmpParseRoutePath(ctx, msg)
	if err != nil {
		return nil, fmt.Errorf("unable to parse the route path from the 'connect' message: %w", err)
	}

	return ptr(RoutePath(routePath)), nil
}

var rtmpConnectMagic []byte

func init() {
	/* An example of a packet:
	00000000  03 00 00 00 00 00 91 14  00 00 00 00 02 00 07 63  |...............c|
	00000010  6f 6e 6e 65 63 74 00 3f  f0 00 00 00 00 00 00 03  |onnect.?........|
	00000020  00 03 61 70 70 02 00 07  74 65 73 74 41 70 70 00  |..app...testApp.|
	00000030  04 74 79 70 65 02 00 0a  6e 6f 6e 70 72 69 76 61  |.type...nonpriva|
	00000040  74 65 00 08 66 6c 61 73  68 56 65 72 02 00 23 46  |te..flashVer..#F|
	00000050  4d 4c 45 2f 33 2e 30 20  28 63 6f 6d 70 61 74 69  |MLE/3.0 (compati|
	00000060  62 6c 65 3b 20 4c 61 76  66 36 31 2e 37 2e 31 30  |ble; Lavf61.7.10|
	00000070  30 29 00 05 74 63 55 72  6c 02 00 1e 72 74 6d 70  |0)..tcUrl...rtmp|
	00000080  3a 2f 2f 31 32 37 2e 30  2e 30 2e 31 c3 3a 34 33  |://127.0.0.1.:43|
	00000090  36 34 35 2f 74 65 73 74  41 70 70 00 00 09        |645/testApp...|
	0000009e
	*/
	rtmpConnectMagic = append(rtmpConnectMagic, []byte{0x02, 0x00, 0x07}...)
	rtmpConnectMagic = append(rtmpConnectMagic, []byte("connect\000")...)
	rtmpConnectMagic = append(rtmpConnectMagic, []byte{0x3f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03}...)
}

func rtmpParseRoutePath(
	ctx context.Context,
	msg []byte,
) (_ret []byte, _err error) {
	logger.Tracef(ctx, "parseAppName(ctx, %X)", msg)
	defer func() { logger.Tracef(ctx, "/parseAppName: '%v' %v", string(_ret), _err) }()
	/*
			An example of a packet:

		00000000  03 00 00 00 00 00 8b 14  00 00 00 00 02 00 07 63  |...............c|
		00000010  6f 6e 6e 65 63 74 00 3f  f0 00 00 00 00 00 00 03  |onnect.?........|
		00000020  00 03 61 70 70 02 00 04  74 65 73 74 00 04 74 79  |..app...test..ty|
		00000030  70 65 02 00 0a 6e 6f 6e  70 72 69 76 61 74 65 00  |pe...nonprivate.|
		00000040  08 66 6c 61 73 68 56 65  72 02 00 23 46 4d 4c 45  |.flashVer..#FMLE|
		00000050  2f 33 2e 30 20 28 63 6f  6d 70 61 74 69 62 6c 65  |/3.0 (compatible|
		00000060  3b 20 4c 61 76 66 36 31  2e 37 2e 31 30 30 29 00  |; Lavf61.7.100).|
		00000070  05 74 63 55 72 6c 02 00  1b 72 74 6d 70 3a 2f 2f  |.tcUrl...rtmp://|
		00000080  31 32 37 2e 30 2e 30 2e  31 3a 34 32 c3 34 34 39  |127.0.0.1:42.449|
		00000090  2f 74 65 73 74 00 00 09                           |/test...|
	*/

	connectMagicIdx := bytes.Index(msg, rtmpConnectMagic)
	if connectMagicIdx < 0 {
		return nil, fmt.Errorf("internal error: the 'connect' magic was not found")
	}

	idx := connectMagicIdx + len(rtmpConnectMagic)
	for {
		if idx+2 >= len(msg) {
			return nil, fmt.Errorf("the message was too short: cannot get the length of the key: %d >= %d", idx+2, len(msg))
		}
		keyLen := binary.BigEndian.Uint16(msg[idx:])
		idx += 2

		if idx+int(keyLen) >= len(msg) {
			return nil, fmt.Errorf("the message was too short: cannot get the key: %d >= %d", idx+int(keyLen), len(msg))
		}
		key := string(msg[idx : idx+int(keyLen)])
		idx += int(keyLen)

		if idx+1 >= len(msg) {
			return nil, fmt.Errorf("the message was too short: cannot get the the value type: %d >= %d (key: '%s')", idx+1, len(msg), key)
		}
		valueType := msg[idx]
		idx++

		if valueType != 0x02 {
			return nil, fmt.Errorf("we currently support only string values, but received type ID %d (key: '%s')", valueType, key)
		}

		if idx+2 >= len(msg) {
			return nil, fmt.Errorf("the message was too short: cannot get the length of the value: %d >= %d (key: '%s')", idx+2, len(msg), key)
		}
		valueLen := binary.BigEndian.Uint16(msg[idx:])
		idx += 2

		if idx+int(valueLen) >= len(msg) {
			return nil, fmt.Errorf("the message was too short: cannot get the value: %d >= %d (key: '%s')", idx+int(valueLen), len(msg), key)
		}
		value := msg[idx : idx+int(valueLen)]
		idx += int(valueLen)

		logger.Debugf(ctx, "'%s': '%s'", key, value)
		switch key {
		case "app":
			return value, nil
		}
	}
}
