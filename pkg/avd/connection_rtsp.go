package avd

import (
	"bytes"
	"context"
	"fmt"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avcommon"
)

func (c *Connection[N]) AVRTSPState() *avcommon.RTSPState {
	return avcommon.WrapRTSPState(c.AVFormatContext().PrivData())
}

func (c *Connection[N]) onInitFinishedRTSP(
	ctx context.Context,
) {
	routePath := c.GetRoutePath()
	rtspState := c.AVRTSPState()
	logger.Debugf(ctx, "updating the control URI: '%s' -> '%s'", rtspState.ControlURI(), routePath)
	rtspState.SetControlURI(string(routePath))
	for idx, stream := range rtspState.RTSPStreams() {
		logger.Debugf(ctx, "updating the control URL in stream #%d: '%s' -> '%s'", idx, stream.ControlURL(), routePath)
		stream.SetControlURL(string(routePath))
	}
}

func (c *Connection[N]) tryExtractRouteStringRTSP(
	_ context.Context,
	msg []byte,
) (*RoutePath, error) {
	parts := bytes.SplitN(msg, []byte(" "), 3)
	if len(parts) < 3 {
		return nil, fmt.Errorf("expected the first packet to contain an 'OPTIONS' request, which consists of 3 parts and headers: OPTIONS URL protocol\\r\\nHeaders, but received '%s'", msg)
	}

	requestName, url, theRest := parts[0], parts[1], parts[2]
	_ = theRest

	if !bytes.Equal(bytes.ToUpper(requestName), []byte("OPTIONS")) {
		return nil, fmt.Errorf("expected the first packet to contain an 'OPTIONS' request, which consists of 3 parts and headers: OPTIONS URL protocol\\r\\nHeaders, but the first word is '%s'", parts[0])
	}

	return ptr(RoutePath(url)), nil
}
