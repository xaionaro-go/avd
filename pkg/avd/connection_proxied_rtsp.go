package avd

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avcommon"
)

const (
	connectionProxiedCorrectRTSPURL = false
)

func (c *ConnectionProxied[N]) AVRTSPState() *avcommon.RTSPState {
	return avcommon.WrapRTSPState(c.AVFormatContext().PrivData())
}

func (c *ConnectionProxied[N]) onInitFinishedRTSP(
	ctx context.Context,
) {
	logger.Debugf(ctx, "onInitFinishedRTSP")
	if !connectionProxiedCorrectRTSPURL {
		return
	}
	routePath := c.GetRoutePath()
	rtspState := c.AVRTSPState()
	logger.Debugf(ctx, "updating the control URI: '%s' -> '%s'", rtspState.ControlURI(), routePath)
	rtspState.SetControlURI(string(routePath))
	for idx, stream := range rtspState.RTSPStreams() {
		logger.Debugf(ctx, "updating the control URL in stream #%d: '%s' -> '%s'", idx, stream.ControlURL(), routePath)
		stream.SetControlURL(string(routePath))
	}
}

func (c *ConnectionProxied[N]) tryExtractRouteStringRTSP(
	_ context.Context,
	msg []byte,
) (*RoutePath, error) {
	parts := bytes.SplitN(msg, []byte(" "), 3)
	if len(parts) < 3 {
		return nil, fmt.Errorf("expected the first packet to contain an 'OPTIONS' request, which consists of 3 parts and headers: OPTIONS URL protocol\\r\\nHeaders, but received '%s'", msg)
	}

	requestName, urlBytes, theRest := parts[0], parts[1], parts[2]
	_ = theRest

	if !bytes.Equal(bytes.ToUpper(requestName), []byte("OPTIONS")) {
		return nil, fmt.Errorf("expected the first packet to contain an 'OPTIONS' request, which consists of 3 parts and headers: OPTIONS URL protocol\\r\\nHeaders, but the first word is '%s'", parts[0])
	}

	url, err := url.Parse(string(urlBytes))
	if err != nil {
		return nil, fmt.Errorf("unable to parse '%s' as an URL: %w", urlBytes, err)
	}

	return ptr(RoutePath(strings.Trim(url.Path, "/"))), nil
}
