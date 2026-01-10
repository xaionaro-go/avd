// connection_proxied_rtsp.go provides RTSP-specific logic for proxied connections.

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
	connectionProxiedCorrectRTSPURL = true
)

func (c *ConnectionProxied) AVRTSPState(ctx context.Context) *avcommon.RTSPState {
	return avcommon.WrapRTSPState(c.AVFormatContext(ctx).PrivData())
}

func (c *ConnectionProxied) onInitFinishedRTSP(
	ctx context.Context,
) {
	logger.Debugf(ctx, "onInitFinishedRTSP")
	if !connectionProxiedCorrectRTSPURL {
		return
	}
	routePath := c.GetRoutePath()
	avFC := c.AVFormatContext(ctx)
	if avFC == nil {
		logger.Debugf(ctx, "AVFormatContext is nil, skipping onInitFinishedRTSP")
		return
	}
	rtspState := avcommon.WrapRTSPState(avFC.PrivData())
	logger.Debugf(ctx, "updating the control URI: '%s' -> '%s'", rtspState.ControlURI(), routePath)
	rtspState.SetControlURI(string(routePath))
}

func (c *ConnectionProxied) tryExtractRouteStringRTSP(
	ctx context.Context,
	msg []byte,
) (*RoutePath, error) {
	logger.Debugf(ctx, "tryExtractRouteStringRTSP: %q", string(msg))
	parts := bytes.SplitN(msg, []byte(" "), 3)
	if len(parts) < 3 {
		logger.Debugf(ctx, "tryExtractRouteStringRTSP: len(parts) < 3: %d", len(parts))
		return nil, nil
	}

	requestName := string(bytes.ToUpper(parts[0]))
	switch requestName {
	case "OPTIONS", "DESCRIBE", "ANNOUNCE", "SETUP", "PLAY", "RECORD", "PAUSE", "TEARDOWN", "SET_PARAMETER", "GET_PARAMETER":
		// valid RTSP methods
	default:
		return nil, nil
	}

	urlBytes := parts[1]
	url, err := url.Parse(string(urlBytes))
	if err != nil {
		return nil, fmt.Errorf("unable to parse '%s' as an URL: %w", urlBytes, err)
	}

	return ptr(RoutePath(strings.Trim(url.Path, "/"))), nil
}

func (c *ConnectionProxied) correctMessageRTSP(
	ctx context.Context,
	msg []byte,
) ([]byte, error) {
	logger.Warnf(ctx, "correctMessageRTSP was called; which is a very inefficient operation (TODO: get rid of it!)")
	if !connectionProxiedCorrectRTSPURL {
		return msg, nil
	}
	if c.AVInputURL == nil {
		return msg, nil
	}

	parts := bytes.SplitN(msg, []byte(" "), 3)
	if len(parts) < 3 {
		return msg, nil
	}

	requestName := string(bytes.ToUpper(parts[0]))
	switch requestName {
	case "OPTIONS", "DESCRIBE", "ANNOUNCE", "SETUP", "PLAY", "RECORD", "PAUSE", "TEARDOWN", "SET_PARAMETER", "GET_PARAMETER":
		// valid RTSP methods
	default:
		return msg, nil
	}

	oldURLBytes := parts[1]
	u, err := url.Parse(string(oldURLBytes))
	if err != nil {
		return msg, nil
	}

	u.Host = c.AVInputURL.Host
	u.Scheme = c.AVInputURL.Scheme

	newURL := u.String()
	newMsg := bytes.Join([][]byte{parts[0], []byte(newURL), parts[2]}, []byte(" "))
	logger.Debugf(ctx, "corrected RTSP message: %q -> %q", string(msg), string(newMsg))
	return newMsg, nil
}
