package avd

import (
	"context"
	"fmt"
	"net"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avd/pkg/avd/types"
)

func (s *Server) ListenRTMP(
	ctx context.Context,
	listener net.Listener,
	mode types.RTMPMode,
	opts ...ListeningPortRTMPOption,
) (_ret *ListeningPortRTMP, _err error) {
	logger.Debugf(ctx, "ListenRTMP(ctx, '%s')", listener.Addr())
	defer func() { logger.Debugf(ctx, "/ListenRTMP(ctx, '%s'): %v %v", listener.Addr(), _ret, _err) }()

	var cfg ListeningPortRTMPConfig
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	result := &ListeningPortRTMP{
		Server:                s,
		Listener:              listener,
		Config:                cfg,
		ConnectionsPublishers: make(map[net.Addr]*ConnectionRTMP[*NodeInput]),
		ConnectionsConsumers:  make(map[net.Addr]*ConnectionRTMP[*NodeOutput]),
	}

	err := result.StartListening(ctx, mode)
	if err != nil {
		return nil, fmt.Errorf("unable to start listening: %w", err)
	}

	return result, nil
}
