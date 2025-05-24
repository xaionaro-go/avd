package avd

import (
	"context"
	"fmt"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avd/pkg/avd/types"
)

type ListeningPortDirect interface {
	ListeningPort
}

func (s *Server) ListenDirect(
	ctx context.Context,
	portAddr PortAddress,
	protocol Protocol,
	mode types.PortMode,
	opts ...ListenOption,
) (_ret ListeningPortDirect, _err error) {
	logger.Debugf(ctx, "ListenDirect(ctx, '%s')", portAddr)
	defer func() { logger.Debugf(ctx, "/ListenDirect(ctx, '%s'): %v %v", portAddr, _ret, _err) }()

	var r ListeningPortDirect
	var err error
	switch mode {
	case PortModePublishers:
		r, err = s.ListenDirectPublishers(ctx, portAddr, protocol, opts...)
	case PortModeConsumers:
		r, err = s.ListenDirectConsumers(ctx, portAddr, protocol, opts...)
	default:
		return nil, fmt.Errorf("unknown port mode: '%s'", mode)
	}
	if err != nil {
		return nil, err
	}
	return r, nil
}
