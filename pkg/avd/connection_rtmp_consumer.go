package avd

import (
	"context"
	"fmt"
	"net"

	"github.com/facebookincubator/go-belt/tool/logger"
)

type ConnectionRTMPConsumer struct {
}

func newConnectionRTMPConsumer(
	ctx context.Context,
	p *ListeningPortRTMPConsumers,
	conn net.Conn,
) (_ret *ConnectionRTMPConsumer, _err error) {
	return nil, fmt.Errorf("not implemented, yet")
}

func (c *ConnectionRTMPConsumer) Close(ctx context.Context) (_err error) {
	logger.Debugf(ctx, "Close()")
	defer logger.Debugf(ctx, "/Close(): %v", _err)
	return fmt.Errorf("not implemented, yet")
}
