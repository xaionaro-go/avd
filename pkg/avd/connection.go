package avd

import (
	"context"
	"fmt"
	"net"

	"github.com/xaionaro-go/avpipeline/node"
)

type Connection interface {
	fmt.Stringer
	Close(ctx context.Context) error
	GetRawConn(ctx context.Context) net.Conn
	GetNode(ctx context.Context) node.Abstract
}

type Connections []Connection
