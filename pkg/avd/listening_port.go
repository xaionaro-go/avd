package avd

import (
	"fmt"

	"github.com/xaionaro-go/avpipeline/types"
)

type ListeningPort interface {
	fmt.Stringer
	types.Closer
	GetServer() *Server
	GetConfig() ListenConfig
}
