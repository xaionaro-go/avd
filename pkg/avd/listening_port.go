// listening_port.go defines the ListeningPort interface and related server methods.

package avd

import (
	"context"
	"fmt"
	"slices"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avpipeline/types"
	"github.com/xaionaro-go/xsync"
)

type ListeningPort interface {
	fmt.Stringer
	types.Closer
	GetServer() *Server
	GetMode() PortMode
	GetConfig() ListenConfig
	GetConnections(ctx context.Context) Connections
}

func (srv *Server) addListeningPort(ctx context.Context, port ListeningPort) {
	srv.ListeningPortsLocker.Do(ctx, func() {
		srv.ListeningPorts = append(srv.ListeningPorts, port)
		logger.Debugf(ctx, "added listening port: %s", port)
	})
}

func (srv *Server) removeListeningPort(ctx context.Context, port ListeningPort) {
	// TODO: optimize it to something better than O(n)
	srv.ListeningPortsLocker.Do(ctx, func() {
		for i, p := range srv.ListeningPorts {
			if p == port {
				srv.ListeningPorts = append(srv.ListeningPorts[:i], srv.ListeningPorts[i+1:]...)
				logger.Debugf(ctx, "removed listening port: %s", port)
				break
			}
		}
	})
}

func (srv *Server) GetListeningPorts(ctx context.Context) []ListeningPort {
	return xsync.DoA1R1(ctx, &srv.ListeningPortsLocker, srv.getListeningPorts, ctx)
}

func (srv *Server) getListeningPorts(ctx context.Context) []ListeningPort {
	return slices.Clone(srv.ListeningPorts)
}
