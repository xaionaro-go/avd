// server_wirer.go bridges the EndpointResolver's endpointWirer
// interface to the concrete *avd.Server. Production code constructs
// one in ApplyConfig; the resolver calls Wire when it needs to
// materialize a pattern-rendered endpoint. Tests inject a fake
// instead so the resolver is exercisable without spinning a real
// server. Pattern-materialized routes are persistent (mirror literal-
// endpoint semantics — see EndpointResolver.Release); a route-
// removal counterpart will be re-introduced together with the
// explicit-eviction RPC (deferred).

package configapplier

import (
	"context"

	"github.com/xaionaro-go/avd/pkg/avd"
	"github.com/xaionaro-go/avd/pkg/avd/types"
	"github.com/xaionaro-go/avd/pkg/config"
)

// serverWirer is the production endpointWirer. It defers to
// wireEndpoint for the wiring side-effects (so the resolver-driven
// path runs the same code as the eager-startup path) and pushes any
// command hooks declared by a pattern-rendered endpoint into the
// shared commandHandler so on_publisher_added / on_consumer_added /
// etc. fire just like they do for literal endpoints.
type serverWirer struct {
	srv     *avd.Server
	cmdHdlr *commandHandler[avd.RouteCustomData]
}

func newServerWirer(
	srv *avd.Server,
	cmdHdlr *commandHandler[avd.RouteCustomData],
) *serverWirer {
	return &serverWirer{srv: srv, cmdHdlr: cmdHdlr}
}

func (w *serverWirer) Wire(
	ctx context.Context,
	path types.RoutePath,
	ep config.EndpointConfig,
) error {
	if err := wireEndpoint(ctx, w.srv, path, ep); err != nil {
		return err
	}
	if w.cmdHdlr != nil {
		w.cmdHdlr.RegisterEndpointCommands(ctx, path, ep)
	}
	return nil
}
