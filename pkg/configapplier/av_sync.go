// av_sync.go provides the wiring helper that constructs a
// kernel.AVSync per-forwarding and exposes it as a PushTo Condition
// for the chain->Output edge. Server-side registration of the AVSync
// is the caller's responsibility — and is performed only after the
// forwarding has been successfully created — so the registry stays
// clean if AddRouteForwardingToRemote fails.

package configapplier

import (
	"context"

	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/kernel/avsynccondition"
	packetorframefiltercondition "github.com/xaionaro-go/avpipeline/node/filter/packetorframefilter/condition"
)

// newAVSyncForRemoteForwarding builds the per-forwarding AVSync and
// the slice of PushTo Conditions to install on the chain->Output edge.
//
// Returns no error: kernel.NewAVSync cannot fail today. If that
// changes upstream, this helper grows an error return then.
//
// Local forwardings have no kernel.Output edge to attach a Condition
// to and so do not call this helper.
//
// The AVSync is NOT registered on the server here — the caller calls
// srv.RegisterAVSyncFilter only after AddRouteForwardingToRemote
// succeeds, so a forwarding-creation failure does not leak an entry
// into the registry.
func newAVSyncForRemoteForwarding(
	ctx context.Context,
) (
	avSync *kernel.AVSync,
	conditions []packetorframefiltercondition.Condition,
) {
	avSync = kernel.NewAVSync(ctx)
	conditions = []packetorframefiltercondition.Condition{
		avsynccondition.New(avSync),
	}
	return avSync, conditions
}
