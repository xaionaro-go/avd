package avd

import (
	"context"

	transcodertypes "github.com/xaionaro-go/avpipeline/chain/transcoderwithpassthrough/types"
	"github.com/xaionaro-go/avpipeline/node"
)

type StreamForwarder interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Source() *NodeRouting
	Destination() node.Abstract
}

func newStreamForwarder(
	ctx context.Context,
	src *NodeRouting,
	dst node.Abstract,
	recoderConfig *transcodertypes.RecoderConfig,
) (_ret StreamForwarder, _err error) {
	var fwd StreamForwarder
	var err error
	if recoderConfig == nil {
		fwd, err = newStreamForwarderCopy(ctx, src, dst)
	} else {
		fwd, err = newStreamForwarderRecoding(ctx, src, dst, recoderConfig)
	}
	if err != nil {
		return nil, err
	}
	return fwd, nil
}
