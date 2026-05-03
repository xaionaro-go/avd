// deblemish.go creates the deblemish kernel factory and filter for a forwarding.
package configapplier

import (
	"context"

	"github.com/xaionaro-go/avd/pkg/avd"
	"github.com/xaionaro-go/avd/pkg/config"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/kernel/deblemish"
	"github.com/xaionaro-go/avpipeline/router"
)

func newDeblemishFactory(
	cfg *config.DeblemishConfig,
) (router.FilterKernelFactory, *avd.DeblemishFilter) {
	if cfg == nil {
		return nil, nil
	}

	filter := &avd.DeblemishFilter{}
	filter.Enabled.Store(cfg.Enabled)
	filter.SetSigmaS(context.Background(), cfg.SigmaS)
	filter.SetSigmaR(context.Background(), cfg.SigmaR)
	filter.SetDiameter(context.Background(), int64(cfg.Diameter))

	factory := func(ctx context.Context) (kernel.Abstract, error) {
		d, err := deblemish.New(deblemish.Config{
			SigmaS:   filter.GetSigmaS(),
			SigmaR:   filter.GetSigmaR(),
			Diameter: int(filter.Diameter.Load()),
			FaceOnly: cfg.FaceOnly,
		})
		if err != nil {
			return nil, err
		}
		// Share Enabled by pointer so gRPC Set propagates immediately.
		d.Enabled = &filter.Enabled
		// Register kernel so SigmaS/SigmaR/Diameter changes propagate.
		filter.SetKernel(ctx, d)
		return d, nil
	}

	return factory, filter
}
