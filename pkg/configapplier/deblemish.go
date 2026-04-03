// deblemish.go creates the deblemish kernel factory and control for a forwarding.
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
) (router.FilterKernelFactory, *avd.DeblemishControl) {
	if cfg == nil {
		return nil, nil
	}

	control := &avd.DeblemishControl{}
	control.Enabled.Store(cfg.Enabled)
	control.SetSigmaS(context.Background(), cfg.SigmaS)
	control.SetSigmaR(context.Background(), cfg.SigmaR)
	control.SetDiameter(context.Background(), int64(cfg.Diameter))

	factory := func(ctx context.Context) (kernel.Abstract, error) {
		d, err := deblemish.New(deblemish.Config{
			SigmaS:   control.GetSigmaS(),
			SigmaR:   control.GetSigmaR(),
			Diameter: int(control.Diameter.Load()),
			FaceOnly: cfg.FaceOnly,
		})
		if err != nil {
			return nil, err
		}
		// Share Enabled by pointer so gRPC Set propagates immediately.
		d.Enabled = &control.Enabled
		// Register kernel so SigmaS/SigmaR/Diameter changes propagate.
		control.SetKernel(ctx, d)
		return d, nil
	}

	return factory, control
}
