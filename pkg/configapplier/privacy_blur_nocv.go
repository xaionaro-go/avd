//go:build !with_cv
// +build !with_cv

package configapplier

import (
	"context"
	"fmt"

	"github.com/xaionaro-go/avd/pkg/avd"
	"github.com/xaionaro-go/avd/pkg/config"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/router"
)

func newPrivacyBlurFactory(
	cfg *config.PrivacyBlurConfig,
) (router.FilterKernelFactory, *avd.PrivacyBlurFilter) {
	if cfg == nil {
		return nil, nil
	}

	filter := &avd.PrivacyBlurFilter{}
	filter.Enabled.Store(cfg.Enabled)
	filter.SetBlurRadius(cfg.BlurRadius)
	filter.PixelateBlockSize.Store(int64(cfg.PixelateBlockSize))

	return func(ctx context.Context) (kernel.Abstract, error) {
		return nil, fmt.Errorf("privacy blur requires the 'with_cv' build tag (OpenCV support)")
	}, filter
}
