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
) (router.FilterKernelFactory, *avd.PrivacyBlurControl) {
	if cfg == nil {
		return nil, nil
	}

	control := &avd.PrivacyBlurControl{}
	control.Enabled.Store(cfg.Enabled)
	control.SetBlurRadius(cfg.BlurRadius)
	control.PixelateBlockSize.Store(int64(cfg.PixelateBlockSize))

	return func(ctx context.Context) (kernel.Abstract, error) {
		return nil, fmt.Errorf("privacy blur requires the 'with_cv' build tag (OpenCV support)")
	}, control
}
