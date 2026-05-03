//go:build with_cv
// +build with_cv

package configapplier

import (
	"context"
	"image"

	"github.com/xaionaro-go/avd/pkg/avd"
	"github.com/xaionaro-go/avd/pkg/config"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/kernel/cascadedata"
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

	var classifiers []kernel.ClassifierConfig
	if cfg.Faces {
		classifiers = append(classifiers, kernel.ClassifierConfig{
			Name:         "face",
			XML:          cascadedata.FaceFrontalDefault,
			ScaleFactor:  1.1,
			MinNeighbors: 3,
			MinSize:      image.Pt(30, 30),
		})
	}
	if cfg.Plates {
		classifiers = append(classifiers, kernel.ClassifierConfig{
			Name:         "plate",
			XML:          cascadedata.PlateRussian,
			ScaleFactor:  1.1,
			MinNeighbors: 3,
			MinSize:      image.Pt(60, 20),
		})
	}
	// Default to both face and plate detection when neither is specified.
	if len(classifiers) == 0 {
		classifiers = append(classifiers,
			kernel.ClassifierConfig{
				Name:         "face",
				XML:          cascadedata.FaceFrontalDefault,
				ScaleFactor:  1.1,
				MinNeighbors: 3,
				MinSize:      image.Pt(30, 30),
			},
			kernel.ClassifierConfig{
				Name:         "plate",
				XML:          cascadedata.PlateRussian,
				ScaleFactor:  1.1,
				MinNeighbors: 3,
				MinSize:      image.Pt(60, 20),
			},
		)
	}

	blurRadius := cfg.BlurRadius
	if blurRadius == 0 && cfg.PixelateBlockSize == 0 {
		blurRadius = 15
	}

	factory := func(ctx context.Context) (kernel.Abstract, error) {
		pb, err := kernel.NewPrivacyBlur(kernel.PrivacyBlurConfig{
			Classifiers: classifiers,
			BlurRadius:  blurRadius,
			BlockSize:   cfg.PixelateBlockSize,
		})
		if err != nil {
			return nil, err
		}
		// Wire the shared filter atomics into the kernel.
		pb.Enabled = &filter.Enabled
		pb.BlurRadius.Store(filter.GetBlurRadius())
		pb.PixelateBlockSize.Store(filter.PixelateBlockSize.Load())
		return pb, nil
	}

	return factory, filter
}
