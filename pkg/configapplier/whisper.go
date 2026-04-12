// whisper.go creates the whisper kernel factory and control for a forwarding.
//
// Unlike deblemish/privacyblur which operate on decoded video frames without
// needing stream metadata, the whisper kernel requires codec parameters
// (sample format, sample rate, channel layout) and time base to construct its
// internal AVFilterGraph. These are only available once the transcoding chain
// starts processing audio frames, not at config-application time.
//
// To bridge this gap, the factory returns a lazyWhisperKernel that defers
// real kernel creation until the first audio frame arrives, at which point
// it extracts the required parameters from the frame's StreamInfo.
package configapplier

import (
	"context"
	"fmt"
	"sync"

	"github.com/asticode/go-astiav"
	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avd/pkg/avd"
	"github.com/xaionaro-go/avd/pkg/config"
	"github.com/xaionaro-go/avpipeline/helpers/closuresignaler"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/kernel/whisper"
	"github.com/xaionaro-go/avpipeline/packetorframe"
	"github.com/xaionaro-go/avpipeline/router"
	globaltypes "github.com/xaionaro-go/avpipeline/types"
)

func newWhisperFactory(
	cfg *config.WhisperConfig,
) (router.FilterKernelFactory, *avd.WhisperControl) {
	if cfg == nil {
		return nil, nil
	}

	control := &avd.WhisperControl{}
	control.Enabled.Store(cfg.Enabled)
	control.SetLanguage(cfg.Language)
	control.SetModel(cfg.Model)

	factory := func(ctx context.Context) (kernel.Abstract, error) {
		return &lazyWhisperKernel{
			ClosureSignaler: closuresignaler.New(),
			control:         control,
		}, nil
	}

	return factory, control
}

// lazyWhisperKernel defers creation of the real Whisper kernel until the first
// audio frame arrives with its StreamInfo (CodecParameters + TimeBase).
// Non-audio frames pass through immediately. If Enabled is false, all frames
// pass through.
type lazyWhisperKernel struct {
	*closuresignaler.ClosureSignaler
	control  *avd.WhisperControl
	initOnce sync.Once
	inner    *whisper.Whisper
	initErr  error
}

func (k *lazyWhisperKernel) GetObjectID() globaltypes.ObjectID {
	return globaltypes.GetObjectID(k)
}

func (k *lazyWhisperKernel) String() string {
	return fmt.Sprintf("LazyWhisper(lang=%s)", k.control.GetLanguage())
}

func (k *lazyWhisperKernel) SendInput(
	ctx context.Context,
	input packetorframe.InputUnion,
	outputCh chan<- packetorframe.OutputUnion,
) error {
	if !k.control.Enabled.Load() {
		outputCh <- input.CloneAsReferencedOutput()
		return nil
	}

	_, frameInput := input.Unwrap()
	if frameInput == nil {
		outputCh <- input.CloneAsReferencedOutput()
		return nil
	}

	if frameInput.GetMediaType() != astiav.MediaTypeAudio {
		outputCh <- input.CloneAsReferencedOutput()
		return nil
	}

	// Initialize the real kernel on first audio frame.
	k.initOnce.Do(func() {
		k.inner, k.initErr = k.createKernel(ctx, frameInput)
		if k.initErr != nil {
			logger.Errorf(ctx, "whisper kernel init failed: %v; passing audio through", k.initErr)
		}
	})
	if k.initErr != nil {
		outputCh <- input.CloneAsReferencedOutput()
		return nil
	}

	return k.inner.SendInput(ctx, input, outputCh)
}

type audioFrameInfo interface {
	GetStreamIndex() int
	GetTimeBase() astiav.Rational
	GetCodecParameters() *astiav.CodecParameters
}

func (k *lazyWhisperKernel) createKernel(
	ctx context.Context,
	frame audioFrameInfo,
) (*whisper.Whisper, error) {
	model := k.control.GetModel()
	if model == "" {
		return nil, fmt.Errorf("whisper model path is empty")
	}

	lang := k.control.GetLanguage()
	if lang == "" {
		lang = "auto"
	}

	cfg := whisper.DefaultWhisperConfig()
	cfg.Model = model
	cfg.Language = lang

	codecParams := frame.GetCodecParameters()
	if codecParams == nil {
		return nil, fmt.Errorf("audio frame has nil codec parameters")
	}

	w, err := whisper.New(
		ctx,
		cfg,
		frame.GetStreamIndex(),
		codecParams,
		frame.GetTimeBase(),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to create whisper kernel: %w", err)
	}

	logger.Debugf(ctx, "whisper kernel initialized: model=%s lang=%s streamIndex=%d",
		model, lang, frame.GetStreamIndex())
	return w, nil
}

func (k *lazyWhisperKernel) Close(ctx context.Context) error {
	k.ClosureSignaler.Close(ctx)
	if k.inner != nil {
		return k.inner.Close(ctx)
	}
	return nil
}

func (k *lazyWhisperKernel) Generate(
	ctx context.Context,
	outputCh chan<- packetorframe.OutputUnion,
) error {
	return nil
}
