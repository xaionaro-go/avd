package avd

import (
	"context"

	"github.com/asticode/go-astiav"
	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avpipeline"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/kernel/bitstreamfilter"
	packetcondition "github.com/xaionaro-go/avpipeline/packet/condition"
	"github.com/xaionaro-go/avpipeline/processor"
)

func tryNewBSFForMPEG2(
	ctx context.Context,
	videoCodecID astiav.CodecID,
	audioCodecID astiav.CodecID,
) (_ret *avpipeline.Node[*processor.FromKernel[*kernel.BitstreamFilter]]) {
	logger.Debugf(ctx, "tryNewBSFForMPEG2(ctx, '%s', '%s')", videoCodecID, audioCodecID)
	defer func() {
		logger.Debugf(ctx, "/tryNewBSFForMPEG2(ctx, '%s', '%s'): %v", videoCodecID, audioCodecID, _ret)
	}()

	recoderVideoBSFName := bitstreamfilter.NameMP4ToMP2(videoCodecID)
	if recoderVideoBSFName == bitstreamfilter.NameNull {
		return nil
	}

	bitstreamFilter, err := kernel.NewBitstreamFilter(ctx, map[packetcondition.Condition]bitstreamfilter.Name{
		packetcondition.MediaType(astiav.MediaTypeVideo): recoderVideoBSFName,
	})
	if err != nil {
		logger.Errorf(ctx, "unable to initialize the bitstream filter '%s': %w", recoderVideoBSFName, err)
		return nil
	}

	return avpipeline.NewNodeFromKernel(
		ctx,
		bitstreamFilter,
		processor.DefaultOptionsOutput()...,
	)
}

func tryNewBSFForMPEG4(
	ctx context.Context,
	videoCodecID astiav.CodecID,
	audioCodecID astiav.CodecID,
) (_ret *avpipeline.Node[*processor.FromKernel[*kernel.BitstreamFilter]]) {
	logger.Debugf(ctx, "tryNewBSFForMPEG4(ctx, '%s', '%s')", videoCodecID, audioCodecID)
	defer func() {
		logger.Debugf(ctx, "/tryNewBSFForMPEG4(ctx, '%s', '%s'): %v", videoCodecID, audioCodecID, _ret)
	}()

	recoderAudioBSFName := bitstreamfilter.NameMP2ToMP4(audioCodecID)
	if recoderAudioBSFName == bitstreamfilter.NameNull {
		return nil
	}

	bitstreamFilter, err := kernel.NewBitstreamFilter(ctx, map[packetcondition.Condition]bitstreamfilter.Name{
		packetcondition.MediaType(astiav.MediaTypeAudio): recoderAudioBSFName,
	})
	if err != nil {
		logger.Errorf(ctx, "unable to initialize the bitstream filter '%s': %w", recoderAudioBSFName, err)
		return nil
	}

	return avpipeline.NewNodeFromKernel(
		ctx,
		bitstreamFilter,
		processor.DefaultOptionsOutput()...,
	)
}
