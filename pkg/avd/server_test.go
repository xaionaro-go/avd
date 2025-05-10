package avd

import (
	"context"
	"io"
	"net"
	"path"
	"sync"
	"testing"

	"github.com/facebookincubator/go-belt"
	"github.com/facebookincubator/go-belt/pkg/runtime"
	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/facebookincubator/go-belt/tool/logger/implementation/logrus"
	"github.com/stretchr/testify/require"
	"github.com/xaionaro-go/avd/pkg/avd/types"
	"github.com/xaionaro-go/avpipeline"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/processor"
	"github.com/xaionaro-go/observability"
	"github.com/xaionaro-go/secret"
	"github.com/xaionaro-go/xsync"
)

func TestServer(t *testing.T) {
	loggerLevel := logger.LevelTrace

	runtime.DefaultCallerPCFilter = observability.CallerPCFilter(runtime.DefaultCallerPCFilter)
	l := logrus.Default().WithLevel(loggerLevel)
	ctx := logger.CtxWithLogger(context.Background(), l)
	ctx = xsync.WithNoLogging(ctx, true)
	logger.Default = func() logger.Logger {
		return l
	}
	defer belt.Flush(ctx)

	for _, proto := range []types.Protocol{
		ProtocolRTMP,
	} {
		t.Run(proto.String(), func(t *testing.T) {
			ctx := belt.WithField(ctx, "protocol", proto.String())
			ctx, cancelFn := context.WithCancel(ctx)
			defer cancelFn()

			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer listener.Close()

			s := NewServer(ctx)
			defer func() { require.NoError(t, s.Close(ctx)) }()

			portHandler, err := s.Listen(ctx, listener, proto, PortModePublishers)
			require.NoError(t, err)
			defer func() { require.NoError(t, portHandler.Close(ctx)) }()

			url, err := portHandler.GetURLForRoute(ctx, "testApp/testKey")
			require.NoError(t, err)
			pushTestFileTo(ctx, t, url.String())
		})
	}
}

func pushTestFileTo(
	ctx context.Context,
	t *testing.T,
	dstAddr string,
) {
	logger.Debugf(ctx, "pushTestFileTo")
	defer func() { logger.Debugf(ctx, "/pushTestFileTo") }()

	var wg sync.WaitGroup
	defer wg.Wait()

	ctx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	recordingPath := path.Join("testdata", "video0-1v1a.mov")

	inputKernel, err := kernel.NewInputFromURL(ctx, recordingPath, secret.New(""), kernel.InputConfig{})
	require.NoError(t, err)

	inputNode := avpipeline.NewNodeFromKernel(ctx, inputKernel, processor.DefaultOptionsInput()...)

	outputKernel, err := kernel.NewOutputFromURL(ctx, dstAddr, secret.New(""), kernel.OutputConfig{})
	require.NoError(t, err)

	outputNode := avpipeline.NewNodeFromKernel(ctx, outputKernel, processor.DefaultOptionsOutput()...)

	inputNode.AddPushPacketsTo(outputNode)
	errCh := make(chan avpipeline.ErrNode, 100)

	wg.Add(1)
	observability.Go(ctx, func() {
		defer wg.Done()
		defer close(errCh)
		avpipeline.ServeRecursively(ctx, avpipeline.ServeConfig{}, errCh, inputNode)
	})

	select {
	case <-ctx.Done():
		return
	case err := <-errCh:
		require.ErrorIs(t, err, io.EOF)
	}
}
