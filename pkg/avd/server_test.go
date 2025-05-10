package avd

import (
	"context"
	"fmt"
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
	"github.com/xaionaro-go/avpipeline"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/processor"
	"github.com/xaionaro-go/observability"
	"github.com/xaionaro-go/secret"
)

func TestServerRTMP(t *testing.T) {
	loggerLevel := logger.LevelTrace

	runtime.DefaultCallerPCFilter = observability.CallerPCFilter(runtime.DefaultCallerPCFilter)
	l := logrus.Default().WithLevel(loggerLevel)
	ctx := logger.CtxWithLogger(context.Background(), l)
	logger.Default = func() logger.Logger {
		return l
	}
	defer belt.Flush(ctx)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s := NewServer(ctx)
	_, err = s.Listen(ctx, listener, ProtocolRTMP, PortModePublishers)
	require.NoError(t, err)

	pushTestFLVTo(ctx, t, fmt.Sprintf("rtmp://%s/testApp/testKey", listener.Addr()))
}

func pushTestFLVTo(
	ctx context.Context,
	t *testing.T,
	dstAddr string,
) {
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
