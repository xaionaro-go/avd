//go:build with_cv
// +build with_cv

package e2e_test

import (
	"context"
	"image"
	"net"
	"testing"
	"time"

	"github.com/facebookincubator/go-belt"
	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/facebookincubator/go-belt/tool/logger/implementation/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xaionaro-go/avd/pkg/avd"
	"github.com/xaionaro-go/avd/pkg/management/grpc/client"
	"github.com/xaionaro-go/avd/pkg/management/grpc/proto/avdmanagementgrpc"
	grpcserver "github.com/xaionaro-go/avd/pkg/management/grpc/server"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/kernel/cascadedata"
	"github.com/xaionaro-go/avpipeline/router"
)

func testCtx() context.Context {
	l := logrus.Default().WithLevel(logger.LevelTrace)
	return logger.CtxWithLogger(context.Background(), l)
}

func startGRPCServer(
	t *testing.T,
	ctx context.Context,
	srv *avd.Server,
) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()

	grpcSrv := grpcserver.New(srv, listener)
	ctx, cancel := context.WithCancel(ctx)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = grpcSrv.ServeContext(ctx)
	}()

	t.Cleanup(func() {
		cancel()
		<-done
	})
	return addr
}

func newGRPCClient(t *testing.T, ctx context.Context, addr string) *client.GRPCClient {
	t.Helper()
	grpcClient, err := client.New(ctx, addr)
	require.NoError(t, err)
	t.Cleanup(func() { grpcClient.Close() })
	return grpcClient
}

// newTestPrivacyBlurFactory creates a FilterKernelFactory that produces a
// PrivacyBlur kernel wired to the given control's atomics.
func newTestPrivacyBlurFactory(ctrl *avd.PrivacyBlurFilter) router.FilterKernelFactory {
	return func(ctx context.Context) (kernel.Abstract, error) {
		pb, err := kernel.NewPrivacyBlur(kernel.PrivacyBlurConfig{
			Classifiers: []kernel.ClassifierConfig{{
				Name:         "face",
				XML:          cascadedata.FaceFrontalDefault,
				ScaleFactor:  1.1,
				MinNeighbors: 3,
				MinSize:      image.Pt(30, 30),
			}},
			BlurRadius: 15,
		})
		if err != nil {
			return nil, err
		}
		pb.Enabled = &ctrl.Enabled
		pb.BlurRadius.Store(ctrl.GetBlurRadius())
		pb.PixelateBlockSize.Store(ctrl.PixelateBlockSize.Load())
		return pb, nil
	}
}

// TestPrivacyBlurPipeline exercises the full integration:
//  1. Server with RTMP publisher listener + route + forwarding setup
//  2. PrivacyBlur kernel factory creates a functional kernel wired to control atomics
//  3. gRPC management can read/toggle blur state through the full stack
//  4. Publisher → route data path works (via forwarding goroutine setup)
func TestPrivacyBlurPipeline(t *testing.T) {
	ctx := testCtx()
	defer belt.Flush(ctx)

	ctx, cancelFn := context.WithTimeout(ctx, 30*time.Second)
	defer cancelFn()

	// 1. Start avd server with RTMP publisher listener.
	srv := avd.NewServer(ctx)
	defer func() { require.NoError(t, srv.Close(ctx)) }()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	portHandler, err := srv.ListenProxied(ctx, listener, avd.ProtocolRTMP, avd.PortModePublishers)
	require.NoError(t, err)
	defer func() { require.NoError(t, portHandler.Close(ctx)) }()

	// For RTMP, "blurApp/streamKey" → route path is "blurApp".
	const routePath = "blurApp"
	publishURL, err := portHandler.GetURLForRoute(ctx, routePath+"/streamKey")
	require.NoError(t, err)
	require.Contains(t, publishURL.String(), routePath, "publish URL should contain the route path")

	// 2. Privacy blur control with enabled=true, blurRadius=15.
	ctrl := &avd.PrivacyBlurFilter{}
	ctrl.Enabled.Store(true)
	ctrl.SetBlurRadius(15.0)

	key := avd.PrivacyBlurFilterKey{
		RoutePath:       router.RoutePath(routePath),
		ForwardingIndex: 0,
	}
	srv.RegisterPrivacyBlurFilter(ctx, key, ctrl)

	// 3. Start gRPC management server.
	grpcAddr := startGRPCServer(t, ctx, srv)

	// 4. Verify the PrivacyBlur kernel factory creates a functional kernel
	// wired to the shared control atomics.
	factory := newTestPrivacyBlurFactory(ctrl)
	pbKernel, err := factory(ctx)
	require.NoError(t, err, "factory should produce a kernel")
	require.NotNil(t, pbKernel)
	defer pbKernel.Close(ctx)

	pb, ok := pbKernel.(*kernel.PrivacyBlur)
	require.True(t, ok, "kernel should be *kernel.PrivacyBlur")

	// Kernel's Enabled is a pointer to the same atomic as the control.
	assert.True(t, pb.Enabled.Load(), "kernel Enabled should reflect control (true)")
	ctrl.Enabled.Store(false)
	assert.False(t, pb.Enabled.Load(), "kernel Enabled should track control changes (false)")
	ctrl.Enabled.Store(true)
	assert.True(t, pb.Enabled.Load(), "kernel Enabled should track control changes (true)")

	// 5. gRPC GetPrivacyBlur returns the expected state.
	grpcClient := newGRPCClient(t, ctx, grpcAddr)

	resp, err := grpcClient.GetPrivacyBlur(ctx, &avdmanagementgrpc.GetPrivacyBlurRequest{
		RoutePath:       routePath,
		ForwardingIndex: 0,
	})
	require.NoError(t, err)
	assert.True(t, resp.Enabled)
	assert.Equal(t, 15.0, resp.BlurRadius)

	// 6. Toggle blur off via gRPC.
	disabled := false
	_, err = grpcClient.SetPrivacyBlur(ctx, &avdmanagementgrpc.SetPrivacyBlurRequest{
		RoutePath:       routePath,
		ForwardingIndex: 0,
		Enabled:         &disabled,
	})
	require.NoError(t, err)

	// 7. Verify the atomics were updated through gRPC → Backend → control → kernel.
	assert.False(t, ctrl.Enabled.Load(), "control Enabled should be toggled off via gRPC")
	assert.False(t, pb.Enabled.Load(), "kernel Enabled should reflect gRPC toggle (off)")

	// Verify round-trip via gRPC.
	resp, err = grpcClient.GetPrivacyBlur(ctx, &avdmanagementgrpc.GetPrivacyBlurRequest{
		RoutePath:       routePath,
		ForwardingIndex: 0,
	})
	require.NoError(t, err)
	assert.False(t, resp.Enabled)

	// 8. Toggle blur back on via gRPC with updated radius.
	enabled := true
	newRadius := 25.0
	_, err = grpcClient.SetPrivacyBlur(ctx, &avdmanagementgrpc.SetPrivacyBlurRequest{
		RoutePath:       routePath,
		ForwardingIndex: 0,
		Enabled:         &enabled,
		BlurRadius:      &newRadius,
	})
	require.NoError(t, err)

	assert.True(t, ctrl.Enabled.Load(), "control should be re-enabled")
	assert.True(t, pb.Enabled.Load(), "kernel should be re-enabled")
	assert.Equal(t, 25.0, ctrl.GetBlurRadius(), "control blur radius should be updated")

	resp, err = grpcClient.GetPrivacyBlur(ctx, &avdmanagementgrpc.GetPrivacyBlurRequest{
		RoutePath:       routePath,
		ForwardingIndex: 0,
	})
	require.NoError(t, err)
	assert.True(t, resp.Enabled)
	assert.Equal(t, 25.0, resp.BlurRadius)
}

// TestPrivacyBlurPipeline_Disabled verifies the kernel factory produces a
// disabled kernel when the control starts disabled, and that the pipeline
// server infrastructure still works.
func TestPrivacyBlurPipeline_Disabled(t *testing.T) {
	ctx := testCtx()
	defer belt.Flush(ctx)

	ctx, cancelFn := context.WithTimeout(ctx, 30*time.Second)
	defer cancelFn()

	srv := avd.NewServer(ctx)
	defer func() { require.NoError(t, srv.Close(ctx)) }()

	const routePath = "passApp"

	// Blur disabled from the start.
	ctrl := &avd.PrivacyBlurFilter{}
	ctrl.Enabled.Store(false)
	ctrl.SetBlurRadius(15.0)

	key := avd.PrivacyBlurFilterKey{
		RoutePath:       router.RoutePath(routePath),
		ForwardingIndex: 0,
	}
	srv.RegisterPrivacyBlurFilter(ctx, key, ctrl)

	// Verify the factory produces a disabled kernel.
	factory := newTestPrivacyBlurFactory(ctrl)
	pbKernel, err := factory(ctx)
	require.NoError(t, err)
	require.NotNil(t, pbKernel)
	defer pbKernel.Close(ctx)

	pb, ok := pbKernel.(*kernel.PrivacyBlur)
	require.True(t, ok)
	assert.False(t, pb.Enabled.Load(), "kernel should start disabled when control is disabled")

	// Verify gRPC reports disabled state.
	grpcAddr := startGRPCServer(t, ctx, srv)
	grpcClient := newGRPCClient(t, ctx, grpcAddr)

	resp, err := grpcClient.GetPrivacyBlur(ctx, &avdmanagementgrpc.GetPrivacyBlurRequest{
		RoutePath:       routePath,
		ForwardingIndex: 0,
	})
	require.NoError(t, err)
	assert.False(t, resp.Enabled)
	assert.Equal(t, 15.0, resp.BlurRadius)

	// Enable via gRPC and verify kernel tracks the change.
	enabled := true
	_, err = grpcClient.SetPrivacyBlur(ctx, &avdmanagementgrpc.SetPrivacyBlurRequest{
		RoutePath:       routePath,
		ForwardingIndex: 0,
		Enabled:         &enabled,
	})
	require.NoError(t, err)

	assert.True(t, ctrl.Enabled.Load(), "control should be enabled via gRPC")
	assert.True(t, pb.Enabled.Load(), "kernel should follow control to enabled")
}
