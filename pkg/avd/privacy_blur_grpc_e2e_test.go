package avd_test

import (
	"context"
	"net"
	"testing"

	"github.com/facebookincubator/go-belt"
	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/facebookincubator/go-belt/tool/logger/implementation/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xaionaro-go/avd/pkg/avd"
	"github.com/xaionaro-go/avd/pkg/management/grpc/client"
	"github.com/xaionaro-go/avd/pkg/management/grpc/proto/avdmanagementgrpc"
	grpcserver "github.com/xaionaro-go/avd/pkg/management/grpc/server"
	"github.com/xaionaro-go/avpipeline/router"
)

func testCtx() context.Context {
	l := logrus.Default().WithLevel(logger.LevelTrace)
	return logger.CtxWithLogger(context.Background(), l)
}

// startGRPCServer starts a gRPC management server backed by the given avd.Server.
// Returns the client address. The server is stopped when the test completes.
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

// TestPrivacyBlurGRPC_SetAndGet exercises the full gRPC round-trip:
// client → gRPC server → Backend (avd.Server) → PrivacyBlurFilter atomics → response.
func TestPrivacyBlurGRPC_SetAndGet(t *testing.T) {
	ctx := testCtx()
	defer belt.Flush(ctx)

	srv := avd.NewServer(ctx)
	defer srv.Close(ctx)

	key := avd.PrivacyBlurFilterKey{
		RoutePath:       router.RoutePath("live/stream"),
		ForwardingIndex: 0,
	}
	ctrl := &avd.PrivacyBlurFilter{}
	srv.RegisterPrivacyBlurFilter(ctx, key, ctrl)

	addr := startGRPCServer(t, ctx, srv)
	grpcClient := newGRPCClient(t, ctx, addr)

	// 1. Get defaults.
	resp, err := grpcClient.GetPrivacyBlur(ctx, &avdmanagementgrpc.GetPrivacyBlurRequest{
		RoutePath:       "live/stream",
		ForwardingIndex: 0,
	})
	require.NoError(t, err)
	assert.False(t, resp.Enabled)
	assert.Equal(t, float64(0), resp.BlurRadius)
	assert.Equal(t, int64(0), resp.PixelateBlockSize)

	// 2. Set all fields.
	enabled := true
	blurRadius := 25.5
	blockSize := int64(12)
	_, err = grpcClient.SetPrivacyBlur(ctx, &avdmanagementgrpc.SetPrivacyBlurRequest{
		RoutePath:         "live/stream",
		ForwardingIndex:   0,
		Enabled:           &enabled,
		BlurRadius:        &blurRadius,
		PixelateBlockSize: &blockSize,
	})
	require.NoError(t, err)

	// 3. Verify via Get over gRPC.
	resp, err = grpcClient.GetPrivacyBlur(ctx, &avdmanagementgrpc.GetPrivacyBlurRequest{
		RoutePath:       "live/stream",
		ForwardingIndex: 0,
	})
	require.NoError(t, err)
	assert.True(t, resp.Enabled)
	assert.Equal(t, 25.5, resp.BlurRadius)
	assert.Equal(t, int64(12), resp.PixelateBlockSize)

	// 4. Verify the underlying atomics were set via Server's exported method.
	gotEnabled, gotRadius, gotBlock, err := srv.GetPrivacyBlurState(ctx, key)
	require.NoError(t, err)
	assert.True(t, gotEnabled)
	assert.Equal(t, 25.5, gotRadius)
	assert.Equal(t, int64(12), gotBlock)
}

// TestPrivacyBlurGRPC_PartialUpdate verifies that setting only some fields
// leaves others unchanged.
func TestPrivacyBlurGRPC_PartialUpdate(t *testing.T) {
	ctx := testCtx()
	defer belt.Flush(ctx)

	srv := avd.NewServer(ctx)
	defer srv.Close(ctx)

	key := avd.PrivacyBlurFilterKey{
		RoutePath:       router.RoutePath("live/cam"),
		ForwardingIndex: 1,
	}
	ctrl := &avd.PrivacyBlurFilter{}
	ctrl.Enabled.Store(true)
	ctrl.SetBlurRadius(10.0)
	ctrl.PixelateBlockSize.Store(5)
	srv.RegisterPrivacyBlurFilter(ctx, key, ctrl)

	addr := startGRPCServer(t, ctx, srv)
	grpcClient := newGRPCClient(t, ctx, addr)

	// Update only blur radius via gRPC.
	newRadius := 50.0
	_, err := grpcClient.SetPrivacyBlur(ctx, &avdmanagementgrpc.SetPrivacyBlurRequest{
		RoutePath:       "live/cam",
		ForwardingIndex: 1,
		BlurRadius:      &newRadius,
	})
	require.NoError(t, err)

	// Verify: blur radius changed, others untouched.
	resp, err := grpcClient.GetPrivacyBlur(ctx, &avdmanagementgrpc.GetPrivacyBlurRequest{
		RoutePath:       "live/cam",
		ForwardingIndex: 1,
	})
	require.NoError(t, err)
	assert.True(t, resp.Enabled, "Enabled should be unchanged")
	assert.Equal(t, 50.0, resp.BlurRadius, "BlurRadius should be updated")
	assert.Equal(t, int64(5), resp.PixelateBlockSize, "PixelateBlockSize should be unchanged")
}

// TestPrivacyBlurGRPC_NotFound verifies that accessing a non-registered route
// returns an error through the gRPC layer.
func TestPrivacyBlurGRPC_NotFound(t *testing.T) {
	ctx := testCtx()
	defer belt.Flush(ctx)

	srv := avd.NewServer(ctx)
	defer srv.Close(ctx)

	addr := startGRPCServer(t, ctx, srv)
	grpcClient := newGRPCClient(t, ctx, addr)

	// Get on non-existent route.
	_, err := grpcClient.GetPrivacyBlur(ctx, &avdmanagementgrpc.GetPrivacyBlurRequest{
		RoutePath:       "nonexistent/stream",
		ForwardingIndex: 0,
	})
	assert.Error(t, err)

	// Set on non-existent route.
	enabled := true
	_, err = grpcClient.SetPrivacyBlur(ctx, &avdmanagementgrpc.SetPrivacyBlurRequest{
		RoutePath:       "nonexistent/stream",
		ForwardingIndex: 0,
		Enabled:         &enabled,
	})
	assert.Error(t, err)
}

// TestPrivacyBlurGRPC_DisableToggle verifies enabling then disabling blur
// via gRPC round-trips correctly.
func TestPrivacyBlurGRPC_DisableToggle(t *testing.T) {
	ctx := testCtx()
	defer belt.Flush(ctx)

	srv := avd.NewServer(ctx)
	defer srv.Close(ctx)

	key := avd.PrivacyBlurFilterKey{
		RoutePath:       router.RoutePath("toggle/test"),
		ForwardingIndex: 0,
	}
	ctrl := &avd.PrivacyBlurFilter{}
	srv.RegisterPrivacyBlurFilter(ctx, key, ctrl)

	addr := startGRPCServer(t, ctx, srv)
	grpcClient := newGRPCClient(t, ctx, addr)

	// Enable.
	enabled := true
	_, err := grpcClient.SetPrivacyBlur(ctx, &avdmanagementgrpc.SetPrivacyBlurRequest{
		RoutePath: "toggle/test",
		Enabled:   &enabled,
	})
	require.NoError(t, err)

	resp, err := grpcClient.GetPrivacyBlur(ctx, &avdmanagementgrpc.GetPrivacyBlurRequest{
		RoutePath: "toggle/test",
	})
	require.NoError(t, err)
	assert.True(t, resp.Enabled)

	// Disable.
	disabled := false
	_, err = grpcClient.SetPrivacyBlur(ctx, &avdmanagementgrpc.SetPrivacyBlurRequest{
		RoutePath: "toggle/test",
		Enabled:   &disabled,
	})
	require.NoError(t, err)

	resp, err = grpcClient.GetPrivacyBlur(ctx, &avdmanagementgrpc.GetPrivacyBlurRequest{
		RoutePath: "toggle/test",
	})
	require.NoError(t, err)
	assert.False(t, resp.Enabled)

	// Verify atomics directly.
	assert.False(t, ctrl.Enabled.Load())
}
