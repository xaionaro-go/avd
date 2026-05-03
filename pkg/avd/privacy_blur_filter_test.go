package avd

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xaionaro-go/avpipeline/router"
)

func TestPrivacyBlurFilter_BlurRadiusRoundTrip(t *testing.T) {
	ctrl := &PrivacyBlurFilter{}

	for _, v := range []float64{0, 1.5, 15.0, 100.7, math.Pi, math.SmallestNonzeroFloat64} {
		ctrl.SetBlurRadius(v)
		got := ctrl.GetBlurRadius()
		assert.Equal(t, v, got, "blur radius round-trip failed for %v", v)
	}
}

func TestPrivacyBlurFilter_Defaults(t *testing.T) {
	ctrl := &PrivacyBlurFilter{}

	assert.False(t, ctrl.Enabled.Load(), "default Enabled should be false")
	assert.Equal(t, float64(0), ctrl.GetBlurRadius(), "default BlurRadius should be 0")
	assert.Equal(t, int64(0), ctrl.PixelateBlockSize.Load(), "default PixelateBlockSize should be 0")
}

func TestServer_RegisterAndGetPrivacyBlurFilter(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	key := PrivacyBlurFilterKey{
		RoutePath:       router.RoutePath("test/stream"),
		ForwardingIndex: 0,
	}
	ctrl := &PrivacyBlurFilter{}
	ctrl.Enabled.Store(true)
	ctrl.SetBlurRadius(20.5)

	srv.RegisterPrivacyBlurFilter(ctx, key, ctrl)

	got, err := srv.GetPrivacyBlurFilter(ctx, key)
	require.NoError(t, err)
	assert.Same(t, ctrl, got, "should return the same control instance")
	assert.True(t, got.Enabled.Load())
	assert.Equal(t, 20.5, got.GetBlurRadius())
}

func TestServer_GetPrivacyBlurFilter_NotFound(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	key := PrivacyBlurFilterKey{
		RoutePath:       router.RoutePath("nonexistent/stream"),
		ForwardingIndex: 99,
	}

	_, err := srv.GetPrivacyBlurFilter(ctx, key)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no privacy_blur filter")
}

func TestServer_RegisterPrivacyBlurFilter_NilIgnored(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	key := PrivacyBlurFilterKey{
		RoutePath:       router.RoutePath("test/stream"),
		ForwardingIndex: 0,
	}

	// Registering nil should be a no-op.
	srv.RegisterPrivacyBlurFilter(ctx, key, nil)

	_, err := srv.GetPrivacyBlurFilter(ctx, key)
	assert.Error(t, err, "nil registration should not create an entry")
}

func TestServer_SetPrivacyBlurState(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	key := PrivacyBlurFilterKey{
		RoutePath:       router.RoutePath("test/stream"),
		ForwardingIndex: 0,
	}
	ctrl := &PrivacyBlurFilter{}
	srv.RegisterPrivacyBlurFilter(ctx, key, ctrl)

	// Set all fields.
	enabled := true
	blurRadius := 25.0
	blockSize := int64(8)
	err := srv.SetPrivacyBlurState(ctx, key, &enabled, &blurRadius, &blockSize)
	require.NoError(t, err)

	assert.True(t, ctrl.Enabled.Load())
	assert.Equal(t, 25.0, ctrl.GetBlurRadius())
	assert.Equal(t, int64(8), ctrl.PixelateBlockSize.Load())
}

func TestServer_SetPrivacyBlurState_PartialUpdate(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	key := PrivacyBlurFilterKey{
		RoutePath:       router.RoutePath("test/stream"),
		ForwardingIndex: 0,
	}
	ctrl := &PrivacyBlurFilter{}
	ctrl.Enabled.Store(true)
	ctrl.SetBlurRadius(10.0)
	ctrl.PixelateBlockSize.Store(5)
	srv.RegisterPrivacyBlurFilter(ctx, key, ctrl)

	// Update only blur radius (nil for others = no change).
	newRadius := 30.0
	err := srv.SetPrivacyBlurState(ctx, key, nil, &newRadius, nil)
	require.NoError(t, err)

	assert.True(t, ctrl.Enabled.Load(), "Enabled should be unchanged")
	assert.Equal(t, 30.0, ctrl.GetBlurRadius(), "BlurRadius should be updated")
	assert.Equal(t, int64(5), ctrl.PixelateBlockSize.Load(), "PixelateBlockSize should be unchanged")
}

func TestServer_SetPrivacyBlurState_NotFound(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	key := PrivacyBlurFilterKey{
		RoutePath:       router.RoutePath("missing/stream"),
		ForwardingIndex: 0,
	}

	enabled := true
	err := srv.SetPrivacyBlurState(ctx, key, &enabled, nil, nil)
	assert.Error(t, err)
}

func TestServer_GetPrivacyBlurState(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	key := PrivacyBlurFilterKey{
		RoutePath:       router.RoutePath("test/stream"),
		ForwardingIndex: 1,
	}
	ctrl := &PrivacyBlurFilter{}
	ctrl.Enabled.Store(true)
	ctrl.SetBlurRadius(42.5)
	ctrl.PixelateBlockSize.Store(16)
	srv.RegisterPrivacyBlurFilter(ctx, key, ctrl)

	enabled, radius, blockSize, err := srv.GetPrivacyBlurState(ctx, key)
	require.NoError(t, err)
	assert.True(t, enabled)
	assert.Equal(t, 42.5, radius)
	assert.Equal(t, int64(16), blockSize)
}

func TestServer_GetPrivacyBlurState_NotFound(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	key := PrivacyBlurFilterKey{
		RoutePath:       router.RoutePath("missing"),
		ForwardingIndex: 0,
	}

	_, _, _, err := srv.GetPrivacyBlurState(ctx, key)
	assert.Error(t, err)
}

func TestServer_MultipleFilters(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	key1 := PrivacyBlurFilterKey{RoutePath: "stream/a", ForwardingIndex: 0}
	key2 := PrivacyBlurFilterKey{RoutePath: "stream/a", ForwardingIndex: 1}
	key3 := PrivacyBlurFilterKey{RoutePath: "stream/b", ForwardingIndex: 0}

	ctrl1 := &PrivacyBlurFilter{}
	ctrl2 := &PrivacyBlurFilter{}
	ctrl3 := &PrivacyBlurFilter{}

	ctrl1.SetBlurRadius(10)
	ctrl2.SetBlurRadius(20)
	ctrl3.SetBlurRadius(30)

	srv.RegisterPrivacyBlurFilter(ctx, key1, ctrl1)
	srv.RegisterPrivacyBlurFilter(ctx, key2, ctrl2)
	srv.RegisterPrivacyBlurFilter(ctx, key3, ctrl3)

	got1, err := srv.GetPrivacyBlurFilter(ctx, key1)
	require.NoError(t, err)
	assert.Equal(t, 10.0, got1.GetBlurRadius())

	got2, err := srv.GetPrivacyBlurFilter(ctx, key2)
	require.NoError(t, err)
	assert.Equal(t, 20.0, got2.GetBlurRadius())

	got3, err := srv.GetPrivacyBlurFilter(ctx, key3)
	require.NoError(t, err)
	assert.Equal(t, 30.0, got3.GetBlurRadius())
}
