package avd

import (
	"context"
	"testing"
	"time"

	"github.com/asticode/go-astiav"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/router"
)

func TestServer_RegisterAndGetAVSyncFilter(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	key := AVSyncFilterKey{
		RoutePath:       router.RoutePath("test/stream"),
		ForwardingIndex: 0,
	}
	avSync := kernel.NewAVSync(ctx)

	srv.RegisterAVSyncFilter(ctx, key, avSync)

	got, err := srv.GetAVSyncFilter(ctx, key)
	require.NoError(t, err)
	assert.Same(t, avSync, got, "should return the same AVSync instance")
}

func TestServer_GetAVSyncFilter_NotFound(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	key := AVSyncFilterKey{
		RoutePath:       router.RoutePath("nonexistent/stream"),
		ForwardingIndex: 99,
	}

	_, err := srv.GetAVSyncFilter(ctx, key)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no av_sync filter")
}

func TestServer_RegisterAVSyncFilter_NilIgnored(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	key := AVSyncFilterKey{
		RoutePath:       router.RoutePath("test/stream"),
		ForwardingIndex: 0,
	}

	// Registering nil should not create an entry.
	srv.RegisterAVSyncFilter(ctx, key, nil)

	_, err := srv.GetAVSyncFilter(ctx, key)
	assert.Error(t, err)
}

func TestServer_RegisterAVSyncFilter_KeyCollisionOverwrites(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	key := AVSyncFilterKey{
		RoutePath:       router.RoutePath("test/stream"),
		ForwardingIndex: 0,
	}
	first := kernel.NewAVSync(ctx)
	second := kernel.NewAVSync(ctx)
	require.NotSame(t, first, second)

	srv.RegisterAVSyncFilter(ctx, key, first)
	srv.RegisterAVSyncFilter(ctx, key, second)

	got, err := srv.GetAVSyncFilter(ctx, key)
	require.NoError(t, err)
	assert.Same(t, second, got, "collision must overwrite with the new instance")
}

func TestServer_GetRegisteredAVSyncKeys(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	keys := srv.GetRegisteredAVSyncKeys(ctx)
	assert.Empty(t, keys, "no entries registered yet")

	key1 := AVSyncFilterKey{RoutePath: "stream/a", ForwardingIndex: 0}
	key2 := AVSyncFilterKey{RoutePath: "stream/a", ForwardingIndex: 1}
	key3 := AVSyncFilterKey{RoutePath: "stream/b", ForwardingIndex: 0}
	srv.RegisterAVSyncFilter(ctx, key1, kernel.NewAVSync(ctx))
	srv.RegisterAVSyncFilter(ctx, key2, kernel.NewAVSync(ctx))
	srv.RegisterAVSyncFilter(ctx, key3, kernel.NewAVSync(ctx))

	keys = srv.GetRegisteredAVSyncKeys(ctx)
	assert.ElementsMatch(t, []AVSyncFilterKey{key1, key2, key3}, keys)
}

func TestServer_SetGetPTSShift_Success(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	key := AVSyncFilterKey{RoutePath: router.RoutePath("live/a"), ForwardingIndex: 0}
	srv.RegisterAVSyncFilter(ctx, key, kernel.NewAVSync(ctx))

	// Audio shift round-trip.
	require.NoError(t, srv.SetPTSShift(ctx, key, astiav.MediaTypeAudio, 100*time.Millisecond))
	got, err := srv.GetPTSShift(ctx, key, astiav.MediaTypeAudio)
	require.NoError(t, err)
	assert.Equal(t, 100*time.Millisecond, got)

	// Video unaffected.
	gotVideo, err := srv.GetPTSShift(ctx, key, astiav.MediaTypeVideo)
	require.NoError(t, err)
	assert.Equal(t, time.Duration(0), gotVideo)

	// Video shift round-trip (negative).
	require.NoError(t, srv.SetPTSShift(ctx, key, astiav.MediaTypeVideo, -50*time.Millisecond))
	gotVideo, err = srv.GetPTSShift(ctx, key, astiav.MediaTypeVideo)
	require.NoError(t, err)
	assert.Equal(t, -50*time.Millisecond, gotVideo)
}

func TestServer_SetPTSShift_RegistryMiss(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	missing := AVSyncFilterKey{RoutePath: router.RoutePath("nope"), ForwardingIndex: 7}
	err := srv.SetPTSShift(ctx, missing, astiav.MediaTypeAudio, time.Second)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no av_sync filter")
}

func TestServer_GetPTSShift_RegistryMiss(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	missing := AVSyncFilterKey{RoutePath: router.RoutePath("nope"), ForwardingIndex: 7}
	_, err := srv.GetPTSShift(ctx, missing, astiav.MediaTypeAudio)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no av_sync filter")
}

func TestServer_GetAVSyncDelta_NotObserved(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	key := AVSyncFilterKey{RoutePath: router.RoutePath("live/b"), ForwardingIndex: 0}
	srv.RegisterAVSyncFilter(ctx, key, kernel.NewAVSync(ctx))

	delta, observed, err := srv.GetAVSyncDelta(ctx, key)
	require.NoError(t, err)
	assert.False(t, observed)
	assert.Equal(t, time.Duration(0), delta)
}

func TestServer_GetAVSyncDelta_RegistryMiss(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	missing := AVSyncFilterKey{RoutePath: router.RoutePath("nope"), ForwardingIndex: 7}
	_, _, err := srv.GetAVSyncDelta(ctx, missing)
	assert.Error(t, err)
}

func TestServer_AutoTuneAVSync_NotObserved(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	key := AVSyncFilterKey{RoutePath: router.RoutePath("live/c"), ForwardingIndex: 0}
	srv.RegisterAVSyncFilter(ctx, key, kernel.NewAVSync(ctx))

	_, err := srv.AutoTuneAVSync(ctx, key)
	require.Error(t, err)
	assert.ErrorIs(t, err, kernel.ErrAVSyncNotObserved)
}

func TestServer_AutoTuneAVSync_RegistryMiss(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	missing := AVSyncFilterKey{RoutePath: router.RoutePath("nope"), ForwardingIndex: 7}
	_, err := srv.AutoTuneAVSync(ctx, missing)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no av_sync filter")
}
