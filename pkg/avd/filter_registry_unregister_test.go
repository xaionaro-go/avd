package avd

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/router"
)

const (
	testRoutePathX = router.RoutePath("test/x")
	testRoutePathY = router.RoutePath("test/y")
)

// --- Whisper ---

func TestServer_UnregisterWhisperFiltersByRoutePath_RemovesAllOnPath(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	srv.RegisterWhisperFilter(ctx, WhisperFilterKey{RoutePath: testRoutePathX, ForwardingIndex: 0}, &WhisperFilter{})
	srv.RegisterWhisperFilter(ctx, WhisperFilterKey{RoutePath: testRoutePathX, ForwardingIndex: 1}, &WhisperFilter{})
	srv.RegisterWhisperFilter(ctx, WhisperFilterKey{RoutePath: testRoutePathX, ForwardingIndex: 2}, &WhisperFilter{})

	require.NoError(t, srv.UnregisterWhisperFiltersByRoutePath(ctx, testRoutePathX))

	assert.Empty(t, srv.GetRegisteredWhisperKeys(ctx))
}

func TestServer_UnregisterWhisperFiltersByRoutePath_LeavesOtherPaths(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	keepKey := WhisperFilterKey{RoutePath: testRoutePathY, ForwardingIndex: 0}
	srv.RegisterWhisperFilter(ctx, WhisperFilterKey{RoutePath: testRoutePathX, ForwardingIndex: 0}, &WhisperFilter{})
	srv.RegisterWhisperFilter(ctx, WhisperFilterKey{RoutePath: testRoutePathX, ForwardingIndex: 1}, &WhisperFilter{})
	srv.RegisterWhisperFilter(ctx, keepKey, &WhisperFilter{})

	require.NoError(t, srv.UnregisterWhisperFiltersByRoutePath(ctx, testRoutePathX))

	got := srv.GetRegisteredWhisperKeys(ctx)
	assert.Equal(t, []WhisperFilterKey{keepKey}, got)
}

func TestServer_UnregisterWhisperFiltersByRoutePath_EmptyRegistry(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	require.NoError(t, srv.UnregisterWhisperFiltersByRoutePath(ctx, testRoutePathX))
	assert.Empty(t, srv.GetRegisteredWhisperKeys(ctx))
}

// --- PrivacyBlur ---

func TestServer_UnregisterPrivacyBlurFiltersByRoutePath_RemovesAllOnPath(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	srv.RegisterPrivacyBlurFilter(ctx, PrivacyBlurFilterKey{RoutePath: testRoutePathX, ForwardingIndex: 0}, &PrivacyBlurFilter{})
	srv.RegisterPrivacyBlurFilter(ctx, PrivacyBlurFilterKey{RoutePath: testRoutePathX, ForwardingIndex: 1}, &PrivacyBlurFilter{})

	require.NoError(t, srv.UnregisterPrivacyBlurFiltersByRoutePath(ctx, testRoutePathX))

	assert.Empty(t, srv.GetRegisteredPrivacyBlurKeys(ctx))
}

func TestServer_UnregisterPrivacyBlurFiltersByRoutePath_LeavesOtherPaths(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	keepKey := PrivacyBlurFilterKey{RoutePath: testRoutePathY, ForwardingIndex: 0}
	srv.RegisterPrivacyBlurFilter(ctx, PrivacyBlurFilterKey{RoutePath: testRoutePathX, ForwardingIndex: 0}, &PrivacyBlurFilter{})
	srv.RegisterPrivacyBlurFilter(ctx, keepKey, &PrivacyBlurFilter{})

	require.NoError(t, srv.UnregisterPrivacyBlurFiltersByRoutePath(ctx, testRoutePathX))

	got := srv.GetRegisteredPrivacyBlurKeys(ctx)
	assert.Equal(t, []PrivacyBlurFilterKey{keepKey}, got)
}

func TestServer_UnregisterPrivacyBlurFiltersByRoutePath_EmptyRegistry(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	require.NoError(t, srv.UnregisterPrivacyBlurFiltersByRoutePath(ctx, testRoutePathX))
	assert.Empty(t, srv.GetRegisteredPrivacyBlurKeys(ctx))
}

// --- Deblemish ---

func TestServer_UnregisterDeblemishFiltersByRoutePath_RemovesAllOnPath(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	srv.RegisterDeblemishFilter(ctx, DeblemishFilterKey{RoutePath: testRoutePathX, ForwardingIndex: 0}, &DeblemishFilter{})
	srv.RegisterDeblemishFilter(ctx, DeblemishFilterKey{RoutePath: testRoutePathX, ForwardingIndex: 1}, &DeblemishFilter{})

	require.NoError(t, srv.UnregisterDeblemishFiltersByRoutePath(ctx, testRoutePathX))

	assert.Empty(t, srv.GetRegisteredDeblemishKeys(ctx))
}

func TestServer_UnregisterDeblemishFiltersByRoutePath_LeavesOtherPaths(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	keepKey := DeblemishFilterKey{RoutePath: testRoutePathY, ForwardingIndex: 0}
	srv.RegisterDeblemishFilter(ctx, DeblemishFilterKey{RoutePath: testRoutePathX, ForwardingIndex: 0}, &DeblemishFilter{})
	srv.RegisterDeblemishFilter(ctx, keepKey, &DeblemishFilter{})

	require.NoError(t, srv.UnregisterDeblemishFiltersByRoutePath(ctx, testRoutePathX))

	got := srv.GetRegisteredDeblemishKeys(ctx)
	assert.Equal(t, []DeblemishFilterKey{keepKey}, got)
}

func TestServer_UnregisterDeblemishFiltersByRoutePath_EmptyRegistry(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	require.NoError(t, srv.UnregisterDeblemishFiltersByRoutePath(ctx, testRoutePathX))
	assert.Empty(t, srv.GetRegisteredDeblemishKeys(ctx))
}

// --- AVSync ---

func TestServer_UnregisterAVSyncFiltersByRoutePath_RemovesAllOnPath(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	srv.RegisterAVSyncFilter(ctx, AVSyncFilterKey{RoutePath: testRoutePathX, ForwardingIndex: 0}, kernel.NewAVSync(ctx))
	srv.RegisterAVSyncFilter(ctx, AVSyncFilterKey{RoutePath: testRoutePathX, ForwardingIndex: 1}, kernel.NewAVSync(ctx))

	require.NoError(t, srv.UnregisterAVSyncFiltersByRoutePath(ctx, testRoutePathX))

	assert.Empty(t, srv.GetRegisteredAVSyncKeys(ctx))
}

func TestServer_UnregisterAVSyncFiltersByRoutePath_LeavesOtherPaths(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	keepKey := AVSyncFilterKey{RoutePath: testRoutePathY, ForwardingIndex: 0}
	srv.RegisterAVSyncFilter(ctx, AVSyncFilterKey{RoutePath: testRoutePathX, ForwardingIndex: 0}, kernel.NewAVSync(ctx))
	srv.RegisterAVSyncFilter(ctx, keepKey, kernel.NewAVSync(ctx))

	require.NoError(t, srv.UnregisterAVSyncFiltersByRoutePath(ctx, testRoutePathX))

	got := srv.GetRegisteredAVSyncKeys(ctx)
	assert.Equal(t, []AVSyncFilterKey{keepKey}, got)
}

func TestServer_UnregisterAVSyncFiltersByRoutePath_EmptyRegistry(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	require.NoError(t, srv.UnregisterAVSyncFiltersByRoutePath(ctx, testRoutePathX))
	assert.Empty(t, srv.GetRegisteredAVSyncKeys(ctx))
}

// --- OnRouteRemoved integration ---

func TestOnRouteRemoved_ClearsAllFilterRegistries(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	path := router.RoutePath("integration/path")
	srv.RegisterWhisperFilter(ctx, WhisperFilterKey{RoutePath: path, ForwardingIndex: 0}, &WhisperFilter{})
	srv.RegisterPrivacyBlurFilter(ctx, PrivacyBlurFilterKey{RoutePath: path, ForwardingIndex: 0}, &PrivacyBlurFilter{})
	srv.RegisterDeblemishFilter(ctx, DeblemishFilterKey{RoutePath: path, ForwardingIndex: 0}, &DeblemishFilter{})
	srv.RegisterAVSyncFilter(ctx, AVSyncFilterKey{RoutePath: path, ForwardingIndex: 0}, kernel.NewAVSync(ctx))

	// Sanity: registries are populated.
	require.Len(t, srv.GetRegisteredWhisperKeys(ctx), 1)
	require.Len(t, srv.GetRegisteredPrivacyBlurKeys(ctx), 1)
	require.Len(t, srv.GetRegisteredDeblemishKeys(ctx), 1)
	require.Len(t, srv.GetRegisteredAVSyncKeys(ctx), 1)

	route := &router.Route[RouteCustomData]{Path: path}
	srv.OnRouteRemoved(ctx, route)

	assert.Empty(t, srv.GetRegisteredWhisperKeys(ctx), "OnRouteRemoved must clear whisper")
	assert.Empty(t, srv.GetRegisteredPrivacyBlurKeys(ctx), "OnRouteRemoved must clear privacy_blur")
	assert.Empty(t, srv.GetRegisteredDeblemishKeys(ctx), "OnRouteRemoved must clear deblemish")
	assert.Empty(t, srv.GetRegisteredAVSyncKeys(ctx), "OnRouteRemoved must clear av_sync")
}

func TestOnRouteRemoved_LeavesOtherRoutes(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(ctx)
	defer srv.Close(ctx)

	removed := router.RoutePath("integration/removed")
	kept := router.RoutePath("integration/kept")

	srv.RegisterWhisperFilter(ctx, WhisperFilterKey{RoutePath: removed, ForwardingIndex: 0}, &WhisperFilter{})
	keepKey := WhisperFilterKey{RoutePath: kept, ForwardingIndex: 0}
	srv.RegisterWhisperFilter(ctx, keepKey, &WhisperFilter{})

	srv.OnRouteRemoved(ctx, &router.Route[RouteCustomData]{Path: removed})

	got := srv.GetRegisteredWhisperKeys(ctx)
	assert.Equal(t, []WhisperFilterKey{keepKey}, got)
}
