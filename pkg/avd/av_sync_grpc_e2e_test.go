// av_sync_grpc_e2e_test.go exercises the gRPC round-trip for the four
// AVSync RPCs against a real avd.Server backend with a live AVSync
// kernel registered for a synthetic forwarding key.

package avd_test

import (
	"context"
	"testing"
	"time"

	"github.com/asticode/go-astiav"
	"github.com/facebookincubator/go-belt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/xaionaro-go/avd/pkg/avd"
	"github.com/xaionaro-go/avd/pkg/management/grpc/proto/avdmanagementgrpc"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/packet"
	"github.com/xaionaro-go/avpipeline/packetorframe"
	"github.com/xaionaro-go/avpipeline/router"
	globaltypes "github.com/xaionaro-go/avpipeline/types"
	"github.com/xaionaro-go/secret"
)

// avSyncObsHarness builds a kernel.Output with audio + video streams so
// the test can feed real packets into kernel.AVSync.ApplyAndObserve and
// flip the hasAudio/hasVideo flags. Returned cleanup must run before
// the test exits.
type avSyncObsHarness struct {
	ctx   context.Context
	out   *kernel.Output
	audio *astiav.Stream
	video *astiav.Stream
}

func newAVSyncObsHarness(t *testing.T, ctx context.Context) *avSyncObsHarness {
	t.Helper()
	out, err := kernel.NewOutputFromURL(ctx, "", secret.New(""), kernel.OutputConfig{
		CustomOptions: globaltypes.DictionaryItems{{
			Key:   "f",
			Value: "null",
		}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = out.Close(ctx) })

	audio := out.FormatContext.NewStream(astiav.FindEncoder(astiav.CodecIDAac))
	audio.SetIndex(0)
	audio.SetTimeBase(astiav.NewRational(1, 1000))
	audio.CodecParameters().SetMediaType(astiav.MediaTypeAudio)
	video := out.FormatContext.NewStream(astiav.FindEncoder(astiav.CodecIDH264))
	video.SetIndex(1)
	video.SetTimeBase(astiav.NewRational(1, 1000))
	video.CodecParameters().SetMediaType(astiav.MediaTypeVideo)

	return &avSyncObsHarness{
		ctx:   ctx,
		out:   out,
		audio: audio,
		video: video,
	}
}

func (h *avSyncObsHarness) feed(
	t *testing.T,
	k *kernel.AVSync,
	stream *astiav.Stream,
	pts int64,
) {
	t.Helper()
	pkt := packet.Pool.Get()
	pkt.SetStreamIndex(stream.Index())
	pkt.SetPts(pts)
	pkt.SetDts(pts)
	input := packet.BuildInput(pkt, &packet.StreamInfo{Stream: stream})
	u := packetorframe.InputUnion{Packet: &input}
	k.ApplyAndObserve(h.ctx, &u)
}

func TestAVSyncGRPC_SetGetPTSShift_Audio(t *testing.T) {
	ctx := testCtx()
	defer belt.Flush(ctx)

	srv := avd.NewServer(ctx)
	defer srv.Close(ctx)

	key := avd.AVSyncFilterKey{
		RoutePath:       router.RoutePath("live/sync"),
		ForwardingIndex: 0,
	}
	k := kernel.NewAVSync(ctx)
	srv.RegisterAVSyncFilter(ctx, key, k)

	addr := startGRPCServer(t, ctx, srv)
	grpcClient := newGRPCClient(t, ctx, addr)

	// SetPTSShift audio = 100ms.
	_, err := grpcClient.SetPTSShift(ctx, &avdmanagementgrpc.SetPTSShiftRequest{
		RoutePath:       "live/sync",
		ForwardingIndex: 0,
		MediaType:       avdmanagementgrpc.AVSyncMediaType_AV_SYNC_MEDIA_TYPE_AUDIO,
		ShiftNs:         int64(100 * time.Millisecond),
	})
	require.NoError(t, err)

	// GetPTSShift audio returns 100ms.
	resp, err := grpcClient.GetPTSShift(ctx, &avdmanagementgrpc.GetPTSShiftRequest{
		RoutePath:       "live/sync",
		ForwardingIndex: 0,
		MediaType:       avdmanagementgrpc.AVSyncMediaType_AV_SYNC_MEDIA_TYPE_AUDIO,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(100*time.Millisecond), resp.ShiftNs)

	// Video offset must remain zero (audio set must NOT bleed into video).
	respV, err := grpcClient.GetPTSShift(ctx, &avdmanagementgrpc.GetPTSShiftRequest{
		RoutePath:       "live/sync",
		ForwardingIndex: 0,
		MediaType:       avdmanagementgrpc.AVSyncMediaType_AV_SYNC_MEDIA_TYPE_VIDEO,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(0), respV.ShiftNs)
}

func TestAVSyncGRPC_SetGetPTSShift_Video_Negative(t *testing.T) {
	ctx := testCtx()
	defer belt.Flush(ctx)

	srv := avd.NewServer(ctx)
	defer srv.Close(ctx)

	key := avd.AVSyncFilterKey{
		RoutePath:       router.RoutePath("live/sync"),
		ForwardingIndex: 0,
	}
	srv.RegisterAVSyncFilter(ctx, key, kernel.NewAVSync(ctx))

	addr := startGRPCServer(t, ctx, srv)
	grpcClient := newGRPCClient(t, ctx, addr)

	_, err := grpcClient.SetPTSShift(ctx, &avdmanagementgrpc.SetPTSShiftRequest{
		RoutePath:       "live/sync",
		ForwardingIndex: 0,
		MediaType:       avdmanagementgrpc.AVSyncMediaType_AV_SYNC_MEDIA_TYPE_VIDEO,
		ShiftNs:         int64(-50 * time.Millisecond),
	})
	require.NoError(t, err)

	resp, err := grpcClient.GetPTSShift(ctx, &avdmanagementgrpc.GetPTSShiftRequest{
		RoutePath:       "live/sync",
		ForwardingIndex: 0,
		MediaType:       avdmanagementgrpc.AVSyncMediaType_AV_SYNC_MEDIA_TYPE_VIDEO,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(-50*time.Millisecond), resp.ShiftNs)
}

func TestAVSyncGRPC_SetPTSShift_Unspecified_InvalidArgument(t *testing.T) {
	ctx := testCtx()
	defer belt.Flush(ctx)

	srv := avd.NewServer(ctx)
	defer srv.Close(ctx)

	key := avd.AVSyncFilterKey{
		RoutePath:       router.RoutePath("live/sync"),
		ForwardingIndex: 0,
	}
	srv.RegisterAVSyncFilter(ctx, key, kernel.NewAVSync(ctx))

	addr := startGRPCServer(t, ctx, srv)
	grpcClient := newGRPCClient(t, ctx, addr)

	_, err := grpcClient.SetPTSShift(ctx, &avdmanagementgrpc.SetPTSShiftRequest{
		RoutePath:       "live/sync",
		ForwardingIndex: 0,
		// MediaType deliberately unspecified.
		ShiftNs: 1,
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())

	_, err = grpcClient.GetPTSShift(ctx, &avdmanagementgrpc.GetPTSShiftRequest{
		RoutePath:       "live/sync",
		ForwardingIndex: 0,
	})
	require.Error(t, err)
	st, ok = status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestAVSyncGRPC_SetPTSShift_NotFound(t *testing.T) {
	ctx := testCtx()
	defer belt.Flush(ctx)

	srv := avd.NewServer(ctx)
	defer srv.Close(ctx)

	addr := startGRPCServer(t, ctx, srv)
	grpcClient := newGRPCClient(t, ctx, addr)

	_, err := grpcClient.SetPTSShift(ctx, &avdmanagementgrpc.SetPTSShiftRequest{
		RoutePath:       "missing/route",
		ForwardingIndex: 0,
		MediaType:       avdmanagementgrpc.AVSyncMediaType_AV_SYNC_MEDIA_TYPE_AUDIO,
		ShiftNs:         1,
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.NotFound, st.Code())
}

func TestAVSyncGRPC_GetAVSyncDelta_NotObserved(t *testing.T) {
	ctx := testCtx()
	defer belt.Flush(ctx)

	srv := avd.NewServer(ctx)
	defer srv.Close(ctx)

	key := avd.AVSyncFilterKey{
		RoutePath:       router.RoutePath("live/sync"),
		ForwardingIndex: 0,
	}
	srv.RegisterAVSyncFilter(ctx, key, kernel.NewAVSync(ctx))

	addr := startGRPCServer(t, ctx, srv)
	grpcClient := newGRPCClient(t, ctx, addr)

	resp, err := grpcClient.GetAVSyncDelta(ctx, &avdmanagementgrpc.GetAVSyncDeltaRequest{
		RoutePath:       "live/sync",
		ForwardingIndex: 0,
	})
	require.NoError(t, err)
	assert.False(t, resp.Observed)
	assert.Equal(t, int64(0), resp.DeltaNs)
}

func TestAVSyncGRPC_GetAVSyncDelta_Observed(t *testing.T) {
	ctx := testCtx()
	defer belt.Flush(ctx)

	srv := avd.NewServer(ctx)
	defer srv.Close(ctx)

	key := avd.AVSyncFilterKey{
		RoutePath:       router.RoutePath("live/sync"),
		ForwardingIndex: 0,
	}
	k := kernel.NewAVSync(ctx)
	srv.RegisterAVSyncFilter(ctx, key, k)

	// Prime audio at 2500ms, video at 1000ms (1/1000 timebase) — delta = +1500ms.
	h := newAVSyncObsHarness(t, ctx)
	h.feed(t, k, h.audio, 2500)
	h.feed(t, k, h.video, 1000)

	addr := startGRPCServer(t, ctx, srv)
	grpcClient := newGRPCClient(t, ctx, addr)

	resp, err := grpcClient.GetAVSyncDelta(ctx, &avdmanagementgrpc.GetAVSyncDeltaRequest{
		RoutePath:       "live/sync",
		ForwardingIndex: 0,
	})
	require.NoError(t, err)
	assert.True(t, resp.Observed)
	assert.Equal(t, int64(1500*time.Millisecond), resp.DeltaNs)
}

func TestAVSyncGRPC_AutoTune_NoObservations_FailedPrecondition(t *testing.T) {
	ctx := testCtx()
	defer belt.Flush(ctx)

	srv := avd.NewServer(ctx)
	defer srv.Close(ctx)

	key := avd.AVSyncFilterKey{
		RoutePath:       router.RoutePath("live/sync"),
		ForwardingIndex: 0,
	}
	srv.RegisterAVSyncFilter(ctx, key, kernel.NewAVSync(ctx))

	addr := startGRPCServer(t, ctx, srv)
	grpcClient := newGRPCClient(t, ctx, addr)

	_, err := grpcClient.AutoTuneAVSync(ctx, &avdmanagementgrpc.AutoTuneAVSyncRequest{
		RoutePath:       "live/sync",
		ForwardingIndex: 0,
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
}

func TestAVSyncGRPC_AutoTune_NegativeDelta_FailedPrecondition(t *testing.T) {
	ctx := testCtx()
	defer belt.Flush(ctx)

	srv := avd.NewServer(ctx)
	defer srv.Close(ctx)

	key := avd.AVSyncFilterKey{
		RoutePath:       router.RoutePath("live/sync"),
		ForwardingIndex: 0,
	}
	k := kernel.NewAVSync(ctx)
	srv.RegisterAVSyncFilter(ctx, key, k)

	// Audio behind video — delta < 0 — AutoTune cannot zero without a
	// backward shift, so kernel.ErrAVSyncVideoLeadsAudio.
	h := newAVSyncObsHarness(t, ctx)
	h.feed(t, k, h.audio, 1000)
	h.feed(t, k, h.video, 2500)

	addr := startGRPCServer(t, ctx, srv)
	grpcClient := newGRPCClient(t, ctx, addr)

	_, err := grpcClient.AutoTuneAVSync(ctx, &avdmanagementgrpc.AutoTuneAVSyncRequest{
		RoutePath:       "live/sync",
		ForwardingIndex: 0,
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
}

func TestAVSyncGRPC_AutoTune_PositiveDelta_AppliesToVideo(t *testing.T) {
	ctx := testCtx()
	defer belt.Flush(ctx)

	srv := avd.NewServer(ctx)
	defer srv.Close(ctx)

	key := avd.AVSyncFilterKey{
		RoutePath:       router.RoutePath("live/sync"),
		ForwardingIndex: 0,
	}
	k := kernel.NewAVSync(ctx)
	srv.RegisterAVSyncFilter(ctx, key, k)

	// Audio ahead of video — delta = +1500ms.
	h := newAVSyncObsHarness(t, ctx)
	h.feed(t, k, h.audio, 2500)
	h.feed(t, k, h.video, 1000)

	addr := startGRPCServer(t, ctx, srv)
	grpcClient := newGRPCClient(t, ctx, addr)

	resp, err := grpcClient.AutoTuneAVSync(ctx, &avdmanagementgrpc.AutoTuneAVSyncRequest{
		RoutePath:       "live/sync",
		ForwardingIndex: 0,
	})
	require.NoError(t, err)
	assert.Greater(t, resp.AppliedDeltaNs, int64(0), "auto-tune must apply a positive video shift")
	assert.Equal(t, int64(1500*time.Millisecond), resp.AppliedDeltaNs)

	// The video offset must now reflect the applied delta.
	respV, err := grpcClient.GetPTSShift(ctx, &avdmanagementgrpc.GetPTSShiftRequest{
		RoutePath:       "live/sync",
		ForwardingIndex: 0,
		MediaType:       avdmanagementgrpc.AVSyncMediaType_AV_SYNC_MEDIA_TYPE_VIDEO,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1500*time.Millisecond), respV.ShiftNs)

	// Audio offset must remain zero — auto-tune NEVER touches audio.
	respA, err := grpcClient.GetPTSShift(ctx, &avdmanagementgrpc.GetPTSShiftRequest{
		RoutePath:       "live/sync",
		ForwardingIndex: 0,
		MediaType:       avdmanagementgrpc.AVSyncMediaType_AV_SYNC_MEDIA_TYPE_AUDIO,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(0), respA.ShiftNs)
}
