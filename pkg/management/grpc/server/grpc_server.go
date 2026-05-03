// grpc_server.go implements the gRPC server for AVD management.

// Package server provides a gRPC server for the AVD management service.
package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/asticode/go-astiav"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/facebookincubator/go-belt"
	"github.com/xaionaro-go/avd/pkg/avd"
	"github.com/xaionaro-go/avd/pkg/avd/types"
	"github.com/xaionaro-go/avd/pkg/management/grpc/proto/avdmanagementgrpc"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/node"
	avpipelinegrpc "github.com/xaionaro-go/avpipeline/protobuf/avpipeline"
	goconvavp "github.com/xaionaro-go/avpipeline/protobuf/goconv/avpipeline"
	"github.com/xaionaro-go/avpipeline/router"
	"github.com/xaionaro-go/observability"
)

type GRPCServer struct {
	avdmanagementgrpc.AvdServiceServer
	Backend       Backend
	Listener      net.Listener
	GRPCServer    *grpc.Server
	Observability *belt.Belt
	IsServing     atomic.Bool
}

type Backend interface {
	GetListeningPorts(ctx context.Context) []avd.ListeningPort
	GetRouter() *router.Router[types.RouteCustomData]
	SetPrivacyBlurState(ctx context.Context, key avd.PrivacyBlurFilterKey, enabled *bool, blurRadius *float64, pixelateBlockSize *int64) error
	GetPrivacyBlurState(ctx context.Context, key avd.PrivacyBlurFilterKey) (enabled bool, blurRadius float64, pixelateBlockSize int64, err error)
	GetRegisteredPrivacyBlurKeys(ctx context.Context) []avd.PrivacyBlurFilterKey
	SetDeblemishState(ctx context.Context, key avd.DeblemishFilterKey, enabled *bool, sigmaS *float64, sigmaR *float64, diameter *int64) error
	GetDeblemishState(ctx context.Context, key avd.DeblemishFilterKey) (enabled bool, sigmaS float64, sigmaR float64, diameter int64, err error)
	GetRegisteredDeblemishKeys(ctx context.Context) []avd.DeblemishFilterKey
	SetWhisperState(ctx context.Context, key avd.WhisperFilterKey, enabled *bool, language *string, model *string) error
	GetWhisperState(ctx context.Context, key avd.WhisperFilterKey) (enabled bool, language string, model string, err error)
	GetRegisteredWhisperKeys(ctx context.Context) []avd.WhisperFilterKey
	SetPTSShift(ctx context.Context, key avd.AVSyncFilterKey, mediaType astiav.MediaType, shift time.Duration) error
	GetPTSShift(ctx context.Context, key avd.AVSyncFilterKey, mediaType astiav.MediaType) (time.Duration, error)
	GetAVSyncDelta(ctx context.Context, key avd.AVSyncFilterKey) (time.Duration, bool, error)
	AutoTuneAVSync(ctx context.Context, key avd.AVSyncFilterKey) (time.Duration, error)
	GetRegisteredAVSyncKeys(ctx context.Context) []avd.AVSyncFilterKey
}

func New(
	backend Backend,
	listener net.Listener,
) *GRPCServer {
	srv := &GRPCServer{
		Backend:    backend,
		Listener:   listener,
		GRPCServer: grpc.NewServer(),
	}
	avdmanagementgrpc.RegisterAvdServiceServer(srv.GRPCServer, srv)
	return srv
}

func (srv *GRPCServer) Serve() error {
	if srv.GRPCServer == nil {
		return fmt.Errorf("GRPCServer is not initialized")
	}
	if srv.IsServing.Swap(true) {
		return fmt.Errorf("GRPCServer is already serving")
	}
	defer func() {
		srv.IsServing.Store(false)
	}()
	return srv.GRPCServer.Serve(srv.Listener)
}

func (srv *GRPCServer) ServeContext(ctx context.Context) error {
	if srv.GRPCServer == nil {
		return fmt.Errorf("GRPCServer is not initialized")
	}
	if srv.IsServing.Swap(true) {
		return fmt.Errorf("GRPCServer is already serving")
	}
	defer func() {
		srv.IsServing.Store(false)
	}()
	srv.Observability = belt.CtxBelt(ctx)
	errCh := make(chan error, 1)
	observability.Go(ctx, func(_ context.Context) {
		errCh <- srv.GRPCServer.Serve(srv.Listener)
	})
	select {
	case <-ctx.Done():
		srv.GRPCServer.GracefulStop()
		return ctx.Err()
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("failed to serve gRPC: %w", err)
		}
		return nil
	}
}

func (srv *GRPCServer) ctx(ctx context.Context) context.Context {
	return belt.CtxWithBelt(ctx, srv.Observability)
}

func (srv *GRPCServer) ListPublishers(
	ctx context.Context,
	req *avdmanagementgrpc.ListPublishersRequest,
) (*avdmanagementgrpc.ListPublishersResponse, error) {
	ctx = srv.ctx(ctx)
	var result []*avpipelinegrpc.Node
	ports := srv.Backend.GetListeningPorts(ctx)
	for _, port := range ports {
		switch port.GetMode() {
		case avd.PortModePublishers:
			for _, conn := range port.GetConnections(ctx) {
				result = append(result, goconvavp.NodeToGRPC(ctx, conn.GetNode(ctx)))
			}
		}
	}
	return &avdmanagementgrpc.ListPublishersResponse{
		Nodes: result,
	}, nil
}

func (srv *GRPCServer) ListRoutes(
	ctx context.Context,
	req *avdmanagementgrpc.ListRoutesRequest,
) (*avdmanagementgrpc.ListRoutesResponse, error) {
	ctx = srv.ctx(ctx)
	var nodes []*avpipelinegrpc.Node
	var routePaths []string
	r := srv.Backend.GetRouter()
	r.Locker.Do(ctx, func() {
		for path, route := range r.RoutesByPath {
			if route == nil {
				continue
			}
			routePaths = append(routePaths, string(path))
			node := goconvavp.NodeToGRPC(ctx, route.Node)
			if node != nil {
				nodes = append(nodes, node)
			}
		}
	})
	return &avdmanagementgrpc.ListRoutesResponse{
		Nodes:      nodes,
		RoutePaths: routePaths,
	}, nil
}

func (srv *GRPCServer) ListConsumers(
	ctx context.Context,
	req *avdmanagementgrpc.ListConsumersRequest,
) (*avdmanagementgrpc.ListConsumersResponse, error) {
	ctx = srv.ctx(ctx)
	var result []*avpipelinegrpc.Node
	ports := srv.Backend.GetListeningPorts(ctx)
	for _, port := range ports {
		switch port.GetMode() {
		case avd.PortModeConsumers:
			for _, conn := range port.GetConnections(ctx) {
				result = append(result, goconvavp.NodeToGRPC(ctx, conn.GetNode(ctx)))
			}
		}
	}
	return &avdmanagementgrpc.ListConsumersResponse{
		Nodes: result,
	}, nil
}

func (srv *GRPCServer) getPipeline(
	ctx context.Context,
) (result []node.Abstract) {
	ports := srv.Backend.GetListeningPorts(ctx)
	for _, port := range ports {
		switch port.GetMode() {
		case avd.PortModePublishers:
			for _, conn := range port.GetConnections(ctx) {
				result = append(result, conn.GetNode(ctx))
			}
		}
	}
	return
}

func (srv *GRPCServer) SetPrivacyBlur(
	ctx context.Context,
	req *avdmanagementgrpc.SetPrivacyBlurRequest,
) (*avdmanagementgrpc.SetPrivacyBlurResponse, error) {
	ctx = srv.ctx(ctx)
	key := avd.PrivacyBlurFilterKey{
		RoutePath:       router.RoutePath(req.RoutePath),
		ForwardingIndex: int(req.ForwardingIndex),
	}
	var enabled *bool
	if req.Enabled != nil {
		v := *req.Enabled
		enabled = &v
	}
	var blurRadius *float64
	if req.BlurRadius != nil {
		v := *req.BlurRadius
		blurRadius = &v
	}
	var pixelateBlockSize *int64
	if req.PixelateBlockSize != nil {
		v := *req.PixelateBlockSize
		pixelateBlockSize = &v
	}
	if err := srv.Backend.SetPrivacyBlurState(ctx, key, enabled, blurRadius, pixelateBlockSize); err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	return &avdmanagementgrpc.SetPrivacyBlurResponse{}, nil
}

func (srv *GRPCServer) GetPrivacyBlur(
	ctx context.Context,
	req *avdmanagementgrpc.GetPrivacyBlurRequest,
) (*avdmanagementgrpc.GetPrivacyBlurResponse, error) {
	ctx = srv.ctx(ctx)
	key := avd.PrivacyBlurFilterKey{
		RoutePath:       router.RoutePath(req.RoutePath),
		ForwardingIndex: int(req.ForwardingIndex),
	}
	enabled, blurRadius, pixelateBlockSize, err := srv.Backend.GetPrivacyBlurState(ctx, key)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	return &avdmanagementgrpc.GetPrivacyBlurResponse{
		Enabled:           enabled,
		BlurRadius:        blurRadius,
		PixelateBlockSize: pixelateBlockSize,
	}, nil
}

func (srv *GRPCServer) SetDeblemish(
	ctx context.Context,
	req *avdmanagementgrpc.SetDeblemishRequest,
) (*avdmanagementgrpc.SetDeblemishResponse, error) {
	ctx = srv.ctx(ctx)
	key := avd.DeblemishFilterKey{
		RoutePath:       router.RoutePath(req.RoutePath),
		ForwardingIndex: int(req.ForwardingIndex),
	}
	var enabled *bool
	if req.Enabled != nil {
		v := *req.Enabled
		enabled = &v
	}
	var sigmaS *float64
	if req.SigmaS != nil {
		v := *req.SigmaS
		sigmaS = &v
	}
	var sigmaR *float64
	if req.SigmaR != nil {
		v := *req.SigmaR
		sigmaR = &v
	}
	var diameter *int64
	if req.Diameter != nil {
		v := *req.Diameter
		diameter = &v
	}
	if err := srv.Backend.SetDeblemishState(ctx, key, enabled, sigmaS, sigmaR, diameter); err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	return &avdmanagementgrpc.SetDeblemishResponse{}, nil
}

func (srv *GRPCServer) GetDeblemish(
	ctx context.Context,
	req *avdmanagementgrpc.GetDeblemishRequest,
) (*avdmanagementgrpc.GetDeblemishResponse, error) {
	ctx = srv.ctx(ctx)
	key := avd.DeblemishFilterKey{
		RoutePath:       router.RoutePath(req.RoutePath),
		ForwardingIndex: int(req.ForwardingIndex),
	}
	enabled, sigmaS, sigmaR, diameter, err := srv.Backend.GetDeblemishState(ctx, key)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	return &avdmanagementgrpc.GetDeblemishResponse{
		Enabled:  enabled,
		SigmaS:   sigmaS,
		SigmaR:   sigmaR,
		Diameter: diameter,
	}, nil
}

// filterControlKey is a composite key for deduplicating filter controls
// across privacy blur and deblemish registries.
type filterControlKey struct {
	RoutePath       string
	ForwardingIndex int
}

func (srv *GRPCServer) ListFilterControls(
	ctx context.Context,
	req *avdmanagementgrpc.ListFilterControlsRequest,
) (*avdmanagementgrpc.ListFilterControlsResponse, error) {
	ctx = srv.ctx(ctx)

	type controlFlags struct {
		HasPrivacyBlur bool
		HasDeblemish   bool
		HasWhisper     bool
		HasAVSync      bool
	}
	controls := map[filterControlKey]*controlFlags{}

	for _, k := range srv.Backend.GetRegisteredPrivacyBlurKeys(ctx) {
		fk := filterControlKey{
			RoutePath:       string(k.RoutePath),
			ForwardingIndex: k.ForwardingIndex,
		}
		flags, ok := controls[fk]
		if !ok {
			flags = &controlFlags{}
			controls[fk] = flags
		}
		flags.HasPrivacyBlur = true
	}

	for _, k := range srv.Backend.GetRegisteredDeblemishKeys(ctx) {
		fk := filterControlKey{
			RoutePath:       string(k.RoutePath),
			ForwardingIndex: k.ForwardingIndex,
		}
		flags, ok := controls[fk]
		if !ok {
			flags = &controlFlags{}
			controls[fk] = flags
		}
		flags.HasDeblemish = true
	}

	for _, k := range srv.Backend.GetRegisteredWhisperKeys(ctx) {
		fk := filterControlKey{
			RoutePath:       string(k.RoutePath),
			ForwardingIndex: k.ForwardingIndex,
		}
		flags, ok := controls[fk]
		if !ok {
			flags = &controlFlags{}
			controls[fk] = flags
		}
		flags.HasWhisper = true
	}

	for _, k := range srv.Backend.GetRegisteredAVSyncKeys(ctx) {
		fk := filterControlKey{
			RoutePath:       string(k.RoutePath),
			ForwardingIndex: k.ForwardingIndex,
		}
		flags, ok := controls[fk]
		if !ok {
			flags = &controlFlags{}
			controls[fk] = flags
		}
		flags.HasAVSync = true
	}

	result := make([]*avdmanagementgrpc.FilterControlInfo, 0, len(controls))
	for fk, flags := range controls {
		result = append(result, &avdmanagementgrpc.FilterControlInfo{
			RoutePath:       fk.RoutePath,
			ForwardingIndex: int32(fk.ForwardingIndex),
			HasPrivacyBlur:  flags.HasPrivacyBlur,
			HasDeblemish:    flags.HasDeblemish,
			HasWhisper:      flags.HasWhisper,
			HasAvSync:       flags.HasAVSync,
		})
	}

	return &avdmanagementgrpc.ListFilterControlsResponse{
		Controls: result,
	}, nil
}

func (srv *GRPCServer) SetWhisper(
	ctx context.Context,
	req *avdmanagementgrpc.SetWhisperRequest,
) (*avdmanagementgrpc.SetWhisperResponse, error) {
	ctx = srv.ctx(ctx)
	key := avd.WhisperFilterKey{
		RoutePath:       router.RoutePath(req.RoutePath),
		ForwardingIndex: int(req.ForwardingIndex),
	}
	var enabled *bool
	if req.Enabled != nil {
		v := *req.Enabled
		enabled = &v
	}
	var language *string
	if req.Language != nil {
		v := *req.Language
		language = &v
	}
	var model *string
	if req.Model != nil {
		v := *req.Model
		model = &v
	}
	if err := srv.Backend.SetWhisperState(ctx, key, enabled, language, model); err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	return &avdmanagementgrpc.SetWhisperResponse{}, nil
}

func (srv *GRPCServer) GetWhisper(
	ctx context.Context,
	req *avdmanagementgrpc.GetWhisperRequest,
) (*avdmanagementgrpc.GetWhisperResponse, error) {
	ctx = srv.ctx(ctx)
	key := avd.WhisperFilterKey{
		RoutePath:       router.RoutePath(req.RoutePath),
		ForwardingIndex: int(req.ForwardingIndex),
	}
	enabled, language, model, err := srv.Backend.GetWhisperState(ctx, key)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	return &avdmanagementgrpc.GetWhisperResponse{
		Enabled:  enabled,
		Language: language,
		Model:    model,
	}, nil
}

// protoToMediaType converts the wire-level AVSyncMediaType enum into
// the astiav.MediaType the kernel expects. UNSPECIFIED (zero value) and
// any other unknown enum value yield InvalidArgument so the operator
// gets a clear "you forgot to set media_type" error rather than a
// silent default.
func protoToMediaType(
	mt avdmanagementgrpc.AVSyncMediaType,
) (astiav.MediaType, error) {
	switch mt {
	case avdmanagementgrpc.AVSyncMediaType_AV_SYNC_MEDIA_TYPE_AUDIO:
		return astiav.MediaTypeAudio, nil
	case avdmanagementgrpc.AVSyncMediaType_AV_SYNC_MEDIA_TYPE_VIDEO:
		return astiav.MediaTypeVideo, nil
	}
	return astiav.MediaTypeUnknown, status.Errorf(codes.InvalidArgument, "media_type unspecified or invalid: %v", mt)
}

// mapAVSyncErr converts AVSync backend errors to gRPC status codes.
// Kernel sentinels (not-yet-observed / video-leads-audio) are
// FailedPrecondition; the unsupported-media-type kernel error
// (currently a plain fmt.Errorf, not a sentinel) is InvalidArgument as
// a defense-in-depth — the gRPC entry points all run req.MediaType
// through protoToMediaType first, which already rejects unsupported
// proto enums with InvalidArgument, so the kernel-side
// "unsupported media type" branch is unreachable in practice; the
// substring check guarantees that if a future kernel change starts
// emitting the same wording before protoToMediaType has filtered it
// out, the gRPC status code stays consistent. Anything else
// (registry miss) is NotFound.
func mapAVSyncErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, kernel.ErrAVSyncNotObserved),
		errors.Is(err, kernel.ErrAVSyncVideoLeadsAudio):
		return status.Errorf(codes.FailedPrecondition, "%v", err)
	case strings.Contains(err.Error(), "unsupported media type"):
		return status.Errorf(codes.InvalidArgument, "%v", err)
	default:
		return status.Errorf(codes.NotFound, "%v", err)
	}
}

func (srv *GRPCServer) GetPTSShift(
	ctx context.Context,
	req *avdmanagementgrpc.GetPTSShiftRequest,
) (*avdmanagementgrpc.GetPTSShiftResponse, error) {
	ctx = srv.ctx(ctx)
	mediaType, err := protoToMediaType(req.MediaType)
	if err != nil {
		return nil, err
	}
	key := avd.AVSyncFilterKey{
		RoutePath:       router.RoutePath(req.RoutePath),
		ForwardingIndex: int(req.ForwardingIndex),
	}
	shift, err := srv.Backend.GetPTSShift(ctx, key, mediaType)
	if err != nil {
		return nil, mapAVSyncErr(err)
	}
	return &avdmanagementgrpc.GetPTSShiftResponse{
		ShiftNs: int64(shift),
	}, nil
}

func (srv *GRPCServer) SetPTSShift(
	ctx context.Context,
	req *avdmanagementgrpc.SetPTSShiftRequest,
) (*avdmanagementgrpc.SetPTSShiftResponse, error) {
	ctx = srv.ctx(ctx)
	mediaType, err := protoToMediaType(req.MediaType)
	if err != nil {
		return nil, err
	}
	key := avd.AVSyncFilterKey{
		RoutePath:       router.RoutePath(req.RoutePath),
		ForwardingIndex: int(req.ForwardingIndex),
	}
	if err := srv.Backend.SetPTSShift(ctx, key, mediaType, time.Duration(req.ShiftNs)); err != nil {
		return nil, mapAVSyncErr(err)
	}
	return &avdmanagementgrpc.SetPTSShiftResponse{}, nil
}

func (srv *GRPCServer) GetAVSyncDelta(
	ctx context.Context,
	req *avdmanagementgrpc.GetAVSyncDeltaRequest,
) (*avdmanagementgrpc.GetAVSyncDeltaResponse, error) {
	ctx = srv.ctx(ctx)
	key := avd.AVSyncFilterKey{
		RoutePath:       router.RoutePath(req.RoutePath),
		ForwardingIndex: int(req.ForwardingIndex),
	}
	delta, observed, err := srv.Backend.GetAVSyncDelta(ctx, key)
	if err != nil {
		return nil, mapAVSyncErr(err)
	}
	return &avdmanagementgrpc.GetAVSyncDeltaResponse{
		DeltaNs:  int64(delta),
		Observed: observed,
	}, nil
}

func (srv *GRPCServer) AutoTuneAVSync(
	ctx context.Context,
	req *avdmanagementgrpc.AutoTuneAVSyncRequest,
) (*avdmanagementgrpc.AutoTuneAVSyncResponse, error) {
	ctx = srv.ctx(ctx)
	key := avd.AVSyncFilterKey{
		RoutePath:       router.RoutePath(req.RoutePath),
		ForwardingIndex: int(req.ForwardingIndex),
	}
	delta, err := srv.Backend.AutoTuneAVSync(ctx, key)
	if err != nil {
		return nil, mapAVSyncErr(err)
	}
	return &avdmanagementgrpc.AutoTuneAVSyncResponse{
		AppliedDeltaNs: int64(delta),
	}, nil
}
