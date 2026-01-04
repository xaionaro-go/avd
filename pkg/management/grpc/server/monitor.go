// monitor.go provides monitoring capabilities for the management server.

package server

import (
	"context"
	"fmt"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avd/pkg/management/grpc/proto/avdmanagementgrpc"
	"github.com/xaionaro-go/avpipeline"
	"github.com/xaionaro-go/avpipeline/monitor"
	"github.com/xaionaro-go/avpipeline/node"
	avpipeline_proto "github.com/xaionaro-go/avpipeline/protobuf/avpipeline"
	avptypes "github.com/xaionaro-go/avpipeline/types"
	"github.com/xaionaro-go/xgrpc"
)

type Monitor struct {
	Object node.Abstract
	Events chan *avpipeline_proto.MonitorEvent
	Type   avpipeline_proto.MonitorEventType
}

func (srv *GRPCServer) Monitor(
	req *avpipeline_proto.MonitorRequest,
	resp avdmanagementgrpc.AvdService_MonitorServer,
) (_err error) {
	ctx := srv.ctx(resp.Context())
	obj := avptypes.ObjectID(req.GetNodeId())
	pipeline := srv.getPipeline(ctx)
	node, err := avpipeline.FindNodeByObjectID(ctx, obj, pipeline...)
	if err != nil {
		return fmt.Errorf("failed to find node by ID %v: %w", obj, err)
	}
	monitor, err := monitor.New(ctx, node, req.GetEventType(), req.GetIncludePacketPayload(), req.GetIncludeFramePayload(), req.GetDoDecode())
	if err != nil {
		return fmt.Errorf("failed to create monitor for node %q: %w", obj, err)
	}
	defer func() {
		err := monitor.Close(ctx)
		if err != nil {
			logger.Errorf(ctx, "failed to close monitor for node %q: %v", obj, err)
		}
	}()
	return xgrpc.WrapChan(ctx,
		func(ctx context.Context) (<-chan *avpipeline_proto.MonitorEvent, error) {
			return monitor.Events, nil
		},
		resp,
		func(in *avpipeline_proto.MonitorEvent) *avpipeline_proto.MonitorEvent {
			return in
		},
	)
}
