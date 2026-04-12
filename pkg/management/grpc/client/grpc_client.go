// Package client provides a gRPC client for the AVD management service.
package client

// grpc_client.go provides a gRPC client for the AVD management service.

import (
	"context"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/xaionaro-go/avd/pkg/management/grpc/proto/avdmanagementgrpc"
	avpipeline_proto "github.com/xaionaro-go/avpipeline/protobuf/avpipeline"
	"github.com/xaionaro-go/xgrpc"
)

type GRPCClient struct {
	conn   *grpc.ClientConn
	client avdmanagementgrpc.AvdServiceClient
}

func New(
	ctx context.Context,
	address string,
) (*GRPCClient, error) {
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}
	client := avdmanagementgrpc.NewAvdServiceClient(conn)
	return &GRPCClient{conn: conn, client: client}, nil
}

func (c *GRPCClient) Close() error {
	return c.conn.Close()
}

func (c *GRPCClient) ListPublishers(
	ctx context.Context,
) (*avdmanagementgrpc.ListPublishersResponse, error) {
	return c.client.ListPublishers(ctx, &avdmanagementgrpc.ListPublishersRequest{})
}

func (c *GRPCClient) ListConsumers(
	ctx context.Context,
) (*avdmanagementgrpc.ListConsumersResponse, error) {
	return c.client.ListConsumers(ctx, &avdmanagementgrpc.ListConsumersRequest{})
}

func (c *GRPCClient) ListRoutes(
	ctx context.Context,
) (*avdmanagementgrpc.ListRoutesResponse, error) {
	return c.client.ListRoutes(ctx, &avdmanagementgrpc.ListRoutesRequest{})
}

func (c *GRPCClient) GRPCClient(
	ctx context.Context,
) (avdmanagementgrpc.AvdServiceClient, io.Closer, error) {
	return c.client, io.NopCloser(nil), nil
}

func (c *GRPCClient) GetCallWrapper() xgrpc.CallWrapperFunc {
	return nil
}

func (c *GRPCClient) ProcessError(
	ctx context.Context,
	in error,
) error {
	return in
}

func (c *GRPCClient) SetPrivacyBlur(
	ctx context.Context,
	req *avdmanagementgrpc.SetPrivacyBlurRequest,
) (*avdmanagementgrpc.SetPrivacyBlurResponse, error) {
	return c.client.SetPrivacyBlur(ctx, req)
}

func (c *GRPCClient) GetPrivacyBlur(
	ctx context.Context,
	req *avdmanagementgrpc.GetPrivacyBlurRequest,
) (*avdmanagementgrpc.GetPrivacyBlurResponse, error) {
	return c.client.GetPrivacyBlur(ctx, req)
}

func (c *GRPCClient) SetDeblemish(
	ctx context.Context,
	req *avdmanagementgrpc.SetDeblemishRequest,
) (*avdmanagementgrpc.SetDeblemishResponse, error) {
	return c.client.SetDeblemish(ctx, req)
}

func (c *GRPCClient) GetDeblemish(
	ctx context.Context,
	req *avdmanagementgrpc.GetDeblemishRequest,
) (*avdmanagementgrpc.GetDeblemishResponse, error) {
	return c.client.GetDeblemish(ctx, req)
}

func (c *GRPCClient) SetWhisper(
	ctx context.Context,
	req *avdmanagementgrpc.SetWhisperRequest,
) (*avdmanagementgrpc.SetWhisperResponse, error) {
	return c.client.SetWhisper(ctx, req)
}

func (c *GRPCClient) GetWhisper(
	ctx context.Context,
	req *avdmanagementgrpc.GetWhisperRequest,
) (*avdmanagementgrpc.GetWhisperResponse, error) {
	return c.client.GetWhisper(ctx, req)
}

func (c *GRPCClient) ListFilterControls(
	ctx context.Context,
) (*avdmanagementgrpc.ListFilterControlsResponse, error) {
	return c.client.ListFilterControls(ctx, &avdmanagementgrpc.ListFilterControlsRequest{})
}

func (c *GRPCClient) Monitor(
	ctx context.Context,
	nodeID uint64,
	eventType avpipeline_proto.MonitorEventType,
	includePacketPayload bool,
	includeFramePayload bool,
	doDecode bool,
) (<-chan *avpipeline_proto.MonitorEvent, error) {
	return xgrpc.UnwrapChan(ctx,
		c,
		func(
			ctx context.Context,
			client avdmanagementgrpc.AvdServiceClient,
		) (avdmanagementgrpc.AvdService_MonitorClient, error) {
			return client.Monitor(ctx, &avpipeline_proto.MonitorRequest{
				NodeId:               nodeID,
				EventType:            eventType,
				IncludePacketPayload: includePacketPayload,
				IncludeFramePayload:  includeFramePayload,
				DoDecode:             doDecode,
			})
		},
		func(
			ctx context.Context,
			ev *avpipeline_proto.MonitorEvent,
		) *avpipeline_proto.MonitorEvent {
			return ev
		},
	)
}
