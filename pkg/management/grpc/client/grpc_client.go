package client

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/xaionaro-go/avd/pkg/management/grpc/proto/avdmanagementgrpc"
)

type GRPCClient struct {
	conn   *grpc.ClientConn
	client avdmanagementgrpc.AvdServiceClient
}

func NewGRPCClient(address string) (*GRPCClient, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
	req *avdmanagementgrpc.ListPublishersRequest,
) (*avdmanagementgrpc.ListPublishersResponse, error) {
	return c.client.ListPublishers(ctx, req)
}

func (c *GRPCClient) ListConsumers(
	ctx context.Context,
	req *avdmanagementgrpc.ListConsumersRequest,
) (*avdmanagementgrpc.ListConsumersResponse, error) {
	return c.client.ListConsumers(ctx, req)
}
