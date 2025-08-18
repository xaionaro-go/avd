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
