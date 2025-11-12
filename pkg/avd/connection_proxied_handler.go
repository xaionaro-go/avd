package avd

import (
	"context"
	"net/url"

	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/node"
	"github.com/xaionaro-go/secret"
)

type ConnectionProxiedHandler interface {
	InitAVHandler(
		ctx context.Context,
		proto Protocol,
		url *url.URL,
		secretKey secret.String,
		listenConfig ListenConfig,
	) error
	GetNode() node.Abstract
	GetKernel() kernel.Abstract
	StartForwarding(context.Context) error
	SetURL(context.Context, *url.URL)
	Close(context.Context) error
}
