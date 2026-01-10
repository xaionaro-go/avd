// connection_proxied_handler.go defines the interface for handling proxied connections.

package avd

import (
	"context"
	"net/url"

	"github.com/asticode/go-astiav"
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
	GetFormatContext() *astiav.FormatContext
	StartForwarding(context.Context) error
	SetURL(context.Context, *url.URL)
	Close(context.Context) error
}
