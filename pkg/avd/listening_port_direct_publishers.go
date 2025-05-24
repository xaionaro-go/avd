package avd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/facebookincubator/go-belt"
	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/node"
	"github.com/xaionaro-go/avpipeline/processor"
	"github.com/xaionaro-go/avpipeline/router"
	"github.com/xaionaro-go/observability"
	"github.com/xaionaro-go/secret"
	"github.com/xaionaro-go/xsync"
)

type ListeningPortDirectPublishers struct {
	Server        *Server
	PortAddress   PortAddress
	Protocol      Protocol
	Config        ListenConfig
	CancelFn      context.CancelFunc
	Node          node.Abstract
	WaitGroup     sync.WaitGroup
	Locker        xsync.Mutex
	AsRouteSource *RouteSource[*ListeningPortDirectPublishers]
}

var _ Publisher = (*ListeningPortDirectPublishers)(nil)

func (s *Server) ListenDirectPublishers(
	ctx context.Context,
	portAddr PortAddress,
	protocol Protocol,
	opts ...ListenOption,
) (_ret *ListeningPortDirectPublishers, _err error) {
	logger.Debugf(ctx, "ListenDirectPublishers(ctx, '%s')", portAddr)
	defer func() { logger.Debugf(ctx, "/ListenDirectPublishers(ctx, '%s'): %v %v", portAddr, _ret, _err) }()

	cfg := ListenOptions(opts).Config()
	result := &ListeningPortDirectPublishers{
		Server:      s,
		PortAddress: portAddr,
		Protocol:    protocol,
		Config:      cfg,
	}

	err := result.startListening(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to start listening: %w", err)
	}

	return result, nil
}

func (p *ListeningPortDirectPublishers) GetPublishMode(
	ctx context.Context,
) router.PublishMode {
	return router.PublishMode(p.Config.PublishMode)
}

func (p *ListeningPortDirectPublishers) String() string {
	return fmt.Sprintf("%s[Publishers](%s)", p.Protocol, p.PortAddress)
}

func (p *ListeningPortDirectPublishers) startListening(
	ctx context.Context,
) (_err error) {
	ctx = belt.WithField(ctx, "protocol", p.Protocol.String())
	ctx = belt.WithField(ctx, "port_addr", p.PortAddress)
	if p.CancelFn != nil {
		return fmt.Errorf("the port was already started")
	}
	ctx, cancelFn := context.WithCancel(ctx)
	p.CancelFn = cancelFn
	logger.Debugf(ctx, "startListening")
	defer func() { logger.Debugf(ctx, "/startListening: %v", _err) }()

	proto, hostPort, err := p.PortAddress.Parse(ctx)
	if err != nil {
		return fmt.Errorf("unable to parse port address '%s': %w", p.PortAddress, err)
	}

	routePath := p.Config.DefaultRoutePath

	url := fmt.Sprintf("%s://%s", proto, hostPort)
	customOptions := p.Config.DictionaryItems(p.Protocol, PortModePublishers)
	var n *NodeInputDirect
	proc, err := processor.NewInputFromURL(
		ctx, url, secret.New(""),
		kernel.InputConfig{
			CustomOptions: customOptions,
			AsyncOpen:     true,
			OnOpened: func(ctx context.Context, k *kernel.Input) error {
				return xsync.DoR1(ctx, &p.Locker, func() error {
					routeSource, err := router.AddRouteSource(
						ctx,
						p.Server.Router,
						n,
						routePath,
						p.Config.PublishMode,
						nil,
						p.onRouteSourceStart,
						p.onRouteSourceStop,
					)
					if err != nil {
						return fmt.Errorf("unable to add a source to route '%s': %w", routePath, err)
					}
					p.AsRouteSource = routeSource

					err = routeSource.Start(ctx)
					if err != nil {
						cancelFn()
						return fmt.Errorf("unable to start the RouteSource: %w", err)
					}
					return nil
				})
			},
		},
	)
	if err != nil {
		return fmt.Errorf("unable to initialize an input listener at '%s': %w", url, err)
	}
	n = node.NewWithCustomData[*ListeningPortDirectPublishers](proc)
	n.CustomData = p
	p.Node = n

	errCh := make(chan node.Error, 100)
	p.WaitGroup.Add(1)
	observability.Go(ctx, func() {
		defer p.WaitGroup.Done()
		defer func() {
			err := p.AsRouteSource.Close(ctx)
			if err != nil {
				logger.Errorf(ctx, "unable to remove myself as the source for route to '%s': %v", p.AsRouteSource.Output, err)
			}
		}()
		defer close(errCh)
		logger.Debugf(ctx, "started")
		defer logger.Debugf(ctx, "finished")
		p.Node.Serve(ctx, node.ServeConfig{}, errCh)
	})
	p.WaitGroup.Add(1)
	observability.Go(ctx, func() {
		defer p.WaitGroup.Done()
		for err := range errCh {
			switch {
			case errors.Is(err, context.Canceled):
				logger.Debugf(ctx, "cancelled: %v", err)
			case errors.Is(err, io.EOF):
				logger.Debugf(ctx, "EOF: %v", err)
			default:
				logger.Errorf(ctx, "got an error: %v", err)
			}
			p.Close(ctx)
		}
	})

	return nil
}

func (p *ListeningPortDirectPublishers) onRouteSourceStart(
	ctx context.Context,
	rs *RouteSource[*ListeningPortDirectPublishers],
) {
	logger.Debugf(ctx, "onRouteSourceStart")
	defer func() { logger.Debugf(ctx, "/onRouteSourceStart") }()
}

func (p *ListeningPortDirectPublishers) onRouteSourceStop(
	ctx context.Context,
	rs *RouteSource[*ListeningPortDirectPublishers],
) {
	logger.Debugf(ctx, "onRouteSourceStop")
	defer func() { logger.Debugf(ctx, "/onRouteSourceStop") }()
	err := PublisherClose(ctx, p, p.Config.OnEndAction)
	if err != nil {
		logger.Errorf(ctx, "unable to close the publisher: %v", err)
	}
}

func (p *ListeningPortDirectPublishers) GetServer() *Server {
	return p.Server
}

func (p *ListeningPortDirectPublishers) GetConfig() ListenConfig {
	return p.Config
}

func (p *ListeningPortDirectPublishers) GetInputNode(
	ctx context.Context,
) node.Abstract {
	return p.Node
}

func (p *ListeningPortDirectPublishers) GetOutputRoute(
	ctx context.Context,
) *router.Route[RouteCustomData] {
	return p.AsRouteSource.Output
}

func (p *ListeningPortDirectPublishers) Close(ctx context.Context) (_err error) {
	logger.Debugf(ctx, "Close")
	defer func() { logger.Debugf(ctx, "/Close: %v", _err) }()
	p.CancelFn()
	p.WaitGroup.Wait()
	return nil
}
