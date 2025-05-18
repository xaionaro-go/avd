package avd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/facebookincubator/go-belt"
	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avd/pkg/avd/types"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/node"
	"github.com/xaionaro-go/avpipeline/processor"
	"github.com/xaionaro-go/avpipeline/router"
	"github.com/xaionaro-go/observability"
	"github.com/xaionaro-go/secret"
	"github.com/xaionaro-go/xsync"
)

type ListeningPortDirect struct {
	Server      *Server
	PortAddress PortAddress
	Protocol    Protocol
	Mode        PortMode
	Config      ListenConfig
	CancelFn    context.CancelFunc
	Node        node.Abstract
	Route       *router.Route
	RouteSource *router.RouteSource[*ListeningPortDirect, *processor.FromKernel[*kernel.Input]]
	WaitGroup   sync.WaitGroup
	Locker      xsync.Mutex
}

func (s *Server) ListenDirect(
	ctx context.Context,
	portAddr PortAddress,
	protocol Protocol,
	mode types.PortMode,
	opts ...ListenOption,
) (_ret *ListeningPortDirect, _err error) {
	logger.Debugf(ctx, "ListenDirect(ctx, '%s')", portAddr)
	defer func() { logger.Debugf(ctx, "/ListenDirect(ctx, '%s'): %v %v", portAddr, _ret, _err) }()

	cfg := ListenOptions(opts).Config()
	result := &ListeningPortDirect{
		Server:      s,
		PortAddress: portAddr,
		Protocol:    protocol,
		Mode:        mode,
		Config:      cfg,
	}

	err := result.startListening(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to start listening: %w", err)
	}

	return result, nil
}

func (p *ListeningPortDirect) String() string {
	return fmt.Sprintf("%s[%s](%s)", p.Protocol, p.Mode, p.PortAddress)
}

func (p *ListeningPortDirect) startListening(
	ctx context.Context,
) (_err error) {
	ctx = belt.WithField(ctx, "protocol", p.Protocol.String())
	ctx = belt.WithField(ctx, "port_addr", p.PortAddress)
	ctx = belt.WithField(ctx, "port_mode", p.Mode.String())
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
	route, err := p.Server.GetRoute(ctx, routePath, router.GetRouteModeCreateIfNotFound)
	if err != nil {
		return fmt.Errorf("unable to get route '%s': %w", routePath, err)
	}
	p.Route = route

	url := fmt.Sprintf("%s://%s", proto, hostPort)
	customOptions := p.Config.DictionaryItems(p.Protocol, p.Mode)
	switch p.Mode {
	case PortModePublishers:
		var n *node.NodeWithCustomData[*ListeningPortDirect, *processor.FromKernel[*kernel.Input]]
		proc, err := processor.NewInputFromURL(
			ctx, url, secret.New(""),
			kernel.InputConfig{
				CustomOptions: customOptions,
				AsyncOpen:     true,
				OnOpened: func(ctx context.Context, k *kernel.Input) error {
					routeSource, err := router.AddRouteSource(ctx, p.Server.Router, n, p.Route.Path, nil)
					if err != nil {
						return fmt.Errorf("unable to add a source to router '%s': %w", p.Route, err)
					}
					p.Locker.Do(ctx, func() {
						p.RouteSource = routeSource
					})
					return nil
				},
			},
		)
		if err != nil {
			return fmt.Errorf("unable to initialize an input listener at '%s': %w", url, err)
		}
		n = node.NewWithCustomData[*ListeningPortDirect](proc)
		n.CustomData = p
		p.Node = n
	case PortModeConsumers:
		proc, err := processor.NewOutputFromURL(
			ctx, url, secret.New(""),
			kernel.OutputConfig{
				CustomOptions: customOptions,
				AsyncOpen:     true,
				OnOpened: func(ctx context.Context, k *kernel.Output) error {
					return nil
				},
			},
		)
		if err != nil {
			return fmt.Errorf("unable to initialize an input listener at '%s': %w", url, err)
		}
		n := node.NewWithCustomData[*ListeningPortDirect](proc)
		n.CustomData = p
		p.Node = n
		route.Node.AddPushPacketsTo(p.Node)
	default:
		return fmt.Errorf("unknown port mode '%s'", p.Mode)
	}

	errCh := make(chan node.Error, 100)
	p.WaitGroup.Add(1)
	observability.Go(ctx, func() {
		defer p.WaitGroup.Done()
		defer func() {
			switch p.Mode {
			case PortModePublishers:
				err := p.RouteSource.Close(ctx)
				if err != nil {
					logger.Errorf(ctx, "unable to remove myself as the source for route to '%s': %v", p.Route, err)
				}
				err = PublisherClose(ctx, p, p.Config.OnEndAction)
				if err != nil {
					logger.Errorf(ctx, "unable to close the publisher to '%s': %v", p.Route, err)
				}
			case PortModeConsumers:
				err := node.RemovePushPacketsTo(ctx, route.Node, p.Node)
				if err != nil {
					logger.Errorf(ctx, "unable to remove myself as a subscriber to '%s': %v", p.Route, err)
				}
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

func (p *ListeningPortDirect) GetServer() *Server {
	return p.Server
}

func (p *ListeningPortDirect) GetConfig() ListenConfig {
	return p.Config
}

func (p *ListeningPortDirect) GetInputNode(
	ctx context.Context,
) node.Abstract {
	if p.Mode != PortModePublishers {
		panic(fmt.Errorf("unexpected mode: %s", p.Mode))
	}
	return p.Node
}

func (p *ListeningPortDirect) GetOutputRoute(
	ctx context.Context,
) *router.Route {
	if p.Mode != PortModePublishers {
		panic(fmt.Errorf("unexpected mode: %s", p.Mode))
	}
	return p.Route
}

func (p *ListeningPortDirect) Close(ctx context.Context) (_err error) {
	logger.Debugf(ctx, "Close")
	defer func() { logger.Debugf(ctx, "/Close: %v", _err) }()
	p.CancelFn()
	p.WaitGroup.Wait()
	return nil
}
