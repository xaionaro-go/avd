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

type ListeningPortDirectConsumers struct {
	Server      *Server
	PortAddress PortAddress
	Protocol    Protocol
	Config      ListenConfig
	CancelFn    context.CancelFunc
	Node        node.Abstract
	WaitGroup   sync.WaitGroup
	Locker      xsync.Mutex
	Route       *router.Route[RouteCustomData]
}

func (s *Server) ListenDirectConsumers(
	ctx context.Context,
	portAddr PortAddress,
	protocol Protocol,
	opts ...ListenOption,
) (_ret *ListeningPortDirectConsumers, _err error) {
	logger.Debugf(ctx, "ListenDirectConsumers(ctx, '%s')", portAddr)
	defer func() { logger.Debugf(ctx, "/ListenDirectConsumers(ctx, '%s'): %v %v", portAddr, _ret, _err) }()

	cfg := ListenOptions(opts).Config()
	result := &ListeningPortDirectConsumers{
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

func (p *ListeningPortDirectConsumers) GetPublishMode(
	ctx context.Context,
) router.PublishMode {
	return router.PublishMode(p.Config.PublishMode)
}

func (p *ListeningPortDirectConsumers) String() string {
	return fmt.Sprintf("%s[Consumers](%s)", p.Protocol, p.PortAddress)
}

func (p *ListeningPortDirectConsumers) startListening(
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
	customOptions := p.Config.DictionaryItems(p.Protocol, PortModeConsumers)

	route, err := p.Server.GetRoute(ctx, routePath, router.GetRouteModeCreateIfNotFound)
	if err != nil {
		return fmt.Errorf("unable to get route '%s': %w", routePath, err)
	}
	p.Route = route

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
	n := node.NewWithCustomData[*ListeningPortDirectConsumers](proc)
	n.CustomData = p
	p.Node = n
	route.Node.AddPushPacketsTo(p.Node)

	errCh := make(chan node.Error, 100)
	p.WaitGroup.Add(1)
	observability.Go(ctx, func() {
		defer p.WaitGroup.Done()
		defer func() {
			err := node.RemovePushPacketsTo(ctx, route.Node, p.Node)
			if err != nil {
				logger.Errorf(ctx, "unable to remove myself as a subscriber to '%s': %v", p.Route, err)
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

func (p *ListeningPortDirectConsumers) GetServer() *Server {
	return p.Server
}

func (p *ListeningPortDirectConsumers) GetConfig() ListenConfig {
	return p.Config
}

func (p *ListeningPortDirectConsumers) Close(ctx context.Context) (_err error) {
	logger.Debugf(ctx, "Close")
	defer func() { logger.Debugf(ctx, "/Close: %v", _err) }()
	p.CancelFn()
	p.WaitGroup.Wait()
	return nil
}
