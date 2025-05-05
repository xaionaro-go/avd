package avd

import (
	"context"
	"fmt"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avpipeline"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/processor"
)

type NodeOutput = avpipeline.NodeWithCustomData[Publisher, *processor.FromKernel[*kernel.Output]]

type ForwardingToRemote struct {
	Source      *Route
	Destination *NodeOutput
}

func (s *Server) AddForwardingToRemote(
	ctx context.Context,
	sourcePath string,
	dstAddr string,
	getRouteMode GetRouteMode,
) (_ret *ForwardingToRemote, _err error) {
	logger.Debugf(ctx, "AddForwardingToRemote(ctx, '%s', '%s', '%s')", sourcePath, dstAddr, getRouteMode)
	defer func() {
		logger.Debugf(ctx, "/AddForwardingToRemote(ctx, '%s', '%s', '%s'): %v %v", sourcePath, dstAddr, getRouteMode, _ret, _err)
	}()

	source, err := s.Router.GetRoute(ctx, sourcePath, getRouteMode)
	if err != nil {
		return nil, fmt.Errorf("unable to get a route by path '%s' with mode '%s': %w", sourcePath, getRouteMode, err)
	}
	if source == nil {
		return nil, fmt.Errorf("there is no active route by path '%s'", sourcePath)
	}

	return nil, fmt.Errorf("NOT IMPLEMENTED")
}
