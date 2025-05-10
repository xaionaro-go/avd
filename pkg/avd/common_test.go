package avd

import (
	"context"

	"github.com/facebookincubator/go-belt/pkg/runtime"
	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/facebookincubator/go-belt/tool/logger/implementation/logrus"
	"github.com/xaionaro-go/observability"
	"github.com/xaionaro-go/xsync"
)

func ctx() context.Context {
	loggerLevel := logger.LevelTrace

	runtime.DefaultCallerPCFilter = observability.CallerPCFilter(runtime.DefaultCallerPCFilter)
	l := logrus.Default().WithLevel(loggerLevel)
	ctx := logger.CtxWithLogger(context.Background(), l)
	ctx = xsync.WithNoLogging(ctx, true)
	logger.Default = func() logger.Logger {
		return l
	}

	return ctx
}
