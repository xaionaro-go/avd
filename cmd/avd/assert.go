package main

import (
	"context"

	"github.com/facebookincubator/go-belt/tool/logger"
)

func assertNoError(
	ctx context.Context,
	err error,
) {
	if err == nil {
		return
	}

	logger.Panicf(ctx, "an assertion failed: the error is not nil: %v", err)
}
