package configapplier

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/xaionaro-go/avpipeline/helpers/closuresignaler"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/packetorframe"
	"github.com/xaionaro-go/avpipeline/router"
	globaltypes "github.com/xaionaro-go/avpipeline/types"
)

// composeFilterKernelFactories combines multiple filter kernel factories
// into one. If only one non-nil factory is provided, it is returned as-is.
// If multiple are provided, they produce a kernelChain that pipes frames
// through each kernel sequentially.
func composeFilterKernelFactories(
	factories ...router.FilterKernelFactory,
) router.FilterKernelFactory {
	var nonNil []router.FilterKernelFactory
	for _, f := range factories {
		if f != nil {
			nonNil = append(nonNil, f)
		}
	}

	switch len(nonNil) {
	case 0:
		return nil
	case 1:
		return nonNil[0]
	default:
		return func(ctx context.Context) (kernel.Abstract, error) {
			var kernels []kernel.Abstract
			for _, f := range nonNil {
				k, err := f(ctx)
				if err != nil {
					for _, prev := range kernels {
						_ = prev.Close(ctx)
					}
					return nil, err
				}
				if k != nil {
					kernels = append(kernels, k)
				}
			}

			switch len(kernels) {
			case 0:
				return nil, nil
			case 1:
				return kernels[0], nil
			default:
				return &kernelChain{
					ClosureSignaler: closuresignaler.New(),
					kernels:         kernels,
				}, nil
			}
		}
	}
}

// kernelChain pipes frames through multiple kernels sequentially.
type kernelChain struct {
	*closuresignaler.ClosureSignaler
	kernels []kernel.Abstract
}

func (c *kernelChain) GetObjectID() globaltypes.ObjectID {
	return globaltypes.GetObjectID(c)
}

func (c *kernelChain) String() string {
	return fmt.Sprintf("kernelChain(%d)", len(c.kernels))
}

func (c *kernelChain) SendInput(
	ctx context.Context,
	input packetorframe.InputUnion,
	outputCh chan<- packetorframe.OutputUnion,
) error {
	if len(c.kernels) == 0 {
		outputCh <- input.CloneAsReferencedOutput()
		return nil
	}

	// Assumes each kernel produces exactly one output per input.
	// This holds for deblemish and privacy blur; a future kernel
	// that emits 0 or 2+ outputs would need a different approach.
	current := input
	for i, k := range c.kernels {
		if i == len(c.kernels)-1 {
			return k.SendInput(ctx, current, outputCh)
		}

		ch := make(chan packetorframe.OutputUnion, 1)
		if err := k.SendInput(ctx, current, ch); err != nil {
			return err
		}
		output := <-ch
		current = output.ToInput()
	}

	return nil
}

func (c *kernelChain) Close(ctx context.Context) error {
	c.ClosureSignaler.Close(ctx)
	var errs []error
	for _, k := range c.kernels {
		if err := k.Close(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (c *kernelChain) Generate(
	_ context.Context,
	_ chan<- packetorframe.OutputUnion,
) error {
	return nil
}

// skipWhenActiveGuard wraps a kernel and passes frames through unchanged
// when a guard condition (an external atomic.Bool) is true.
// Used to skip deblemish when privacy blur with face detection is active.
type skipWhenActiveGuard struct {
	kernel.Abstract
	guard *atomic.Bool
}

func (g *skipWhenActiveGuard) SendInput(
	ctx context.Context,
	input packetorframe.InputUnion,
	outputCh chan<- packetorframe.OutputUnion,
) error {
	if g.guard != nil && g.guard.Load() {
		outputCh <- input.CloneAsReferencedOutput()
		return nil
	}
	return g.Abstract.SendInput(ctx, input, outputCh)
}

// wrapWithSkipGuard wraps a filter kernel factory so that the produced kernel
// is skipped (passthrough) whenever the guard atomic.Bool is true.
func wrapWithSkipGuard(
	factory router.FilterKernelFactory,
	guard *atomic.Bool,
) router.FilterKernelFactory {
	if factory == nil || guard == nil {
		return factory
	}
	return func(ctx context.Context) (kernel.Abstract, error) {
		k, err := factory(ctx)
		if err != nil {
			return nil, err
		}
		if k == nil {
			return nil, nil
		}
		return &skipWhenActiveGuard{
			Abstract: k,
			guard:    guard,
		}, nil
	}
}
