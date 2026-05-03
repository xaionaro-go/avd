package configapplier

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xaionaro-go/avpipeline/kernel"
	"github.com/xaionaro-go/avpipeline/packetorframe"
	"github.com/xaionaro-go/avpipeline/router"
)

// stubKernel is a minimal kernel.Abstract for testing that counts SendInput calls.
type stubKernel struct {
	kernel.Abstract
	sendCount atomic.Int64
	closed    atomic.Bool
}

func newStubKernel() *stubKernel {
	return &stubKernel{}
}

func (s *stubKernel) SendInput(
	_ context.Context,
	input packetorframe.InputUnion,
	outputCh chan<- packetorframe.OutputUnion,
) error {
	s.sendCount.Add(1)
	outputCh <- input.CloneAsReferencedOutput()
	return nil
}

func (s *stubKernel) Close(_ context.Context) error {
	s.closed.Store(true)
	return nil
}

func (s *stubKernel) CloseChan() <-chan struct{} {
	return nil
}

func (s *stubKernel) Generate(_ context.Context, _ chan<- packetorframe.OutputUnion) error {
	return nil
}

func (s *stubKernel) String() string {
	return "stubKernel"
}

func TestComposeFilterKernelFactories_NilFactories(t *testing.T) {
	result := composeFilterKernelFactories(nil, nil)
	assert.Nil(t, result)
}

func TestComposeFilterKernelFactories_SingleFactory(t *testing.T) {
	k := newStubKernel()
	factory := router.FilterKernelFactory(func(_ context.Context) (kernel.Abstract, error) {
		return k, nil
	})

	result := composeFilterKernelFactories(nil, factory, nil)
	require.NotNil(t, result)

	got, err := result(context.Background())
	require.NoError(t, err)
	assert.Equal(t, k, got)
}

func TestComposeFilterKernelFactories_MultipleFactories(t *testing.T) {
	k1 := newStubKernel()
	k2 := newStubKernel()

	f1 := router.FilterKernelFactory(func(_ context.Context) (kernel.Abstract, error) {
		return k1, nil
	})
	f2 := router.FilterKernelFactory(func(_ context.Context) (kernel.Abstract, error) {
		return k2, nil
	})

	result := composeFilterKernelFactories(f1, f2)
	require.NotNil(t, result)

	chain, err := result(context.Background())
	require.NoError(t, err)
	require.NotNil(t, chain)

	// Send a nil-frame input through the chain.
	outputCh := make(chan packetorframe.OutputUnion, 1)
	err = chain.SendInput(context.Background(), packetorframe.InputUnion{}, outputCh)
	require.NoError(t, err)

	// Both kernels should have been called.
	assert.Equal(t, int64(1), k1.sendCount.Load())
	assert.Equal(t, int64(1), k2.sendCount.Load())
}

func TestSkipWhenActiveGuard_GuardFalse(t *testing.T) {
	k := newStubKernel()
	guard := &atomic.Bool{}
	guard.Store(false)

	guarded := &skipWhenActiveGuard{
		Abstract: k,
		guard:    guard,
	}

	outputCh := make(chan packetorframe.OutputUnion, 1)
	err := guarded.SendInput(context.Background(), packetorframe.InputUnion{}, outputCh)
	require.NoError(t, err)

	// Kernel should have been called (guard is false).
	assert.Equal(t, int64(1), k.sendCount.Load())
}

func TestSkipWhenActiveGuard_GuardTrue(t *testing.T) {
	k := newStubKernel()
	guard := &atomic.Bool{}
	guard.Store(true)

	guarded := &skipWhenActiveGuard{
		Abstract: k,
		guard:    guard,
	}

	outputCh := make(chan packetorframe.OutputUnion, 1)
	err := guarded.SendInput(context.Background(), packetorframe.InputUnion{}, outputCh)
	require.NoError(t, err)

	// Kernel should NOT have been called (guard is true → passthrough).
	assert.Equal(t, int64(0), k.sendCount.Load())
}

func TestSkipWhenActiveGuard_RuntimeToggle(t *testing.T) {
	k := newStubKernel()
	guard := &atomic.Bool{}
	guard.Store(false)

	guarded := &skipWhenActiveGuard{
		Abstract: k,
		guard:    guard,
	}
	outputCh := make(chan packetorframe.OutputUnion, 1)

	// Guard off → kernel runs.
	err := guarded.SendInput(context.Background(), packetorframe.InputUnion{}, outputCh)
	require.NoError(t, err)
	<-outputCh
	assert.Equal(t, int64(1), k.sendCount.Load())

	// Toggle guard on → kernel skipped.
	guard.Store(true)
	err = guarded.SendInput(context.Background(), packetorframe.InputUnion{}, outputCh)
	require.NoError(t, err)
	<-outputCh
	assert.Equal(t, int64(1), k.sendCount.Load())

	// Toggle guard off → kernel runs again.
	guard.Store(false)
	err = guarded.SendInput(context.Background(), packetorframe.InputUnion{}, outputCh)
	require.NoError(t, err)
	<-outputCh
	assert.Equal(t, int64(2), k.sendCount.Load())
}
