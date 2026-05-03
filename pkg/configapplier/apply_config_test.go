// apply_config_test.go pins the deadline-trimming behavior of
// collectForwardingErrors that aspect-I L-1 ("subtract unspawned
// forwardings from expected") relies on:
//
//   - expected==0 must return nil immediately (no deadline wait).
//   - expected==N with fewer than N senders must run to the deadline
//     and return only the errors actually delivered.
//
// Failure mode this test guards against: a regression that goes back
// to passing the full forwarding count (including the unspawned
// endpoints' senders) into collectForwardingErrors would block the
// whole forwardingSetupInitialDeadline (2s today) before applyConfig
// surfaces the spawn error.

package configapplier

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xaionaro-go/avd/pkg/avd"
	"github.com/xaionaro-go/avd/pkg/avd/types"
	"github.com/xaionaro-go/avd/pkg/config"
)

func TestCollectForwardingErrors_ExpectedZeroReturnsImmediately(t *testing.T) {
	ctx := context.Background()
	errChan := make(chan error, 0) // empty, capacity zero is fine — expected==0 means no recv

	start := time.Now()
	err := collectForwardingErrors(ctx, errChan, 0, time.Second)
	elapsed := time.Since(start)

	assert.NoError(t, err)
	assert.Less(t, elapsed, 50*time.Millisecond, "expected==0 must short-circuit before the deadline")
}

func TestCollectForwardingErrors_DeadlineTripsWhenSendersMissing(t *testing.T) {
	ctx := context.Background()
	errChan := make(chan error, 2)
	errChan <- errors.New("setup-err")
	// Only one sender out of expected==2; the deadline must trip.

	start := time.Now()
	err := collectForwardingErrors(ctx, errChan, 2, 25*time.Millisecond)
	elapsed := time.Since(start)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "setup-err")
	assert.GreaterOrEqual(t, elapsed, 25*time.Millisecond, "must wait the full deadline when senders are missing")
	assert.Less(t, elapsed, 250*time.Millisecond, "must not wait past the deadline by more than scheduling slack")
}

func TestApplyLiteralEndpoints_EmptyMapShortCircuits(t *testing.T) {
	ctx := context.Background()
	srv := avd.NewServer(ctx)
	t.Cleanup(func() { _ = srv.Close(context.Background()) })

	// Empty map → applyLiteralEndpoints must NOT block on the
	// 2-second forwardingSetupInitialDeadline. Pre-L-1 path passed
	// the static (incorrect) totalForwardings into
	// collectForwardingErrors; the empty case happened to have zero
	// already, but the fast return is still the property aspect-I
	// L-1 protects against regressing as the loop is restructured.
	start := time.Now()
	err := applyLiteralEndpoints(ctx, srv, map[types.RoutePath]config.EndpointConfig{})
	elapsed := time.Since(start)

	assert.NoError(t, err)
	assert.Less(t, elapsed, 100*time.Millisecond, "empty endpoints map must not wait the deadline")
}

func TestCollectForwardingErrors_AllSendersBeforeDeadline(t *testing.T) {
	ctx := context.Background()
	errChan := make(chan error, 3)
	errChan <- nil
	errChan <- errors.New("e1")
	errChan <- nil

	start := time.Now()
	err := collectForwardingErrors(ctx, errChan, 3, time.Second)
	elapsed := time.Since(start)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "e1")
	assert.Less(t, elapsed, 50*time.Millisecond, "must return as soon as `expected` senders have reported")
}
