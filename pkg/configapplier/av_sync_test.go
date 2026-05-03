package configapplier

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	packetorframefiltercondition "github.com/xaionaro-go/avpipeline/node/filter/packetorframefilter/condition"
)

// fakeMatchInput returns a minimal Input with no packet/frame — the
// AVSync Condition must accept it (return true) without
// observation/mutation.
func fakeMatchInput() packetorframefiltercondition.Input {
	return packetorframefiltercondition.Input{}
}

func TestNewAVSyncForRemoteForwarding_Construction(t *testing.T) {
	ctx := context.Background()

	avSync, conditions := newAVSyncForRemoteForwarding(ctx)

	require.NotNil(t, avSync, "AVSync must be constructed")
	require.Len(t, conditions, 1, "must produce one Condition")
	require.NotNil(t, conditions[0])
}

func TestNewAVSyncForRemoteForwarding_ConditionMatchesAlwaysTrue(t *testing.T) {
	ctx := context.Background()

	_, conditions := newAVSyncForRemoteForwarding(ctx)
	assert.True(t, conditions[0].Match(ctx, fakeMatchInput()), "AVSync Condition must always return true")
}
