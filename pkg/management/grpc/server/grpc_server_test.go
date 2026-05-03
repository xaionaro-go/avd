// grpc_server_test.go pins mapAVSyncErr's status-code mapping table.
//
// Failure mode this test guards against: a future regression that
// folds the kernel sentinels (ErrAVSyncNotObserved /
// ErrAVSyncVideoLeadsAudio) into the default branch (NotFound), or
// drops the strings.Contains("unsupported media type") branch added
// for aspect-I L-3.

package server

import (
	"errors"
	"fmt"
	"testing"

	stretchrassert "github.com/stretchr/testify/assert"
	"github.com/xaionaro-go/avpipeline/kernel"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMapAVSyncErr(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		wantCode codes.Code
	}{
		{"nil maps to nil (covered separately)", nil, codes.OK},
		{"ErrAVSyncNotObserved -> FailedPrecondition", kernel.ErrAVSyncNotObserved, codes.FailedPrecondition},
		{"ErrAVSyncVideoLeadsAudio -> FailedPrecondition", kernel.ErrAVSyncVideoLeadsAudio, codes.FailedPrecondition},
		{"wrapped sentinel still FailedPrecondition", fmt.Errorf("ctx: %w", kernel.ErrAVSyncNotObserved), codes.FailedPrecondition},
		{"unsupported media type -> InvalidArgument (defense-in-depth)", errors.New("av_sync: unsupported media type Subtitle (only Audio/Video)"), codes.InvalidArgument},
		{"registry miss -> NotFound", errors.New("no av_sync filter for key route 'live/x' forwarding #0"), codes.NotFound},
		{"unknown error -> NotFound", errors.New("something else"), codes.NotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mapAVSyncErr(tc.err)
			if tc.err == nil {
				stretchrassert.Nil(t, got)
				return
			}
			st, ok := status.FromError(got)
			stretchrassert.True(t, ok, "must be a gRPC status error")
			stretchrassert.Equal(t, tc.wantCode, st.Code(), "wrong code for %v", tc.err)
		})
	}
}
