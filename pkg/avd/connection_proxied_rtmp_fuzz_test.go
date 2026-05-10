// connection_proxied_rtmp_fuzz_test.go pins the documented scope of the
// chunk-header-blind RTMP AMF snooper (see the package doc-comment block
// at the top of connection_proxied_rtmp.go). The tests here serve two
// purposes:
//
//  1. Demonstrate the *expected* libav-only success path: connect+publish
//     AMFs that fit inside one default-size (128B) RTMP chunk parse
//     correctly.
//  2. Pin the *known* failure mode: a connect AMF whose payload exceeds
//     128 bytes, so the chunked wire form contains a 0xc3 continuation
//     header inside the AMF property run, fails to parse — and we
//     deliberately assert that failure so a future change that silently
//     broadens or breaks the parser is caught loudly. If somebody adds
//     chunk reassembly, this test must be updated together with the
//     scope doc-comment to reflect the new contract.
//
// We also include a short fuzz target that exercises the parser against
// random byte slices to catch index-bound regressions (no panic / no
// silent stall). The fuzz seed corpus uses real-shaped connect+publish
// payloads.

package avd

import (
	"strings"
	"testing"

	"github.com/facebookincubator/go-belt"
	"github.com/stretchr/testify/require"
)

// TestRTMPSnooperChunkSpanningConnect_DocumentedFailureMode is a
// negative-pin: it asserts the *known broken* behavior of the chunk-
// header-blind snooper for >128B AMFs so a future change to the snooper
// (e.g. someone adds chunk reassembly) is caught by this test and
// forces an explicit update of (a) the scope doc-comment at the top of
// connection_proxied_rtmp.go and (b) this test. The shape of "broken"
// pinned here is: the snooper silently absorbs the 0xc3 RTMP-chunk
// continuation byte verbatim into the app value, producing a route-key
// no client can match. That is the failure mode the doc-comment warns
// about; if it changes, the doc-comment changes too.
func TestRTMPSnooperChunkSpanningConnect_DocumentedFailureMode(t *testing.T) {
	ctx := ctx()
	defer belt.Flush(ctx)
	c := newRTMPSnooperConn(t, PortModePublishers)

	// Long app value: 200 'a' bytes pushes the AMF "app" *value* past
	// the 128-byte default chunk size so rtmpChunk splits it with a
	// 0xc3 continuation header inserted INSIDE the app value run.
	longApp := strings.Repeat("a", 200)
	payload := append(amf0String("connect"), amf0Number(1)...)
	payload = append(payload, amf0Object([]amfKV{
		{Key: "app", Value: amf0String(longApp)},
	})...)
	chunked := rtmpChunk(0, payload)

	hasContinuation := false
	for _, b := range chunked[12:] {
		if b == 0xc3 {
			hasContinuation = true
			break
		}
	}
	require.True(t, hasContinuation, "test premise: chunked payload must contain a 0xc3 continuation byte")

	_, _ = c.tryExtractRouteString(ctx, chunked)
	app, ok := c.ProtocolProperties[rtmpPropPendingApp].(string)
	require.True(t, ok, "documented behavior: the parser still stashes something even on chunk-spanning input")
	require.Contains(t, app, "\xc3",
		"documented failure mode no longer reproduces — the chunk-header-blind parser appears to "+
			"have been hardened. Update the scope doc-comment at the top of connection_proxied_rtmp.go "+
			"and either delete this negative-pin or rewrite it to assert the new contract.")
}

// TestRTMPSnooperSingleChunkConnect_Succeeds is the in-scope success
// pin: a connect AMF that fits in one 128-byte chunk parses correctly.
// This is the documented audience for the snooper (libav-originated
// short-route-path clients). If a refactor breaks this, we must catch it
// before deploy.
func TestRTMPSnooperSingleChunkConnect_Succeeds(t *testing.T) {
	ctx := ctx()
	defer belt.Flush(ctx)
	c := newRTMPSnooperConn(t, PortModePublishers)

	rp, err := c.tryExtractRouteString(ctx, connectChunk("pixel"))
	require.NoError(t, err)
	require.Nil(t, rp, "connect alone must not finalize the route path")

	rp, err = c.tryExtractRouteString(ctx, publishChunk("dji-osmo-pocket-3-merged"))
	require.NoError(t, err)
	require.NotNil(t, rp)
	require.Equal(t, "pixel/dji-osmo-pocket-3-merged", string(*rp))
}

// TestRTMPParseConnectApp_SkipsNonStringProperties pins the hardened
// behavior for connect AMFs that interleave numeric/boolean properties
// before "app". Before the fix the parser short-circuited on the first
// non-string property; now it skips them and keeps scanning for "app".
func TestRTMPParseConnectApp_SkipsNonStringProperties(t *testing.T) {
	ctx := ctx()
	defer belt.Flush(ctx)

	// Build a connect AMF where objectEncoding (number) and audioCodecs
	// (number) precede "app" in the object — a layout legal under AMF0
	// and emitted by some non-libav clients. The parser must reach "app"
	// rather than erroring out on the first non-string value.
	payload := append(amf0String("connect"), amf0Number(1)...)
	payload = append(payload, amf0Object([]amfKV{
		{Key: "objectEncoding", Value: amf0Number(0)},
		{Key: "audioCodecs", Value: amf0Number(3575)},
		{Key: "app", Value: amf0String("pixel")},
	})...)
	chunked := rtmpChunk(0, payload)

	c := newRTMPSnooperConn(t, PortModePublishers)
	rp, err := c.tryExtractRouteString(ctx, chunked)
	require.NoError(t, err)
	require.Nil(t, rp, "connect alone must not finalize the route path")

	// Confirm the app was stashed correctly.
	require.Equal(t, "pixel", c.ProtocolProperties[rtmpPropPendingApp])
}

// FuzzRTMPSnooper_NoPanic feeds random byte slices to the parser to
// catch index-bound or infinite-loop regressions. The contract is "no
// panic"; both nil and non-nil returns are acceptable.
func FuzzRTMPSnooper_NoPanic(f *testing.F) {
	// Seed corpus: realistic shapes the parser was designed for.
	f.Add(connectChunk("pixel"))
	f.Add(publishChunk("dji-osmo-pocket-3-merged"))
	f.Add(playChunk("merged"))
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0x02, 0x00, 0x07, 'c', 'o', 'n', 'n', 'e', 'c', 't'})

	f.Fuzz(func(t *testing.T, msg []byte) {
		c := newRTMPSnooperConn(t, PortModePublishers)
		ctx := ctx()
		// We accept any (rp, err) return; the only assertion is that
		// the parser does not panic and returns within bounded work.
		_, _ = c.tryExtractRouteString(ctx, msg)
	})
}
