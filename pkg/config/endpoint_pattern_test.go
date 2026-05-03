// endpoint_pattern_test.go covers the regexp-prefixed endpoint patterns'
// load-time parsing: literal/regexp split, declaration order, validation.

package config

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xaionaro-go/avd/pkg/avd/types"
)

func TestConfig_Endpoints_LiteralOnly(t *testing.T) {
	src := `
ports_service: []
ports_streaming: []
endpoints:
  literal_a:
    forwardings: []
  literal_b:
    forwardings: []
`
	var cfg Config
	_, err := cfg.Read([]byte(src))
	require.NoError(t, err)
	assert.Len(t, cfg.Endpoints, 2, "both literal endpoints must populate the map")
	assert.Empty(t, cfg.Patterns, "no regex prefix => no patterns")
	_, ok := cfg.Endpoints[types.RoutePath("literal_a")]
	assert.True(t, ok, "literal_a expected in Endpoints map")
	_, ok = cfg.Endpoints[types.RoutePath("literal_b")]
	assert.True(t, ok, "literal_b expected in Endpoints map")
}

func TestConfig_Endpoints_MixedLiteralAndRegexp(t *testing.T) {
	src := `
ports_service: []
ports_streaming: []
endpoints:
  literal_first:
    forwardings: []
  "regexp:^cam/(?P<id>[0-9]+)$":
    forwardings: []
  literal_last:
    forwardings: []
  "regexp:^foo/(?P<who>.+)$":
    forwardings: []
`
	var cfg Config
	_, err := cfg.Read([]byte(src))
	require.NoError(t, err)
	require.Len(t, cfg.Endpoints, 2, "two literal endpoints expected")
	require.Len(t, cfg.Patterns, 2, "two regexp patterns expected")
	// Declaration order must be preserved: cam first, then foo.
	assert.Equal(t, "^cam/(?P<id>[0-9]+)$", cfg.Patterns[0].Raw)
	assert.Equal(t, "^foo/(?P<who>.+)$", cfg.Patterns[1].Raw)
	require.NotNil(t, cfg.Patterns[0].Regexp)
	require.NotNil(t, cfg.Patterns[0].Template)
	require.NotNil(t, cfg.Patterns[1].Regexp)
	require.NotNil(t, cfg.Patterns[1].Template)
}

func TestConfig_Endpoints_BadRegexp(t *testing.T) {
	src := `
ports_service: []
ports_streaming: []
endpoints:
  "regexp:[unclosed":
    forwardings: []
`
	var cfg Config
	_, err := cfg.Read([]byte(src))
	require.Error(t, err)
	assert.True(t,
		strings.Contains(err.Error(), "regexp") || strings.Contains(err.Error(), "compile"),
		"error must mention regex compilation failure: %v", err,
	)
}

func TestConfig_Endpoints_BadTemplate(t *testing.T) {
	// [[ is opened but never closed: invalid template. Pattern bodies
	// use [[ ... ]] delimiters (see endpoint_pattern.go).
	src := `
ports_service: []
ports_streaming: []
endpoints:
  "regexp:^bad/(?P<x>.+)$":
    forwardings:
      - destination:
          url: "rtmp://example/[[ .Match.x"
`
	var cfg Config
	_, err := cfg.Read([]byte(src))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "template")
}

func TestConfig_Endpoints_RegexpKeyTrimsPrefix(t *testing.T) {
	src := `
ports_service: []
ports_streaming: []
endpoints:
  "regexp:^cam/(?P<id>[0-9]+)$":
    forwardings: []
`
	var cfg Config
	_, err := cfg.Read([]byte(src))
	require.NoError(t, err)
	require.Len(t, cfg.Patterns, 1)
	assert.NotContains(t, cfg.Patterns[0].Raw, "regexp:", "Raw must not retain the prefix")
}
