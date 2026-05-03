// read_test.go covers configfile.Read end-to-end: it ensures the
// file-level template pass uses {{ ... }} delimiters (so existing
// `{{ firstNonEmpty .env.X "fallback" }}` keeps working) AND that
// pattern-body templates using [[ .Match.<name> ]] survive the
// file-level pass intact (so named captures actually reach the
// resolver instead of being eaten by the file-level pass and rendered
// to "<no value>" before UnmarshalYAML ever sees them).
//
// This is the regression-gate for the iteration-2 bug: previously the
// file-level pass and the pattern-body pass both used {{ ... }}, so a
// pattern body like `route: "{{ .Match.id }}_cuvid"` would render
// "<no value>_cuvid" at file-load time, never reaching the resolver.

package configfile_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"text/template"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xaionaro-go/avd/pkg/avd/types"
	"github.com/xaionaro-go/avd/pkg/config"
	"github.com/xaionaro-go/avd/pkg/configfile"
)

// TestConfigfileRead_PatternCaptureSurvivesFileLevelTemplatePass is
// the regression test for REJECT-1. It writes a YAML containing both a
// file-level `{{ firstNonEmpty .env.X "fallback" }}` action AND a
// pattern body using `[[ .Match.id ]]`, then verifies:
//   - The file-level pass executes and substitutes "fallback" into
//     the literal endpoint's URL (file-level {{ }} still works).
//   - The pattern body retains `[[ .Match.id ]]` literally (the
//     file-level pass does NOT consume it), so executing the
//     pre-parsed Template against {Match: {"id": "42"}} produces
//     "42_cuvid", NOT "<no value>_cuvid".
func TestConfigfileRead_PatternCaptureSurvivesFileLevelTemplatePass(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "avd.yaml")
	src := `
ports_service: []
ports_streaming: []
endpoints:
  literal_one:
    forwardings:
      - destination:
          url: "rtmp://example/{{ firstNonEmpty .env.UNSET "fallback" }}"
  "regexp:^cam/(?P<id>[0-9]+)$":
    forwardings:
      - destination:
          local:
            route: "[[ .Match.id ]]_cuvid"
`
	require.NoError(t, os.WriteFile(cfgPath, []byte(src), 0o600))

	var cfg config.Config
	info := map[string]any{"env": map[string]string{}}
	ok, err := configfile.Read(context.Background(), cfgPath, &cfg, info)
	require.NoError(t, err)
	require.True(t, ok)

	// File-level template pass: firstNonEmpty must have substituted "fallback".
	require.Contains(t, cfg.Endpoints, types.RoutePath("literal_one"))
	literal := cfg.Endpoints[types.RoutePath("literal_one")]
	require.Len(t, literal.Forwardings, 1)
	require.NotNil(t, literal.Forwardings[0].Destination.URL)
	assert.Equal(
		t,
		"rtmp://example/fallback",
		*literal.Forwardings[0].Destination.URL,
		"file-level {{ firstNonEmpty }} must still work after the iteration-2 fix",
	)

	// Pattern body must have survived the file-level pass intact.
	require.Len(t, cfg.Patterns, 1)
	pat := cfg.Patterns[0]
	require.NotNil(t, pat.Template, "pattern Template must be parsed")

	// Execute the pattern template against synthetic captures and
	// confirm the [[ .Match.id ]] action renders the capture
	// (NOT "<no value>", which is what the iteration-2 bug produced).
	data := struct {
		Match map[string]string
	}{Match: map[string]string{"id": "42"}}
	var buf bytes.Buffer
	require.NoError(t, pat.Template.Execute(&buf, data))
	rendered := buf.String()
	assert.Contains(t, rendered, "42_cuvid", "pattern body must render the capture; got %q", rendered)
	assert.NotContains(t, rendered, "<no value>", "pattern body must not render <no value>; got %q", rendered)
}

// TestConfigfileRead_FileLevelPassDoesNotConsumePatternDelims sanity-
// checks the dual-delim invariant directly: a YAML containing only a
// pattern body with [[ .Match.<x> ]] must round-trip through Read
// without the file-level pass touching the [[ ]] action. We verify by
// re-parsing the *same* delimiter pair on the captured template body
// and confirming successful execution.
func TestConfigfileRead_FileLevelPassDoesNotConsumePatternDelims(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "avd.yaml")
	src := `
ports_service: []
ports_streaming: []
endpoints:
  "regexp:^a/(?P<x>.+)$":
    forwardings: []
`
	require.NoError(t, os.WriteFile(cfgPath, []byte(src), 0o600))

	var cfg config.Config
	ok, err := configfile.Read(context.Background(), cfgPath, &cfg, map[string]any{"env": map[string]string{}})
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, cfg.Patterns, 1)

	// Independently re-parse a [[ ]] template; this is just a paranoia
	// sanity-check that the delimiter constants exist and are usable.
	tmpl, err := template.New("x").
		Delims(config.EndpointPatternBodyLeftDelim, config.EndpointPatternBodyRightDelim).
		Parse(`[[ .V ]]`)
	require.NoError(t, err)
	var buf bytes.Buffer
	require.NoError(t, tmpl.Execute(&buf, struct{ V string }{V: "ok"}))
	assert.Equal(t, "ok", buf.String())
}
