// endpoint_pattern.go defines an EndpointPattern: a regexp-keyed
// endpoint template parsed and validated at config-load time.
//
// Pattern-body templates use the Go text/template delimiters
// `[[` ... `]]` instead of the default `{{` ... `}}`. Rationale:
// `pkg/configfile.Read` runs the entire YAML file through a
// text/template pass with the default `{{ }}` delimiters BEFORE
// `(*Config).UnmarshalYAML` ever sees the bytes. If pattern bodies
// also used `{{ }}`, any `{{ .Match.<name> }}` in a pattern body
// would render to `<no value>` at file-load time (the file-level
// pass has no `.Match` data) — and the named captures would be
// lost long before the resolver got a chance to render them.
// Distinct delimiters keep the file-level pass and the pattern-body
// pass independent: file-level helpers like
// `{{ firstNonEmpty .env.X "y" }}` keep working in both literal and
// pattern values, while pattern bodies reach for `[[ .Match.<name> ]]`
// only when they need a capture.

package config

import (
	"regexp"
	"text/template"
)

// EndpointPatternBodyLeftDelim and EndpointPatternBodyRightDelim are
// the Go template delimiters used for pattern bodies. They differ from
// the default `{{`/`}}` so the file-level template pass at
// pkg/configfile.Read does not consume pattern-body actions before
// UnmarshalYAML runs (see file-level comment for the full rationale).
const (
	EndpointPatternBodyLeftDelim  = "[["
	EndpointPatternBodyRightDelim = "]]"
)

// EndpointPatternKeyPrefix is the YAML key prefix that marks an entry
// in the `endpoints:` map as a regex pattern instead of a literal route
// path. Keys without this prefix are stored in Config.Endpoints; keys
// with this prefix have the prefix trimmed, the remainder compiled as
// a Go regular expression, and the YAML value re-marshaled and parsed
// as a text/template (rendered later when a concrete route path
// matches the regex).
const EndpointPatternKeyPrefix = "regexp:"

// EndpointPattern is an entry from the `endpoints:` map whose key
// begins with EndpointPatternKeyPrefix. The pattern is compiled and
// the value is parsed as a text/template at Config.Read time, so any
// regex or template syntax error fails the load fast instead of
// surfacing later when a publisher arrives.
type EndpointPattern struct {
	// Raw is the pattern string with EndpointPatternKeyPrefix stripped.
	Raw string

	// Regexp is the compiled form of Raw. Used to test concrete route
	// paths and extract named captures.
	Regexp *regexp.Regexp

	// Template is the pre-parsed text/template form of the original
	// YAML value, rendered against {.Match: <named captures>} at
	// resolve time and then unmarshaled as an EndpointConfig.
	Template *template.Template
}
