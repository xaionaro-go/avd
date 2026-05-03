// config.go defines the configuration structures for AVD.

// Package config provides the configuration structures for AVD.
package config

import (
	"fmt"
	"regexp"
	"strings"
	"text/template"

	"github.com/goccy/go-yaml"
	"github.com/xaionaro-go/avd/pkg/avd/types"
	"github.com/xaionaro-go/avd/pkg/configfile"
)

type Config struct {
	ServicePorts   []ServicePortConfig                `yaml:"ports_service"`
	StreamingPorts []StreamingPortConfig              `yaml:"ports_streaming"`
	Endpoints      map[types.RoutePath]EndpointConfig `yaml:"endpoints"`
	// Patterns holds the ordered list of regexp-prefixed endpoint
	// entries. Keys in YAML `endpoints:` that begin with
	// EndpointPatternKeyPrefix populate this slice in declaration
	// order; the remainder populate Endpoints. Patterns is not
	// re-emitted by MarshalYAML — the regex/template pair is
	// load-only and round-trip serialization is not required (the
	// YAML source remains the source of truth).
	Patterns []EndpointPattern `yaml:"-"`
}

// configRaw mirrors Config but keeps the endpoints field as an ordered
// MapSlice so we can route each entry to either Endpoints or Patterns
// according to its key prefix while preserving declaration order.
type configRaw struct {
	ServicePorts   []ServicePortConfig   `yaml:"ports_service"`
	StreamingPorts []StreamingPortConfig `yaml:"ports_streaming"`
	Endpoints      yaml.MapSlice         `yaml:"endpoints"`
}

// UnmarshalYAML splits the `endpoints:` mapping into literal and
// regexp-prefixed entries. Bad regex or template parse errors fail
// the load (no silent fallback): operators must see the failure when
// avd starts up, not later when an unrelated publisher arrives.
func (cfg *Config) UnmarshalYAML(b []byte) error {
	var raw configRaw
	if err := yaml.Unmarshal(b, &raw); err != nil {
		return err
	}
	cfg.ServicePorts = raw.ServicePorts
	cfg.StreamingPorts = raw.StreamingPorts
	cfg.Endpoints = nil
	cfg.Patterns = nil
	for _, item := range raw.Endpoints {
		keyStr, ok := item.Key.(string)
		if !ok {
			return fmt.Errorf("endpoint key must be a string, got %T", item.Key)
		}
		valBytes, err := yaml.Marshal(item.Value)
		if err != nil {
			return fmt.Errorf("re-marshal endpoint %q: %w", keyStr, err)
		}
		if strings.HasPrefix(keyStr, EndpointPatternKeyPrefix) {
			pattern, err := newEndpointPattern(keyStr, valBytes)
			if err != nil {
				return err
			}
			cfg.Patterns = append(cfg.Patterns, pattern)
			continue
		}
		var ep EndpointConfig
		if err := yaml.Unmarshal(valBytes, &ep); err != nil {
			return fmt.Errorf("decode endpoint %q: %w", keyStr, err)
		}
		if cfg.Endpoints == nil {
			cfg.Endpoints = map[types.RoutePath]EndpointConfig{}
		}
		cfg.Endpoints[types.RoutePath(keyStr)] = ep
	}
	return nil
}

// newEndpointPattern compiles the regex behind keyStr and pre-parses
// valBytes as a text/template. Both errors are returned with enough
// context for an operator to identify the offending pattern.
func newEndpointPattern(keyStr string, valBytes []byte) (EndpointPattern, error) {
	rawPattern := strings.TrimPrefix(keyStr, EndpointPatternKeyPrefix)
	re, err := regexp.Compile(rawPattern)
	if err != nil {
		return EndpointPattern{}, fmt.Errorf("compile endpoint regexp %q: %w", rawPattern, err)
	}
	tmpl, err := template.New("endpoint:" + rawPattern).
		Delims(EndpointPatternBodyLeftDelim, EndpointPatternBodyRightDelim).
		Funcs(configfile.TemplateFuncs).
		Parse(string(valBytes))
	if err != nil {
		return EndpointPattern{}, fmt.Errorf("parse endpoint template for %q: %w", rawPattern, err)
	}
	return EndpointPattern{Raw: rawPattern, Regexp: re, Template: tmpl}, nil
}
