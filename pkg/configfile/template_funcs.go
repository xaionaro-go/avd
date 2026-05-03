// template_funcs.go provides template functions for configuration files.

package configfile

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"text/template"
)

// toFloat64 parses v as a float64. Accepted dynamic types:
//   - string: parsed via strconv.ParseFloat (int or float syntax both
//     accepted, matches the common regex-capture / yaml-scalar paths).
//   - any signed integer width (int, int8/16/32/64): widened.
//   - any unsigned integer width (uint, uint8/16/32/64): widened (a
//     uint64 above 2^53 silently loses precision in float64; callers
//     that need exact arithmetic should not use this helper).
//   - any float width (float32, float64): widened.
//   - fmt.Stringer: stringified then parsed as float (covers
//     json.Number and similar typed wrappers without a hard import).
//
// Used by the arithmetic template helpers (mul, div, ceil) so callers
// may pass values captured from regex matches (string), YAML scalars
// (int / float64 / json.Number), or Go literals interchangeably without
// the helper hard-coding every concrete numeric type.
func toFloat64(v any) (float64, error) {
	if v == nil {
		return 0, fmt.Errorf("unsupported numeric argument: nil")
	}
	if s, ok := v.(string); ok {
		n, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, fmt.Errorf("parse %q as number: %w", s, err)
		}
		return n, nil
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(rv.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(rv.Uint()), nil
	case reflect.Float32, reflect.Float64:
		return rv.Float(), nil
	}
	if s, ok := v.(fmt.Stringer); ok { // covers json.Number etc.
		n, err := strconv.ParseFloat(s.String(), 64)
		if err != nil {
			return 0, fmt.Errorf("parse %q (from %T) as number: %w", s.String(), v, err)
		}
		return n, nil
	}
	return 0, fmt.Errorf("unsupported numeric argument type %T", v)
}

// TemplateFuncs is the FuncMap shared by every text/template execution
// performed against avd configuration files. Exported so subordinate
// parsers (e.g. the regex-pattern endpoint resolver in pkg/config) can
// reuse the exact same helper set without forking the definitions.
var TemplateFuncs = template.FuncMap{
	"firstNonEmpty": func(args ...any) any {
		for _, arg := range args {
			v := reflect.ValueOf(arg)
			if v.IsValid() {
				return arg
			}
		}
		return nil
	},
	"dict": func(args ...any) (map[string]any, error) {
		if len(args)%2 != 0 {
			return nil, fmt.Errorf("dict requires an even number of arguments, got %d", len(args))
		}
		m := make(map[string]any, len(args)/2)
		for i := 0; i < len(args); i += 2 {
			k, ok := args[i].(string)
			if !ok {
				return nil, fmt.Errorf("dict key at position %d must be string, got %T", i, args[i])
			}
			m[k] = args[i+1]
		}
		return m, nil
	},
	"list": func(args ...any) []any {
		out := make([]any, len(args))
		copy(out, args)
		return out
	},
	// mul returns a*b as float64. a and b may be string (parsed as
	// number — int or float syntax accepted), int, int64, or float64.
	// Float result keeps composition with div lossless (no surprise
	// integer truncation between calls).
	//
	// Example use in a pattern body:
	//   width: [[ ceil (div (mul .Match.height 16) 9) ]]
	"mul": func(a, b any) (float64, error) {
		af, err := toFloat64(a)
		if err != nil {
			return 0, fmt.Errorf("mul: first arg: %w", err)
		}
		bf, err := toFloat64(b)
		if err != nil {
			return 0, fmt.Errorf("mul: second arg: %w", err)
		}
		return af * bf, nil
	},
	// div returns a/b as a *true* float64 quotient — NOT integer
	// truncation. Preserving the fraction matters for callers that
	// pipe the result through ceil to derive a derived dimension
	// (e.g. width = ceil(height*16/9)); integer truncation here would
	// silently drop the remainder and break the ceil. Errors on parse
	// failure or b == 0.
	"div": func(a, b any) (float64, error) {
		af, err := toFloat64(a)
		if err != nil {
			return 0, fmt.Errorf("div: first arg: %w", err)
		}
		bf, err := toFloat64(b)
		if err != nil {
			return 0, fmt.Errorf("div: second arg: %w", err)
		}
		if bf == 0 {
			return 0, fmt.Errorf("div: division by zero")
		}
		return af / bf, nil
	},
	// ceil returns the smallest integer >= x as int64, using math.Ceil
	// internally. x may be string (parsed as float), int, int64, or
	// float64. Errors only on parse / type failure.
	"ceil": func(x any) (int64, error) {
		f, err := toFloat64(x)
		if err != nil {
			return 0, fmt.Errorf("ceil: %w", err)
		}
		return int64(math.Ceil(f)), nil
	},
}
