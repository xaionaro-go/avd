// template_funcs_test.go covers dict, list, and firstNonEmpty template helpers.

package configfile

import (
	"bytes"
	"encoding/json"
	"testing"
	"text/template"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// renderTemplate parses src using TemplateFuncs and executes it with no data.
func renderTemplate(t *testing.T, src string) (string, error) {
	t.Helper()
	tmpl, err := template.New("test").Funcs(TemplateFuncs).Parse(src)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, nil); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func TestTemplateFuncs_Dict_HappyPath(t *testing.T) {
	out, err := renderTemplate(t, `{{ index (dict "a" 1 "b" 2) "a" }}`)
	require.NoError(t, err)
	assert.Equal(t, "1", out)

	out, err = renderTemplate(t, `{{ index (dict "a" 1 "b" 2) "b" }}`)
	require.NoError(t, err)
	assert.Equal(t, "2", out)
}

func TestTemplateFuncs_Dict_EmptyArgs(t *testing.T) {
	out, err := renderTemplate(t, `{{ len (dict) }}`)
	require.NoError(t, err)
	assert.Equal(t, "0", out)
}

func TestTemplateFuncs_Dict_OddArgsErrors(t *testing.T) {
	_, err := renderTemplate(t, `{{ dict "a" }}`)
	require.Error(t, err, "dict with odd argument count must error")
	assert.Contains(t, err.Error(), "even number of arguments")
}

func TestTemplateFuncs_Dict_NonStringKeyErrors(t *testing.T) {
	_, err := renderTemplate(t, `{{ dict 1 "a" }}`)
	require.Error(t, err, "dict with non-string key must error")
	assert.Contains(t, err.Error(), "key at position 0")
}

func TestTemplateFuncs_List_HappyPath(t *testing.T) {
	out, err := renderTemplate(t, `{{ range list "a" "b" "c" }}{{ . }},{{ end }}`)
	require.NoError(t, err)
	assert.Equal(t, "a,b,c,", out)
}

func TestTemplateFuncs_List_Empty(t *testing.T) {
	out, err := renderTemplate(t, `{{ len (list) }}`)
	require.NoError(t, err)
	assert.Equal(t, "0", out)
}

func TestTemplateFuncs_List_MixedTypes(t *testing.T) {
	out, err := renderTemplate(t, `{{ index (list "x" 42 "y") 1 }}`)
	require.NoError(t, err)
	assert.Equal(t, "42", out)
}

func TestTemplateFuncs_Mul_HappyPath(t *testing.T) {
	// String + int argument: parsed as numbers, multiplied as float64,
	// rendered as "144" (Go's default float-format strips trailing zero).
	out, err := renderTemplate(t, `{{ mul "16" 9 }}`)
	require.NoError(t, err)
	assert.Equal(t, "144", out)
}

func TestTemplateFuncs_Mul_FloatArg(t *testing.T) {
	// 0.5 * 4 == 2.0; result is float64.
	out, err := renderTemplate(t, `{{ mul 0.5 4 }}`)
	require.NoError(t, err)
	assert.Equal(t, "2", out)
}

func TestTemplateFuncs_Mul_ParseErrors(t *testing.T) {
	_, err := renderTemplate(t, `{{ mul "abc" 1 }}`)
	require.Error(t, err, "mul with unparsable string must error")
}

func TestTemplateFuncs_Div_TrueDivision(t *testing.T) {
	// 20 / 3 must produce a *fraction* — NOT integer truncation.
	// This is load-bearing: ceil(div(7680,9)) only works when div
	// preserves the remainder.
	out, err := renderTemplate(t, `{{ div 20 3 }}`)
	require.NoError(t, err)
	assert.Equal(t, "6.666666666666667", out)
}

func TestTemplateFuncs_Div_StringArgs(t *testing.T) {
	// 7680 / 9 == 853.333...; preserves fractional part.
	out, err := renderTemplate(t, `{{ div "7680" "9" }}`)
	require.NoError(t, err)
	assert.Equal(t, "853.3333333333334", out)
}

func TestTemplateFuncs_Div_ZeroDivisorErrors(t *testing.T) {
	_, err := renderTemplate(t, `{{ div 1 0 }}`)
	require.Error(t, err, "div by zero must error")
}

func TestTemplateFuncs_Div_ParseErrors(t *testing.T) {
	_, err := renderTemplate(t, `{{ div "abc" 1 }}`)
	require.Error(t, err, "div with unparsable string must error")
}

func TestTemplateFuncs_Ceil_FractionalRoundsUp(t *testing.T) {
	out, err := renderTemplate(t, `{{ ceil 853.3333 }}`)
	require.NoError(t, err)
	assert.Equal(t, "854", out)
}

func TestTemplateFuncs_Ceil_IntegerInputUnchanged(t *testing.T) {
	out, err := renderTemplate(t, `{{ ceil 853 }}`)
	require.NoError(t, err)
	assert.Equal(t, "853", out)
}

func TestTemplateFuncs_Ceil_WholeFloatUnchanged(t *testing.T) {
	out, err := renderTemplate(t, `{{ ceil 853.0 }}`)
	require.NoError(t, err)
	assert.Equal(t, "853", out)
}

func TestTemplateFuncs_Ceil_StringFloatRoundsUp(t *testing.T) {
	out, err := renderTemplate(t, `{{ ceil "1.5" }}`)
	require.NoError(t, err)
	assert.Equal(t, "2", out)
}

func TestTemplateFuncs_Ceil_ParseErrors(t *testing.T) {
	_, err := renderTemplate(t, `{{ ceil "abc" }}`)
	require.Error(t, err, "ceil with unparsable string must error")
}

// Composition test that mirrors the ~/.avd.conf use case.
func TestTemplateFuncs_Compose_CeilDivMul_Exact(t *testing.T) {
	// 1080*16 / 9 == 1920.0 exactly; ceil == 1920.
	out, err := renderTemplate(t, `{{ ceil (div (mul 1080 16) 9) }}`)
	require.NoError(t, err)
	assert.Equal(t, "1920", out)
}

func TestTemplateFuncs_Compose_CeilDivMul_RoundsUp_480(t *testing.T) {
	// 480*16 / 9 == 853.333...; ceil == 854. Load-bearing.
	out, err := renderTemplate(t, `{{ ceil (div (mul 480 16) 9) }}`)
	require.NoError(t, err)
	assert.Equal(t, "854", out)
}

func TestTemplateFuncs_Compose_CeilDivMul_RoundsUp_240(t *testing.T) {
	// 240*16 / 9 == 426.666...; ceil == 427.
	out, err := renderTemplate(t, `{{ ceil (div (mul 240 16) 9) }}`)
	require.NoError(t, err)
	assert.Equal(t, "427", out)
}

// TestToFloat64_BroadenedTypeCoverage pins the post-fix contract that
// toFloat64 now accepts every integer width (signed + unsigned), every
// float width, json.Number (via fmt.Stringer), and still rejects nil
// and exotic types like complex128. Before the fix the helper only
// accepted string/int/int64/float64, which silently rejected uint64
// (common in YAML), int32 (common in protobuf shims), float32 (common
// in image-processing flows), and json.Number (common in JSON configs).
func TestToFloat64_BroadenedTypeCoverage(t *testing.T) {
	cases := []struct {
		name    string
		in      any
		want    float64
		wantErr bool
	}{
		{"string", "1.5", 1.5, false},
		{"int", int(2), 2, false},
		{"int8", int8(3), 3, false},
		{"int16", int16(4), 4, false},
		{"int32", int32(5), 5, false},
		{"int64", int64(6), 6, false},
		{"uint", uint(7), 7, false},
		{"uint8", uint8(8), 8, false},
		{"uint16", uint16(9), 9, false},
		{"uint32", uint32(10), 10, false},
		{"uint64", uint64(11), 11, false},
		{"float32", float32(12.5), 12.5, false},
		{"float64", float64(13.5), 13.5, false},
		{"json.Number", json.Number("14.25"), 14.25, false},
		{"nil", nil, 0, true},
		{"unparsable string", "abc", 0, true},
		{"complex128 unsupported", complex(1, 2), 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := toFloat64(tc.in)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestTemplateFuncs_FirstNonEmpty_PreservedBehavior(t *testing.T) {
	// firstNonEmpty returns the first arg with a valid reflect.Value;
	// "" has a valid string reflect.Value, so it currently returns ""
	// (rather than skipping to "fallback"). Lock that exact behaviour
	// to detect any future regression that would change the semantics.
	out, err := renderTemplate(t, `{{ firstNonEmpty "" "fallback" }}`)
	require.NoError(t, err)
	assert.Equal(t, "", out, "firstNonEmpty(\"\", \"fallback\") must return \"\"; got %q", out)

	// And a positive case: a nil first arg has an invalid reflect.Value
	// and is skipped, so the "fallback" string must be returned.
	out, err = renderTemplate(t, `{{ firstNonEmpty nil "fallback" }}`)
	require.NoError(t, err)
	assert.Equal(t, "fallback", out, "firstNonEmpty(nil, \"fallback\") must return \"fallback\"; got %q", out)
}
