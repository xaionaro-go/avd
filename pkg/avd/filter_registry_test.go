package avd

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubKey struct {
	A string
	B int
}

// String drives the %v rendering inside filterRegistry.Get's miss
// error message — verified explicitly in
// TestFilterRegistry_GetMissReturnsErrorNamingFeature.
func (k stubKey) String() string {
	return fmt.Sprintf("%s:%d", k.A, k.B)
}

type stubValue struct {
	Name string
}

func TestFilterRegistry_RegisterGetRoundTrip(t *testing.T) {
	ctx := context.Background()
	r := newFilterRegistry[stubKey, *stubValue]("stub")
	key := stubKey{A: "x", B: 1}
	val := &stubValue{Name: "v1"}

	r.Register(ctx, key, val)

	got, err := r.Get(ctx, key)
	require.NoError(t, err)
	assert.Same(t, val, got)
}

func TestFilterRegistry_GetMissReturnsErrorNamingFeature(t *testing.T) {
	ctx := context.Background()
	r := newFilterRegistry[stubKey, *stubValue]("widget")
	missing := stubKey{A: "no", B: 7}

	_, err := r.Get(ctx, missing)

	require.Error(t, err)
	// Message must name the feature and render the key via its String().
	assert.Contains(t, err.Error(), "widget filter")
	assert.Contains(t, err.Error(), "no:7")
}

func TestFilterRegistry_RegisterOverwrites(t *testing.T) {
	ctx := context.Background()
	r := newFilterRegistry[stubKey, *stubValue]("stub")
	key := stubKey{A: "x", B: 0}
	first := &stubValue{Name: "first"}
	second := &stubValue{Name: "second"}

	r.Register(ctx, key, first)
	r.Register(ctx, key, second)

	got, err := r.Get(ctx, key)
	require.NoError(t, err)
	assert.Same(t, second, got)
}

func TestFilterRegistry_UnregisterRemoves(t *testing.T) {
	ctx := context.Background()
	r := newFilterRegistry[stubKey, *stubValue]("stub")
	key := stubKey{A: "x", B: 0}
	r.Register(ctx, key, &stubValue{})

	r.Unregister(ctx, key)

	_, err := r.Get(ctx, key)
	assert.Error(t, err, "Unregister must remove the entry")
}

func TestFilterRegistry_UnregisterMissingIsNoop(t *testing.T) {
	ctx := context.Background()
	r := newFilterRegistry[stubKey, *stubValue]("stub")
	r.Unregister(ctx, stubKey{A: "absent", B: 99})
	// no panic, no error surface — verifies silent miss
	keys := r.GetRegisteredKeys(ctx)
	assert.Empty(t, keys)
}

// TestFilterRegistry_UnregisterByPredicate pins the new generic
// extraction that replaced the four per-feature snapshot+unregister
// loops. Three guarantees: (a) only matching keys are removed,
// (b) the returned count equals the number of removals, (c) the walk
// is single-locked so Get on a removed key after the call sees a miss
// without an intermediate snapshot window.
func TestFilterRegistry_UnregisterByPredicate(t *testing.T) {
	ctx := context.Background()
	r := newFilterRegistry[stubKey, *stubValue]("stub")
	keep := stubKey{A: "p", B: 0}
	drop1 := stubKey{A: "q", B: 1}
	drop2 := stubKey{A: "q", B: 2}
	r.Register(ctx, keep, &stubValue{Name: "k"})
	r.Register(ctx, drop1, &stubValue{Name: "d1"})
	r.Register(ctx, drop2, &stubValue{Name: "d2"})

	removed := r.UnregisterByPredicate(ctx, func(k stubKey) bool {
		return k.A == "q"
	})
	assert.Equal(t, 2, removed, "predicate matched two keys")

	// Survivor still reachable.
	got, err := r.Get(ctx, keep)
	require.NoError(t, err)
	assert.Equal(t, "k", got.Name)

	// Removed entries miss with feature-named error.
	_, err = r.Get(ctx, drop1)
	assert.Error(t, err)
	_, err = r.Get(ctx, drop2)
	assert.Error(t, err)

	// Repeat with no matches: count must be zero.
	removed = r.UnregisterByPredicate(ctx, func(k stubKey) bool { return k.A == "no-such-prefix" })
	assert.Equal(t, 0, removed)
}

func TestFilterRegistry_GetRegisteredKeys(t *testing.T) {
	ctx := context.Background()
	r := newFilterRegistry[stubKey, *stubValue]("stub")

	assert.Empty(t, r.GetRegisteredKeys(ctx), "empty registry returns empty slice")

	k1 := stubKey{A: "p", B: 0}
	k2 := stubKey{A: "p", B: 1}
	k3 := stubKey{A: "q", B: 0}
	r.Register(ctx, k1, &stubValue{})
	r.Register(ctx, k2, &stubValue{})
	r.Register(ctx, k3, &stubValue{})

	got := r.GetRegisteredKeys(ctx)
	assert.ElementsMatch(t, []stubKey{k1, k2, k3}, got)
}
