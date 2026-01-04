// ptr.go provides a small utility for creating pointers from literal values.

package avd

func ptr[T any](in T) *T {
	return &in
}
