// ptr.go provides a small utility for creating pointers from literal values.

package types

func ptr[T any](v T) *T {
	return &v
}
