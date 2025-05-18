package types

import "fmt"

type ErrNotImplemented struct {
	Err error
}

func (e ErrNotImplemented) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("not implemented: %v", e.Err)
	}
	return "not implemented"
}

func (e ErrNotImplemented) Unwrap() error {
	return e.Err
}
