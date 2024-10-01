package serialization

import (
	"errors"
	"fmt"
)

var unmarshalErr = errors.New("unmarshal unexpectedly failed with error")
var marshalErr = errors.New("marshal unexpectedly failed with error")

type unequalError struct {
	Expected string
	Got      string
}

func (e unequalError) Error() string {
	return fmt.Sprintf("expect %s but got %s", e.Expected, e.Got)
}

type panicErr struct {
	err   error
	stack []byte
}

func (e panicErr) Error() string {
	return fmt.Sprintf("%v\n%s", e.err, e.stack)
}
