package serialization

import (
	"errors"
	"fmt"
	"reflect"
)

var UnmarshalErr = errors.New("unmarshal unexpectedly failed with error")
var MarshalErr = errors.New("marshal unexpectedly failed with error")

type UnequalError struct {
	Expected string
	Got      string
}

func (e UnequalError) Error() string {
	return fmt.Sprintf("expect %s but got %s", e.Expected, e.Got)
}

func isTypeOf(value interface{}, types []reflect.Type) bool {
	valueType := reflect.TypeOf(value)
	for i := range types {
		if types[i] == valueType {
			return true
		}
	}
	return false
}
