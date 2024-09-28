package serialization

import (
	"errors"
	"github.com/gocql/gocql/internal/tests/utils"
	"reflect"
	"runtime/debug"
	"testing"
)

// NegativeMarshalSet is a tool for marshal funcs testing for cases when the function should an error.
type NegativeMarshalSet struct {
	Values      []interface{}
	BrokenTypes []reflect.Type
}

func (s NegativeMarshalSet) Run(name string, t *testing.T, marshal func(interface{}) ([]byte, error)) {
	if name == "" {
		t.Fatal("name should be provided")
	}
	if marshal == nil {
		t.Fatal("marshal function should be provided")
	}
	t.Run(name, func(t *testing.T) {
		for m := range s.Values {
			val := s.Values[m]

			t.Run(utils.StringValue(val), func(t *testing.T) {
				_, err := func() (d []byte, err error) {
					defer func() {
						if r := recover(); r != nil {
							err = utils.PanicErr{Err: r.(error), Stack: debug.Stack()}
						}
					}()
					return marshal(val)
				}()

				testFailed := false
				wasPanic := errors.As(err, &utils.PanicErr{})
				if err == nil || wasPanic {
					testFailed = true
				}

				if isTypeOf(val, s.BrokenTypes) {
					if testFailed {
						t.Skipf("skipped bacause there is unsolved problem")
					}
					t.Fatalf("expected to panic or no error for (%T), but got an error", val)
				}

				if testFailed {
					if wasPanic {
						t.Fatalf("was panic %s", err)
					}
					t.Errorf("expected an error for (%T), but got no error", val)
				}
			})
		}
	})
}
