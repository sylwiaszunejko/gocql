package serialization

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"
	"testing"

	"github.com/gocql/gocql/internal/tests/utils"
)

type Sets []*Set

// Set is a tool for generating test cases of marshal and unmarshall funcs.
// For cases when the function should no error,
// marshaled data from Set.Values should be equal with Set.Data,
// unmarshalled value from Set.Data should be equal with Set.Values.
type Set struct {
	Data   []byte
	Values []interface{}

	BrokenMarshalTypes   []reflect.Type
	BrokenUnmarshalTypes []reflect.Type
}

func (s Set) Run(name string, t *testing.T, marshal func(interface{}) ([]byte, error), unmarshal func([]byte, interface{}) error) {
	if name == "" {
		t.Fatal("name should be provided")
	}

	t.Run(name, func(t *testing.T) {
		for i := range s.Values {
			val := s.Values[i]

			t.Run(fmt.Sprintf("%T", val), func(t *testing.T) {
				if marshal != nil {
					s.runMarshalTest(t, marshal, val)
				}

				if unmarshal != nil {
					if rt := reflect.TypeOf(val); rt.Kind() != reflect.Ptr {
						unmarshalIn := utils.NewRef(val)
						s.runUnmarshalTest("unmarshal", t, unmarshal, val, unmarshalIn)
					} else {
						// Test unmarshal to (*type)(nil)
						unmarshalIn := utils.NewRef(val)
						s.runUnmarshalTest("unmarshal**nil", t, unmarshal, val, unmarshalIn)

						// Test unmarshal to &type{}
						unmarshalInZero := utils.NewRefToZero(val)
						s.runUnmarshalTest("unmarshal**zero", t, unmarshal, val, unmarshalInZero)
					}
				}
			})
		}
	})
}

func (s Set) runMarshalTest(t *testing.T, f func(interface{}) ([]byte, error), val interface{}) {
	t.Run("marshal", func(t *testing.T) {

		result, err := func() (d []byte, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = utils.PanicErr{Err: r.(error), Stack: debug.Stack()}
				}
			}()
			return f(val)
		}()

		expected := bytes.Clone(s.Data)
		if err != nil {
			if !errors.As(err, &utils.PanicErr{}) {
				err = errors.Join(MarshalErr, err)
			}
		} else if !utils.EqualData(expected, result) {
			err = UnequalError{Expected: utils.StringData(s.Data), Got: utils.StringData(result)}
		}

		if isTypeOf(val, s.BrokenMarshalTypes) {
			if err == nil {
				t.Fatalf("expected to fail for (%T), but did not fail", val)
			}
			t.Skipf("skipped bacause there is unsolved problem")
		}
		if err != nil {
			t.Error(err)
		}
	})
}

func (s Set) runUnmarshalTest(name string, t *testing.T, f func([]byte, interface{}) error, expected, result interface{}) {
	t.Run(name, func(t *testing.T) {

		expectedPtr := getPointers(result)

		err := func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = utils.PanicErr{Err: fmt.Errorf("%s", r), Stack: debug.Stack()}
				}
			}()
			return f(bytes.Clone(s.Data), result)
		}()

		if err != nil {
			if !errors.As(err, &utils.PanicErr{}) {
				err = errors.Join(UnmarshalErr, err)
			}
		} else if !utils.EqualVals(expected, utils.DeReference(result)) {
			err = UnequalError{Expected: utils.StringValue(expected), Got: utils.StringValue(result)}
		} else {
			err = expectedPtr.Valid(result)
		}

		if isTypeOf(expected, s.BrokenUnmarshalTypes) {
			if err == nil {
				t.Fatalf("expected to fail for (%T), but did not fail", expected)
			}
			t.Skipf("skipped bacause there is unsolved problem")
		}
		if err != nil {
			t.Error(err)
		}
	})
}
