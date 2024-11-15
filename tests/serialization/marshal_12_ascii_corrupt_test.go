//go:build all || unit
// +build all unit

package serialization_test

import (
	"testing"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
)

func TestMarshalAsciiMustFail(t *testing.T) {
	tType := gocql.NewNativeType(4, gocql.TypeAscii, "")

	marshal := func(i interface{}) ([]byte, error) {
		return gocql.Marshal(tType, i)
	}
	unmarshal := func(bytes []byte, i interface{}) error {
		return gocql.Unmarshal(tType, bytes, i)
	}

	// according to the 'cql protocol' - 'ascii' a sequence of bytes in the ASCII range [0, 127].
	// marshal and unmarshal functions does not return an error if bytes have values outside of this range.
	brokenAllTypes := serialization.GetTypes(mod.Values{[]byte{}, ""}.AddVariants(mod.All...)...)

	serialization.NegativeMarshalSet{
		Values: mod.Values{
			[]byte{255},
			[]byte{127, 255, 127},
			string([]byte{255}),
			string([]byte{127, 255, 127}),
		}.AddVariants(mod.All...),
		BrokenTypes: brokenAllTypes,
	}.Run("corrupt_vals", t, marshal)

	serialization.NegativeUnmarshalSet{
		Data:        []byte{255},
		Values:      mod.Values{[]byte{}, ""}.AddVariants(mod.All...),
		BrokenTypes: brokenAllTypes,
	}.Run("corrupt_data1", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data:        []byte{127, 255, 127},
		Values:      mod.Values{[]byte{}, ""}.AddVariants(mod.All...),
		BrokenTypes: brokenAllTypes,
	}.Run("corrupt_data2", t, unmarshal)
}
