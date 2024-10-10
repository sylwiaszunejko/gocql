package serialization_test

import (
	"testing"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
)

func TestMarshalAscii(t *testing.T) {
	tType := gocql.NewNativeType(4, gocql.TypeAscii, "")

	marshal := func(i interface{}) ([]byte, error) {
		return gocql.Marshal(tType, i)
	}
	unmarshal := func(bytes []byte, i interface{}) error {
		return gocql.Unmarshal(tType, bytes, i)
	}

	// unmarshal `zero` data into ([]byte)(nil), (*[]byte)(*[nil])
	brokenZeroSlices := serialization.GetTypes(make([]byte, 0), (*[]byte)(nil))

	serialization.PositiveSet{
		Data: nil,
		Values: mod.Values{
			([]byte)(nil),
			(*[]byte)(nil),
			(*string)(nil),
		}.AddVariants(mod.CustomType),
	}.Run("[nil]nullable", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data:   nil,
		Values: mod.Values{""}.AddVariants(mod.CustomType),
	}.Run("[nil]unmarshal", t, nil, unmarshal)

	serialization.PositiveSet{
		Data:                 make([]byte, 0),
		Values:               mod.Values{make([]byte, 0), ""}.AddVariants(mod.All...),
		BrokenUnmarshalTypes: brokenZeroSlices,
	}.Run("[]unmarshal", t, nil, unmarshal)

	serialization.PositiveSet{
		Data:   []byte("test text string"),
		Values: mod.Values{[]byte("test text string"), "test text string"}.AddVariants(mod.All...),
	}.Run("text", t, nil, unmarshal)
}
