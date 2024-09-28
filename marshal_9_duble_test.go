package gocql_test

import (
	"math"
	"testing"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
)

func TestMarshalDouble(t *testing.T) {
	tType := gocql.NewNativeType(4, gocql.TypeDouble, "")

	marshal := func(i interface{}) ([]byte, error) { return gocql.Marshal(tType, i) }
	unmarshal := func(bytes []byte, i interface{}) error {
		return gocql.Unmarshal(tType, bytes, i)
	}

	serialization.PositiveSet{
		Data:   nil,
		Values: mod.Values{(*float64)(nil)}.AddVariants(mod.CustomType),
	}.Run("[nil]nullable", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data:   nil,
		Values: mod.Values{float64(0)}.AddVariants(mod.CustomType),
	}.Run("[nil]unmarshal", t, nil, unmarshal)

	serialization.PositiveSet{
		Data:   make([]byte, 0),
		Values: mod.Values{float64(0)}.AddVariants(mod.All...),
	}.Run("[]unmarshal", t, nil, unmarshal)

	serialization.PositiveSet{
		Data:   []byte("\x00\x00\x00\x00\x00\x00\x00\x00"),
		Values: mod.Values{float64(0)}.AddVariants(mod.All...),
	}.Run("zeros", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data:   []byte("\x7f\xef\xff\xff\xff\xff\xff\xff"),
		Values: mod.Values{float64(math.MaxFloat64)}.AddVariants(mod.All...),
	}.Run("max", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data:   []byte("\x00\x00\x00\x00\x00\x00\x00\x01"),
		Values: mod.Values{float64(math.SmallestNonzeroFloat64)}.AddVariants(mod.All...),
	}.Run("smallest", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data:   []byte("\x7f\xf0\x00\x00\x00\x00\x00\x00"),
		Values: mod.Values{float64(math.Inf(1))}.AddVariants(mod.All...),
	}.Run("inf+", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data:   []byte("\xff\xf0\x00\x00\x00\x00\x00\x00"),
		Values: mod.Values{float64(math.Inf(-1))}.AddVariants(mod.All...),
	}.Run("inf-", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data:   []byte("\x7f\xf8\x00\x00\x00\x00\x00\x01"),
		Values: mod.Values{float64(math.NaN())}.AddVariants(mod.All...),
	}.Run("nan", t, marshal, unmarshal)
}
