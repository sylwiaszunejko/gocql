package gocql_test

import (
	"math"
	"testing"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
)

func TestMarshalFloat(t *testing.T) {
	tType := gocql.NewNativeType(4, gocql.TypeFloat, "")

	marshal := func(i interface{}) ([]byte, error) { return gocql.Marshal(tType, i) }
	unmarshal := func(bytes []byte, i interface{}) error {
		return gocql.Unmarshal(tType, bytes, i)
	}

	serialization.PositiveSet{
		Data:   nil,
		Values: mod.Values{(*float32)(nil)}.AddVariants(mod.CustomType),
	}.Run("[nil]nullable", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data:   nil,
		Values: mod.Values{float32(0)}.AddVariants(mod.CustomType),
	}.Run("[nil]unmarshal", t, nil, unmarshal)

	serialization.PositiveSet{
		Data:   make([]byte, 0),
		Values: mod.Values{float32(0)}.AddVariants(mod.All...),
	}.Run("[]unmarshal", t, nil, unmarshal)

	serialization.PositiveSet{
		Data:   []byte("\x00\x00\x00\x00"),
		Values: mod.Values{float32(0)}.AddVariants(mod.All...),
	}.Run("zeros", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data:   []byte("\x7f\x7f\xff\xff"),
		Values: mod.Values{float32(math.MaxFloat32)}.AddVariants(mod.All...),
	}.Run("max", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data:   []byte("\x00\x00\x00\x01"),
		Values: mod.Values{float32(math.SmallestNonzeroFloat32)}.AddVariants(mod.All...),
	}.Run("smallest", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data:   []byte("\x7f\x80\x00\x00"),
		Values: mod.Values{float32(math.Inf(1))}.AddVariants(mod.All...),
	}.Run("inf+", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data:   []byte("\xff\x80\x00\x00"),
		Values: mod.Values{float32(math.Inf(-1))}.AddVariants(mod.All...),
	}.Run("inf-", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data:   []byte("\x7f\xc0\x00\x00"),
		Values: mod.Values{float32(math.NaN())}.AddVariants(mod.All...),
	}.Run("nan", t, marshal, unmarshal)
}
