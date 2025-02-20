//go:build unit
// +build unit

package serialization_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
)

func TestMarshalMapV2(t *testing.T) {
	elem := gocql.NewNativeType(2, gocql.TypeSmallInt, "")
	tType := gocql.NewCollectionType(gocql.NewNativeType(2, gocql.TypeMap, ""), elem, elem)

	refInt16 := func(v int16) *int16 { return &v }
	refModInt16 := func(v mod.Int16) *mod.Int16 { return &v }

	marshal := func(i interface{}) ([]byte, error) { return gocql.Marshal(tType, i) }
	unmarshal := func(bytes []byte, i interface{}) error {
		return gocql.Unmarshal(tType, bytes, i)
	}

	serialization.PositiveSet{
		Data: nil,
		Values: mod.Values{
			(map[int16]int16)(nil), (map[int16]*int16)(nil),
			(map[mod.Int16]mod.Int16)(nil), (map[mod.Int16]*mod.Int16)(nil),
			(*map[int16]int16)(nil), (*map[int16]*int16)(nil),
			(*map[mod.Int16]mod.Int16)(nil), (*map[mod.Int16]*mod.Int16)(nil),
		}.AddVariants(mod.CustomType),
	}.Run("[nil]nullable", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x00"),
		Values: mod.Values{
			make(map[int16]int16), make(map[int16]*int16),
			make(map[mod.Int16]mod.Int16), make(map[mod.Int16]*mod.Int16),
		}.AddVariants(mod.All...),
	}.Run("zero elems", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x01\x00\x00\x00\x00"),
		Values: mod.Values{
			map[int16]int16{0: 0}, map[int16]*int16{0: refInt16(0)},
			map[mod.Int16]mod.Int16{0: 0}, map[mod.Int16]*mod.Int16{0: refModInt16(0)},
		}.AddVariants(mod.All...),
	}.Run("[]{zero elem}unmarshal", t, nil, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x01\x00\x02\x00\x00\x00\x02\x00\x00"),
		Values: mod.Values{
			map[int16]int16{0: 0}, map[int16]*int16{0: refInt16(0)},
			map[mod.Int16]mod.Int16{0: 0}, map[mod.Int16]*mod.Int16{0: refModInt16(0)},
		}.AddVariants(mod.All...),
	}.Run("[]{0:0}", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x01\x00\x02\x7f\xff\x00\x02\x7f\xff"),
		Values: mod.Values{
			map[int16]int16{32767: 32767}, map[int16]*int16{32767: refInt16(32767)},
			map[mod.Int16]mod.Int16{32767: 32767}, map[mod.Int16]*mod.Int16{32767: refModInt16(32767)},
		}.AddVariants(mod.All...),
	}.Run("[]{max:max}", t, marshal, unmarshal)
}

func TestMarshalMapV2Max(t *testing.T) {
	t.Parallel()
	elem := gocql.NewNativeType(2, gocql.TypeSmallInt, "")
	tType := gocql.NewCollectionType(gocql.NewNativeType(2, gocql.TypeMap, ""), elem, elem)

	elems := math.MaxUint16

	data := make([]byte, 0, elems*4+2)
	data = append(data, 255, 255)
	uintData := func(v uint) (byte, byte) {
		return byte(v >> 8), byte(v)
	}
	for v := 0; v < elems; v++ {
		b1, b2 := uintData(uint(v))
		data = append(data, 0, 2, b1, b2, 0, 2, 0, 1)
	}

	values := []func() interface{}{
		func() interface{} {
			out := make(map[int16]int16, elems)
			for i := 0; i < elems; i++ {
				out[int16(i)] = int16(1)
			}
			return out
		},
		func() interface{} {
			out := make(map[int16]*int16, elems)
			for i := 0; i < elems; i++ {
				tmp := int16(1)
				out[int16(i)] = &tmp
			}
			return out
		},
		func() interface{} {
			out := make(map[mod.Int16]mod.Int16, elems)
			for i := 0; i < elems; i++ {
				out[mod.Int16(i)] = mod.Int16(1)
			}
			return out
		},
		func() interface{} {
			out := make(map[mod.Int16]*mod.Int16, elems)
			for i := 0; i < elems; i++ {
				tmp := mod.Int16(1)
				out[mod.Int16(i)] = &tmp
			}
			return out
		},
	}
	unmarshal := func(bytes []byte, i interface{}) error {
		return gocql.Unmarshal(tType, bytes, i)
	}

	for _, v := range values {
		value := v()
		name := fmt.Sprintf("%T", value)

		serialization.PositiveSet{
			Data:   data,
			Values: mod.Values{value}.AddVariants(mod.All...),
		}.Run(name, t, nil, unmarshal)
	}
}
