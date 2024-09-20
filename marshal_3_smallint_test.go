package gocql

import (
	"testing"

	"github.com/gocql/gocql/internal/tests/utils"
	"github.com/gocql/gocql/marshal/tests/mod"
	"github.com/gocql/gocql/marshal/tests/serialization"
)

func TestMarshalSmallint(t *testing.T) {
	marshal := func(i interface{}) ([]byte, error) { return Marshal(NativeType{proto: 4, typ: TypeSmallInt}, i) }
	unmarshal := func(bytes []byte, i interface{}) error {
		return Unmarshal(NativeType{proto: 4, typ: TypeSmallInt}, bytes, i)
	}

	brokenTypes := utils.GetTypes(mod.String(""), (*mod.String)(nil))

	serialization.Set{
		Data: nil,
		Values: mod.Values{
			(*int8)(nil), (*int16)(nil), (*int32)(nil), (*int64)(nil), (*int)(nil),
			(*uint8)(nil), (*uint16)(nil), (*uint32)(nil), (*uint64)(nil), (*uint)(nil), (*string)(nil),
		}.AddVariants(mod.CustomType),
	}.Run("[nil]refs", t, marshal, unmarshal)

	serialization.Set{
		Data: nil,
		Values: mod.Values{
			int8(0), int16(0), int32(0), int64(0), int(0),
			uint8(0), uint16(0), uint32(0), uint64(0), uint(0), "0",
		}.AddVariants(mod.CustomType),
		BrokenUnmarshalTypes: brokenTypes,
	}.Run("unmarshal nil data", t, nil, unmarshal)

	serialization.Set{
		Data: make([]byte, 0),
		Values: mod.Values{
			int8(0), int16(0), int32(0), int64(0), int(0),
			uint8(0), uint16(0), uint32(0), uint64(0), uint(0), "0",
		}.AddVariants(mod.All...),
		BrokenUnmarshalTypes: brokenTypes,
	}.Run("unmarshal zero data", t, nil, unmarshal)

	serialization.Set{
		Data: []byte("\x00\x00"),
		Values: mod.Values{
			int8(0), int16(0), int32(0), int64(0), int(0),
			uint8(0), uint16(0), uint32(0), uint64(0), uint(0), "0",
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenTypes,
		BrokenUnmarshalTypes: brokenTypes,
	}.Run("zeros", t, marshal, unmarshal)

	serialization.Set{
		Data:                 []byte("\x00\x7f"),
		Values:               mod.Values{int8(127), int16(127), int32(127), int64(127), int(127), "127"}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenTypes,
		BrokenUnmarshalTypes: brokenTypes,
	}.Run("127", t, marshal, unmarshal)

	serialization.Set{
		Data:                 []byte("\xff\x80"),
		Values:               mod.Values{int8(-128), int16(-128), int32(-128), int64(-128), int(-128), "-128"}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenTypes,
		BrokenUnmarshalTypes: brokenTypes,
	}.Run("-128", t, marshal, unmarshal)

	serialization.Set{
		Data:                 []byte("\x7f\xff"),
		Values:               mod.Values{int16(32767), int32(32767), int64(32767), int(32767), "32767"}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenTypes,
		BrokenUnmarshalTypes: brokenTypes,
	}.Run("32767", t, marshal, unmarshal)

	serialization.Set{
		Data:                 []byte("\x80\x00"),
		Values:               mod.Values{int16(-32768), int32(-32768), int64(-32768), int(-32768), "-32768"}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenTypes,
		BrokenUnmarshalTypes: brokenTypes,
	}.Run("-32768", t, marshal, unmarshal)

	serialization.Set{
		Data:   []byte("\x00\xff"),
		Values: mod.Values{uint8(255), uint16(255), uint32(255), uint64(255), uint(255)}.AddVariants(mod.All...),
	}.Run("255", t, marshal, unmarshal)

	serialization.Set{
		Data:   []byte("\xff\xff"),
		Values: mod.Values{uint16(65535), uint32(65535), uint64(65535), uint(65535)}.AddVariants(mod.All...),
	}.Run("65535", t, marshal, unmarshal)
}
