package gocql_test

import (
	"math/big"
	"testing"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/utils"
	"github.com/gocql/gocql/marshal/tests/mod"
	"github.com/gocql/gocql/marshal/tests/serialization"
)

func TestMarshalCounter(t *testing.T) {
	tType := gocql.NewNativeType(4, gocql.TypeCounter, "")

	marshal := func(i interface{}) ([]byte, error) { return gocql.Marshal(tType, i) }
	unmarshal := func(bytes []byte, i interface{}) error {
		return gocql.Unmarshal(tType, bytes, i)
	}

	// unmarshal `custom string` unsupported
	brokenCustomStrings := utils.GetTypes(mod.String(""), (*mod.String)(nil))

	// marshal "" (empty string) unsupported
	// unmarshal nil value into (string)("0")
	brokenEmptyStrings := utils.GetTypes(string(""), mod.String(""))

	// marshal `custom string` unsupported
	// marshal `big.Int` unsupported
	brokenMarshalTypes := append(brokenCustomStrings, utils.GetTypes(big.Int{}, &big.Int{})...)

	// marshal data, which equal math.MaxUint64, into uint and uit64 leads to an error
	brokenUints := utils.GetTypes(uint(0), mod.Uint64(0), mod.Uint(0), (*uint)(nil), (*mod.Uint64)(nil), (*mod.Uint)(nil))

	serialization.PositiveSet{
		Data: nil,
		Values: mod.Values{
			(*int8)(nil), (*int16)(nil), (*int32)(nil), (*int64)(nil), (*int)(nil),
			(*uint8)(nil), (*uint16)(nil), (*uint32)(nil), (*uint64)(nil), (*uint)(nil),
			(*string)(nil), (*big.Int)(nil), "",
		}.AddVariants(mod.CustomType),
		BrokenMarshalTypes:   brokenEmptyStrings,
		BrokenUnmarshalTypes: brokenEmptyStrings,
	}.Run("[nil]nullable", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: nil,
		Values: mod.Values{
			int8(0), int16(0), int32(0), int64(0), int(0),
			uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
			"0", big.Int{},
		}.AddVariants(mod.CustomType),
		BrokenUnmarshalTypes: brokenCustomStrings,
	}.Run("[nil]unmarshal", t, nil, unmarshal)

	serialization.PositiveSet{
		Data: make([]byte, 0),
		Values: mod.Values{
			int8(0), int16(0), int32(0), int64(0), int(0),
			uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
			"0", *big.NewInt(0),
		}.AddVariants(mod.All...),
		BrokenUnmarshalTypes: brokenCustomStrings,
	}.Run("[]unmarshal", t, nil, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x00\x00\x00\x00\x00\x00\x00"),
		Values: mod.Values{
			int8(0), int16(0), int32(0), int64(0), int(0),
			uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
			"0", *big.NewInt(0),
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenMarshalTypes,
		BrokenUnmarshalTypes: brokenCustomStrings,
	}.Run("zeros", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x7f\xff\xff\xff\xff\xff\xff\xff"),
		Values: mod.Values{
			int64(9223372036854775807), int(9223372036854775807),
			"9223372036854775807", *big.NewInt(9223372036854775807),
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustomStrings,
		BrokenUnmarshalTypes: brokenCustomStrings,
	}.Run("max", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x80\x00\x00\x00\x00\x00\x00\x00"),
		Values: mod.Values{
			int64(-9223372036854775808), int(-9223372036854775808),
			"-9223372036854775808", *big.NewInt(-9223372036854775808),
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustomStrings,
		BrokenUnmarshalTypes: brokenCustomStrings,
	}.Run("min", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x00\x00\x00\x7f\xff\xff\xff"),
		Values: mod.Values{
			int32(2147483647), int64(2147483647), int(2147483647),
			"2147483647", *big.NewInt(2147483647),
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenMarshalTypes,
		BrokenUnmarshalTypes: brokenCustomStrings,
	}.Run("2147483647", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\xff\xff\xff\xff\x80\x00\x00\x00"),
		Values: mod.Values{
			int32(-2147483648), int64(-2147483648), int(-2147483648),
			"-2147483648", *big.NewInt(-2147483648),
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenMarshalTypes,
		BrokenUnmarshalTypes: brokenCustomStrings,
	}.Run("-2147483648", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x00\x00\x00\x00\x00\x7f\xff"),
		Values: mod.Values{
			int16(32767), int32(32767), int64(32767), int(32767),
			"32767", *big.NewInt(32767),
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenMarshalTypes,
		BrokenUnmarshalTypes: brokenCustomStrings,
	}.Run("32767", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\xff\xff\xff\xff\xff\xff\x80\x00"),
		Values: mod.Values{
			int16(-32768), int32(-32768), int64(-32768), int(-32768),
			"-32768", *big.NewInt(-32768),
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenMarshalTypes,
		BrokenUnmarshalTypes: brokenCustomStrings,
	}.Run("-32768", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x00\x00\x00\x00\x00\x00\x7f"),
		Values: mod.Values{
			int8(127), int16(127), int32(127), int64(127), int(127),
			"127", *big.NewInt(127),
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenMarshalTypes,
		BrokenUnmarshalTypes: brokenCustomStrings,
	}.Run("127", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\xff\xff\xff\xff\xff\xff\xff\x80"),
		Values: mod.Values{
			int8(-128), int16(-128), int32(-128), int64(-128), int(-128),
			"-128", *big.NewInt(-128),
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenMarshalTypes,
		BrokenUnmarshalTypes: brokenCustomStrings,
	}.Run("-128", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x00\x00\x00\x00\x00\x00\xff"),
		Values: mod.Values{
			uint8(255), uint16(255), uint32(255), uint64(255), uint(255),
		}.AddVariants(mod.All...),
	}.Run("255", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x00\x00\x00\x00\x00\xff\xff"),
		Values: mod.Values{
			uint16(65535), uint32(65535), uint64(65535), uint(65535),
		}.AddVariants(mod.All...),
	}.Run("65535", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x00\x00\x00\xff\xff\xff\xff"),
		Values: mod.Values{
			uint32(4294967295), uint64(4294967295), uint(4294967295),
		}.AddVariants(mod.All...),
	}.Run("4294967295", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\xff\xff\xff\xff\xff\xff\xff\xff"),
		Values: mod.Values{
			uint64(18446744073709551615), uint(18446744073709551615),
		}.AddVariants(mod.All...),
		BrokenMarshalTypes: brokenUints,
	}.Run("max_uint", t, marshal, unmarshal)
}
