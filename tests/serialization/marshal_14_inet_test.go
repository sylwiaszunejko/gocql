package serialization_test

import (
	"net"
	"testing"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
)

func TestMarshalsInet(t *testing.T) {
	tType := gocql.NewNativeType(4, gocql.TypeInet, "")

	marshal := func(i interface{}) ([]byte, error) { return gocql.Marshal(tType, i) }
	unmarshal := func(bytes []byte, i interface{}) error {
		return gocql.Unmarshal(tType, bytes, i)
	}

	// marshal and unmarshal []byte{}, [4]byte{}, [16]byte{} and `custom types` of these types unsupported
	// marshal and unmarshal `custom string` unsupported
	brokenTypes := serialization.GetTypes(mod.Values{
		[]byte{},
		[4]byte{},
		[16]byte{},
		mod.String(""),
	}.AddVariants(mod.All...)...)

	// unmarshal zero and nil data into `net.IP` and `string` unsupported
	brokenZeroUnmarshal := serialization.GetTypes(mod.Values{net.IP{}, ""}.AddVariants(mod.Reference)...)

	serialization.PositiveSet{
		Data: nil,
		Values: mod.Values{
			([]byte)(nil),
			(*[]byte)(nil),
			(*[4]byte)(nil),
			(*[16]byte)(nil),
			(net.IP)(nil),
			(*net.IP)(nil),
			"",
			(*string)(nil),
		}.AddVariants(mod.CustomType),
		BrokenMarshalTypes:   serialization.GetTypes([]byte{}, mod.Bytes{}, "", mod.String("")),
		BrokenUnmarshalTypes: serialization.GetTypes([]byte{}, mod.Bytes{}, net.IP{}, mod.String("")),
	}.Run("[nil]nullable", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: nil,
		Values: mod.Values{
			[4]byte{},
			[16]byte{},
		}.AddVariants(mod.CustomType),
		BrokenUnmarshalTypes: brokenTypes,
	}.Run("[nil]unmarshal", t, nil, unmarshal)

	serialization.PositiveSet{
		Data: make([]byte, 0),
		Values: mod.Values{
			make([]byte, 0),
			[4]byte{},
			[16]byte{},
			make(net.IP, 0),
			"0.0.0.0",
		}.AddVariants(mod.All...),
		BrokenUnmarshalTypes: append(brokenTypes, brokenZeroUnmarshal...),
	}.Run("[]unmarshal", t, nil, unmarshal)

	serialization.PositiveSet{
		Data: []byte{0, 0, 0, 0},
		Values: mod.Values{
			"0.0.0.0",
			[]byte{0, 0, 0, 0},
			net.IP{0, 0, 0, 0},
			[4]byte{0, 0, 0, 0},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenTypes,
		BrokenUnmarshalTypes: brokenTypes,
	}.Run("zerosV4", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		Values: mod.Values{
			"::",
			[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			net.IP{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			[16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenTypes,
		BrokenUnmarshalTypes: brokenTypes,
	}.Run("zerosV6", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte{192, 168, 0, 1},
		Values: mod.Values{
			"192.168.0.1",
			[]byte{192, 168, 0, 1},
			net.IP{192, 168, 0, 1},
			[4]byte{192, 168, 0, 1},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenTypes,
		BrokenUnmarshalTypes: brokenTypes,
	}.Run("ipV4", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\xfe\x80\xcd\x00\x00\x00\x0c\xde\x12\x57\x00\x00\x21\x1e\x72\x9c"),
		Values: mod.Values{
			"fe80:cd00:0:cde:1257:0:211e:729c",
			[]byte("\xfe\x80\xcd\x00\x00\x00\x0c\xde\x12\x57\x00\x00\x21\x1e\x72\x9c"),
			net.IP("\xfe\x80\xcd\x00\x00\x00\x0c\xde\x12\x57\x00\x00\x21\x1e\x72\x9c"),
			[16]byte{254, 128, 205, 0, 0, 0, 12, 222, 18, 87, 0, 0, 33, 30, 114, 156},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenTypes,
		BrokenUnmarshalTypes: brokenTypes,
	}.Run("ipV6", t, marshal, unmarshal)
}
