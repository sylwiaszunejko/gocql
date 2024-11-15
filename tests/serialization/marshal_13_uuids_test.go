//go:build all || unit
// +build all unit

package serialization_test

import (
	"testing"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
)

func TestMarshalUUIDs(t *testing.T) {
	tTypes := []gocql.NativeType{
		gocql.NewNativeType(4, gocql.TypeUUID, ""),
		gocql.NewNativeType(4, gocql.TypeTimeUUID, ""),
	}

	// marshal and unmarshal `custom string` and `custom []byte` unsupported in `nil` data case
	brokenCustomTypes := serialization.GetTypes(mod.Bytes{}, mod.String(""), mod.Bytes16{})

	// marshal and unmarshal `custom types` unsupported in not `nil` data cases
	brokenCustomTypesFull := serialization.GetTypes(mod.Values{mod.Bytes{}, mod.String(""), mod.Bytes16{}}.AddVariants(mod.Reference)...)

	// marshal (string)("") and ([]byte)(nil) unsupported
	brokenNullableTypes := serialization.GetTypes("", []byte{})

	// unmarshal `zero` data into ([]byte)(nil), (*[]byte)(*[nil])
	brokenZeroSlices := serialization.GetTypes(make([]byte, 0), (*[]byte)(nil))

	for _, tType := range tTypes {
		marshal := func(i interface{}) ([]byte, error) { return gocql.Marshal(tType, i) }
		unmarshal := func(bytes []byte, i interface{}) error {
			return gocql.Unmarshal(tType, bytes, i)
		}

		t.Run(tType.String(), func(t *testing.T) {
			serialization.PositiveSet{
				Data: nil,
				Values: mod.Values{
					([]byte)(nil), (*[]byte)(nil),
					"", (*string)(nil),
					(*[16]byte)(nil),
				}.AddVariants(mod.CustomType),
				BrokenUnmarshalTypes: brokenCustomTypes,
				BrokenMarshalTypes:   append(brokenNullableTypes, brokenCustomTypes...),
			}.Run("[nil]nullable", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: nil,
				Values: mod.Values{
					[16]byte{},
				}.AddVariants(mod.CustomType),
				BrokenUnmarshalTypes: brokenCustomTypes,
			}.Run("[nil]unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data: make([]byte, 0),
				Values: mod.Values{
					make([]byte, 0), [16]byte{}, "", gocql.UUID{},
				}.AddVariants(mod.All...),
				BrokenUnmarshalTypes: append(brokenCustomTypesFull, brokenZeroSlices...),
			}.Run("[]unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"),
				Values: mod.Values{
					"00000000-0000-0000-0000-000000000000",
					[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					[16]byte{},
					gocql.UUID{},
				}.AddVariants(mod.All...),
				BrokenMarshalTypes:   brokenCustomTypesFull,
				BrokenUnmarshalTypes: brokenCustomTypesFull,
			}.Run("zeros", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\xb6\xb7\x7c\x23\xc7\x76\x40\xff\x82\x8d\xa3\x85\xf3\xe8\xa2\xaf"),
				Values: mod.Values{
					"b6b77c23-c776-40ff-828d-a385f3e8a2af",
					[]byte{182, 183, 124, 35, 199, 118, 64, 255, 130, 141, 163, 133, 243, 232, 162, 175},
					[16]byte{182, 183, 124, 35, 199, 118, 64, 255, 130, 141, 163, 133, 243, 232, 162, 175},
					gocql.UUID{182, 183, 124, 35, 199, 118, 64, 255, 130, 141, 163, 133, 243, 232, 162, 175},
				}.AddVariants(mod.All...),
				BrokenMarshalTypes:   brokenCustomTypesFull,
				BrokenUnmarshalTypes: brokenCustomTypesFull,
			}.Run("uuid", t, marshal, unmarshal)
		})
	}
}
