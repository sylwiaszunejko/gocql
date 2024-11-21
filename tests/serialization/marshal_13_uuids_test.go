//go:build all || unit
// +build all unit

package serialization_test

import (
	"testing"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
	"github.com/gocql/gocql/serialization/timeuuid"
	"github.com/gocql/gocql/serialization/uuid"
)

func TestMarshalUUIDs(t *testing.T) {
	tTypes := []gocql.NativeType{
		gocql.NewNativeType(4, gocql.TypeUUID, ""),
		gocql.NewNativeType(4, gocql.TypeTimeUUID, ""),
	}

	type testSuite struct {
		name      string
		marshal   func(interface{}) ([]byte, error)
		unmarshal func(bytes []byte, i interface{}) error
	}

	testSuites := [4]testSuite{
		{
			name:      "serialization.uuid",
			marshal:   uuid.Marshal,
			unmarshal: uuid.Unmarshal,
		},
		{
			name: "glob.uuid",
			marshal: func(i interface{}) ([]byte, error) {
				return gocql.Marshal(tTypes[0], i)
			},
			unmarshal: func(bytes []byte, i interface{}) error {
				return gocql.Unmarshal(tTypes[0], bytes, i)
			},
		},
		{
			name:      "serialization.timeuuid",
			marshal:   timeuuid.Marshal,
			unmarshal: timeuuid.Unmarshal,
		},
		{
			name:      "glob.timeuuid",
			marshal:   func(i interface{}) ([]byte, error) { return gocql.Marshal(tTypes[1], i) },
			unmarshal: func(bytes []byte, i interface{}) error { return gocql.Unmarshal(tTypes[1], bytes, i) },
		},
	}

	for _, tSuite := range testSuites {
		marshal := tSuite.marshal
		unmarshal := tSuite.unmarshal

		t.Run(tSuite.name, func(t *testing.T) {

			serialization.PositiveSet{
				Data: nil,
				Values: mod.Values{
					([]byte)(nil), (*[]byte)(nil),
					"", (*string)(nil),
					(*[16]byte)(nil),
					(*gocql.UUID)(nil),
				}.AddVariants(mod.CustomType),
			}.Run("[nil]nullable", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: nil,
				Values: mod.Values{
					[16]byte{},
					gocql.UUID{},
				}.AddVariants(mod.CustomType),
			}.Run("[nil]unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data: make([]byte, 0),
				Values: mod.Values{
					"00000000-0000-0000-0000-000000000000",
					make([]byte, 0),
					[16]byte{},
					gocql.UUID{},
				}.AddVariants(mod.All...),
			}.Run("[]unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data: []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Values: mod.Values{
					"00000000-0000-0000-0000-000000000000",
					[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					[16]byte{},
					gocql.UUID{},
				}.AddVariants(mod.All...),
			}.Run("zeros", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\xb6\xb7\x7c\x23\xc7\x76\x40\xff\x82\x8d\xa3\x85\xf3\xe8\xa2\xaf"),
				Values: mod.Values{
					"b6b77c23-c776-40ff-828d-a385f3e8a2af",
					[]byte{182, 183, 124, 35, 199, 118, 64, 255, 130, 141, 163, 133, 243, 232, 162, 175},
					[16]byte{182, 183, 124, 35, 199, 118, 64, 255, 130, 141, 163, 133, 243, 232, 162, 175},
					gocql.UUID{182, 183, 124, 35, 199, 118, 64, 255, 130, 141, 163, 133, 243, 232, 162, 175},
				}.AddVariants(mod.All...),
			}.Run("uuid", t, marshal, unmarshal)
		})
	}
}
