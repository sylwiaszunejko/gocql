//go:build all || unit
// +build all unit

package serialization_test

import (
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
	"github.com/gocql/gocql/serialization/timestamp"
)

func TestMarshalTimestampCorrupt(t *testing.T) {
	tType := gocql.NewNativeType(4, gocql.TypeTimestamp, "")

	type testSuite struct {
		name      string
		marshal   func(interface{}) ([]byte, error)
		unmarshal func(bytes []byte, i interface{}) error
	}

	testSuites := [2]testSuite{
		{
			name:      "serialization.timestamp",
			marshal:   timestamp.Marshal,
			unmarshal: timestamp.Unmarshal,
		},
		{
			name: "glob",
			marshal: func(i interface{}) ([]byte, error) {
				return gocql.Marshal(tType, i)
			},
			unmarshal: func(bytes []byte, i interface{}) error {
				return gocql.Unmarshal(tType, bytes, i)
			},
		},
	}

	for _, tSuite := range testSuites {
		unmarshal := tSuite.unmarshal

		t.Run(tSuite.name, func(t *testing.T) {

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x7f\xff\xff\xff\xff\xff\xff\xff\xff"),
				Values: mod.Values{
					int64(0), time.Time{},
				}.AddVariants(mod.All...),
			}.Run("big_data", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\xff\xff\xff\xff\xff\xff\xff"),
				Values: mod.Values{
					int64(0), time.Time{},
				}.AddVariants(mod.All...),
			}.Run("small_data1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00"),
				Values: mod.Values{
					int64(0), time.Time{},
				}.AddVariants(mod.All...),
			}.Run("small_data2", t, unmarshal)
		})
	}
}
