//go:build all || unit
// +build all unit

package serialization_test

import (
	"math"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
	"github.com/gocql/gocql/serialization/timestamp"
)

func TestMarshalsTimestamp(t *testing.T) {
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

	zeroTime := time.Unix(0, 0).UTC()

	// The `time` package have a speciality - values `time.Time{}` and `time.Unix(0,0).UTC()` are different
	// The old unmarshal function unmarshalls `nil` and `zero` data into `time.Time{}`, but data with zeros into `time.Unix(0,0).UTC()`
	brokenTime := serialization.GetTypes(time.Time{}, &time.Time{})
	_ = brokenTime

	for _, tSuite := range testSuites {
		marshal := tSuite.marshal
		unmarshal := tSuite.unmarshal

		t.Run(tSuite.name, func(t *testing.T) {
			serialization.PositiveSet{
				Data: nil,
				Values: mod.Values{
					(*int64)(nil), (*time.Time)(nil),
				}.AddVariants(mod.CustomType),
			}.Run("[nil]nullable", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: nil,
				Values: mod.Values{
					int64(0), zeroTime,
				}.AddVariants(mod.CustomType),
				BrokenUnmarshalTypes: brokenTime,
			}.Run("[nil]unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data: make([]byte, 0),
				Values: mod.Values{
					int64(0), zeroTime,
				}.AddVariants(mod.All...),
				BrokenUnmarshalTypes: brokenTime,
			}.Run("[]unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x00\x00\x00\x00\x00\x00\x00"),
				Values: mod.Values{
					int64(0), zeroTime,
				}.AddVariants(mod.All...),
			}.Run("zeros", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x7f\xff\xff\xff\xff\xff\xff\xff"),
				Values: mod.Values{
					int64(math.MaxInt64), time.UnixMilli(math.MaxInt64).UTC(),
				}.AddVariants(mod.All...),
			}.Run("max", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x80\x00\x00\x00\x00\x00\x00\x00"),
				Values: mod.Values{
					int64(math.MinInt64), time.UnixMilli(math.MinInt64).UTC(),
				}.AddVariants(mod.All...),
			}.Run("min", t, marshal, unmarshal)
		})
	}
}
