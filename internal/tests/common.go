package tests

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/google/uuid"
)

func AssertTrue(t *testing.T, description string, value bool) {
	t.Helper()
	if !value {
		t.Fatalf("expected %s to be true", description)
	}
}

func AssertEqual(t *testing.T, description string, expected, actual interface{}) {
	t.Helper()
	if expected != actual {
		t.Fatalf("expected %s to be (%+v) but was (%+v) instead", description, expected, actual)
	}
}

func AssertDeepEqual(t *testing.T, description string, expected, actual interface{}) {
	t.Helper()
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("expected %s to be (%+v) but was (%+v) instead", description, expected, actual)
	}
}

func AssertNil(t *testing.T, description string, actual interface{}) {
	t.Helper()
	if actual != nil {
		t.Fatalf("expected %s to be (nil) but was (%+v) instead", description, actual)
	}
}

func RandomUUID() string {
	val, err := uuid.NewRandom()
	if err != nil {
		panic(fmt.Sprintf("failed to generate UUID: %s", err.Error()))
	}
	return val.String()
}
