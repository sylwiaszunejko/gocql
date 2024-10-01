package serialization

import (
	"reflect"
)

func GetTypes(values ...interface{}) []reflect.Type {
	types := make([]reflect.Type, len(values))
	for i, value := range values {
		types[i] = reflect.TypeOf(value)
	}
	return types
}

func isTypeOf(value interface{}, types []reflect.Type) bool {
	valueType := reflect.TypeOf(value)
	for i := range types {
		if types[i] == valueType {
			return true
		}
	}
	return false
}

func deReference(in interface{}) interface{} {
	return reflect.Indirect(reflect.ValueOf(in)).Interface()
}
