package utils

import (
	"reflect"
)

func DeReference(in interface{}) interface{} {
	return reflect.Indirect(reflect.ValueOf(in)).Interface()
}

func Reference(val interface{}) interface{} {
	out := reflect.New(reflect.TypeOf(val))
	out.Elem().Set(reflect.ValueOf(val))
	return out.Interface()
}

func GetTypes(values ...interface{}) []reflect.Type {
	types := make([]reflect.Type, len(values))
	for i, value := range values {
		types[i] = reflect.TypeOf(value)
	}
	return types
}
