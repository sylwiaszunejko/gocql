package mod

import "reflect"

var Reference Mod = func(vals ...interface{}) []interface{} {
	out := make([]interface{}, 0)
	for i := range vals {
		if vals[i] != nil {
			out = append(out, reference(vals[i]))
		}
	}
	return out
}

func reference(val interface{}) interface{} {
	inV := reflect.ValueOf(val)
	out := reflect.New(reflect.TypeOf(val))
	out.Elem().Set(inV)
	return out.Interface()
}
