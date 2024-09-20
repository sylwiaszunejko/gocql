package utils

import (
	"fmt"
	"gopkg.in/inf.v0"
	"math/big"
	"net"
	"reflect"
	"time"
)

// StringValue returns (value_type)(value) in the human-readable format.
func StringValue(in interface{}) string {
	valStr := stringValue(in)
	if len(valStr) > printLimit {
		valStr = valStr[:printLimit]
	}
	return fmt.Sprintf("(%T)(%s)", in, valStr)
}

func stringValue(in interface{}) string {
	switch i := in.(type) {
	case string:
		return i
	case inf.Dec:
		return fmt.Sprintf("%v", i.String())
	case big.Int:
		return fmt.Sprintf("%v", i.String())
	case net.IP:
		return fmt.Sprintf("%v", []byte(i))
	case time.Time:
		return fmt.Sprintf("%v", i.UnixMilli())
	case nil:
		return "nil"
	}

	rv := reflect.ValueOf(in)
	switch rv.Kind() {
	case reflect.Ptr:
		if rv.IsNil() {
			return "*nil"
		}
		return fmt.Sprintf("*%s", stringValue(rv.Elem().Interface()))
	case reflect.Slice:
		if rv.IsNil() {
			return "[nil]"
		}
		return fmt.Sprintf("%v", rv.Interface())
	default:
		return fmt.Sprintf("%v", in)
	}
}
