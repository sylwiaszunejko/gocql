package valcases

import (
	"reflect"
)

type SimpleTypes []SimpleTypeCases

type SimpleTypeCases struct {
	CQLName string
	CQLType int
	Cases   []SimpleTypeCase
}

type SimpleTypeCase struct {
	Name      string
	Data      []byte
	LangCases []LangCase
}

type LangCase struct {
	LangType  string
	Value     interface{}
	ErrInsert bool
	ErrSelect bool
}

var nilBytes = ([]byte)(nil)

func GetSimple() SimpleTypes {
	return simpleTypesCases
}

func nilRef(in interface{}) interface{} {
	out := reflect.NewAt(reflect.TypeOf(in), nil).Interface()
	return out
}
