//go:build unit
// +build unit

package gocql

import (
	"reflect"
	"testing"
)

// typeOption writes a CQL [option] type description the way a server does.
type typeOption func(f *framer)

func optNative(t Type) typeOption {
	return func(f *framer) { f.writeShort(uint16(t)) }
}

func optCustom(class string) typeOption {
	return func(f *framer) {
		f.writeShort(uint16(TypeCustom))
		f.writeString(class)
	}
}

func optCollection(t Type, elems ...typeOption) typeOption {
	return func(f *framer) {
		f.writeShort(uint16(t))
		for _, e := range elems {
			e(f)
		}
	}
}

func optTuple(elems ...typeOption) typeOption {
	return func(f *framer) {
		f.writeShort(uint16(TypeTuple))
		f.writeShort(uint16(len(elems)))
		for _, e := range elems {
			e(f)
		}
	}
}

type optField struct {
	name string
	typ  typeOption
}

func optUDT(keyspace, name string, fields ...optField) typeOption {
	return func(f *framer) {
		f.writeShort(uint16(TypeUDT))
		f.writeString(keyspace)
		f.writeString(name)
		f.writeShort(uint16(len(fields)))
		for _, fl := range fields {
			f.writeString(fl.name)
			fl.typ(f)
		}
	}
}

func readTypeOption(opt typeOption) (TypeInfo, int) {
	f := &framer{proto: protoVersion4}
	opt(f)
	info := f.readTypeInfo()
	return info, len(f.buf)
}

func TestReadTypeInfo(t *testing.T) {
	t.Parallel()

	native := func(t Type) NativeType { return NativeType{proto: protoVersion4, typ: t} }

	tests := []struct {
		name string
		opt  typeOption
		want TypeInfo
	}{
		{"Native", optNative(TypeInt), native(TypeInt)},
		{"NativeDuration", optNative(TypeDuration), native(TypeDuration)},
		// Cassandra's marshal class names for native types resolve to the
		// native type, keeping the class name.
		{"CustomNativeClass", optCustom("org.apache.cassandra.db.marshal.Int32Type"),
			NativeType{proto: protoVersion4, typ: TypeInt, custom: "org.apache.cassandra.db.marshal.Int32Type"}},
		{"CustomUnknownClass", optCustom("com.example.MyType"),
			NativeType{proto: protoVersion4, typ: TypeCustom, custom: "com.example.MyType"}},
		{"List", optCollection(TypeList, optNative(TypeVarchar)),
			CollectionType{NativeType: native(TypeList), Elem: native(TypeVarchar)}},
		{"Set", optCollection(TypeSet, optNative(TypeUUID)),
			CollectionType{NativeType: native(TypeSet), Elem: native(TypeUUID)}},
		{"Map", optCollection(TypeMap, optNative(TypeVarchar), optNative(TypeBigInt)),
			CollectionType{NativeType: native(TypeMap), Key: native(TypeVarchar), Elem: native(TypeBigInt)}},
		{"Tuple", optTuple(optNative(TypeInt), optNative(TypeBoolean)),
			TupleTypeInfo{NativeType: native(TypeTuple), Elems: []TypeInfo{native(TypeInt), native(TypeBoolean)}}},
		{"EmptyTuple", optTuple(), TupleTypeInfo{NativeType: native(TypeTuple), Elems: []TypeInfo{}}},
		{"UDT", optUDT("ks", "addr",
			optField{"street", optNative(TypeVarchar)},
			optField{"zip", optNative(TypeInt)},
		), UDTTypeInfo{NativeType: native(TypeUDT), KeySpace: "ks", Name: "addr", Elements: []UDTField{
			{Name: "street", Type: native(TypeVarchar)},
			{Name: "zip", Type: native(TypeInt)},
		}}},
		{"Nested", optCollection(TypeMap,
			optNative(TypeVarchar),
			optCollection(TypeList, optTuple(optNative(TypeInt), optUDT("ks", "u", optField{"f", optNative(TypeBlob)}))),
		), CollectionType{
			NativeType: native(TypeMap),
			Key:        native(TypeVarchar),
			Elem: CollectionType{NativeType: native(TypeList), Elem: TupleTypeInfo{
				NativeType: native(TypeTuple),
				Elems: []TypeInfo{native(TypeInt), UDTTypeInfo{
					NativeType: native(TypeUDT), KeySpace: "ks", Name: "u",
					Elements: []UDTField{{Name: "f", Type: native(TypeBlob)}},
				}},
			}},
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, rest := readTypeOption(tc.opt)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("readTypeInfo = %#v\nwant %#v", got, tc.want)
			}
			if rest != 0 {
				t.Fatalf("%d bytes left unread", rest)
			}
		})
	}
}

func TestTypeStringUnknown(t *testing.T) {
	t.Parallel()

	if got := Type(0x00ff).String(); got != "unknown_type_255" {
		t.Fatalf("String() = %q, want unknown_type_255", got)
	}
}
