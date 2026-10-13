//go:build unit
// +build unit

package gocql

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

// testUDTInfo is ks.u{a int, b text}.
func testUDTInfo() UDTTypeInfo {
	return UDTTypeInfo{
		NativeType: NativeType{proto: protoVersion4, typ: TypeUDT},
		KeySpace:   "ks",
		Name:       "u",
		Elements: []UDTField{
			{Name: "a", Type: NativeType{proto: protoVersion4, typ: TypeInt}},
			{Name: "b", Type: NativeType{proto: protoVersion4, typ: TypeVarchar}},
		},
	}
}

// udtField encodes one UDT element as [bytes]; nil encodes a null element.
func udtField(v []byte) []byte {
	if v == nil {
		return []byte{0xff, 0xff, 0xff, 0xff}
	}
	return bytesWithLength(v)
}

func udtValue(fields ...[]byte) []byte {
	var out []byte
	for _, f := range fields {
		out = append(out, udtField(f)...)
	}
	return out
}

var (
	udtA7  = []byte{0, 0, 0, 7}
	udtBHi = []byte("hi")
	// udtFull is {a: 7, b: "hi"}.
	udtFull = udtValue(udtA7, udtBHi)
)

type udtTagged struct {
	A int    `cql:"a"`
	B string `cql:"b"`
}

func TestUnmarshalUDTIntoStruct(t *testing.T) {
	t.Parallel()

	t.Run("ByTag", func(t *testing.T) {
		t.Parallel()
		var got udtTagged
		if err := Unmarshal(testUDTInfo(), udtFull, &got); err != nil {
			t.Fatal(err)
		}
		if want := (udtTagged{A: 7, B: "hi"}); got != want {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	})

	// Without a tag a field is matched by its exact Go name.
	t.Run("ByFieldName", func(t *testing.T) {
		t.Parallel()
		info := testUDTInfo()
		info.Elements[0].Name = "A"
		var got struct {
			A int
			B string `cql:"b"`
		}
		if err := Unmarshal(info, udtFull, &got); err != nil {
			t.Fatal(err)
		}
		if got.A != 7 || got.B != "hi" {
			t.Fatalf("got %+v, want {A:7 B:hi}", got)
		}
	})

	// UDT fields with no struct counterpart are skipped, and struct fields
	// with no UDT counterpart are left alone.
	t.Run("MismatchedFields", func(t *testing.T) {
		t.Parallel()
		got := struct {
			B     string `cql:"b"`
			Extra string `cql:"extra"`
		}{Extra: "kept"}
		if err := Unmarshal(testUDTInfo(), udtFull, &got); err != nil {
			t.Fatal(err)
		}
		if got.B != "hi" || got.Extra != "kept" {
			t.Fatalf("got %+v, want B=hi Extra=kept", got)
		}
	})

	// A null UDT resets the destination.
	t.Run("NullValueZeroesStruct", func(t *testing.T) {
		t.Parallel()
		got := udtTagged{A: 1, B: "old"}
		if err := Unmarshal(testUDTInfo(), nil, &got); err != nil {
			t.Fatal(err)
		}
		if got != (udtTagged{}) {
			t.Fatalf("got %+v, want zero value", got)
		}
	})

	t.Run("NullElementIsZero", func(t *testing.T) {
		t.Parallel()
		got := udtTagged{A: 1, B: "old"}
		if err := Unmarshal(testUDTInfo(), udtValue(nil, udtBHi), &got); err != nil {
			t.Fatal(err)
		}
		if got.A != 0 || got.B != "hi" {
			t.Fatalf("got %+v, want {A:0 B:hi}", got)
		}
	})

	// A value serialized before a field was added to the type is shorter than
	// the current definition; the missing trailing fields are not an error.
	t.Run("FewerElementsThanDefinition", func(t *testing.T) {
		t.Parallel()
		var got udtTagged
		if err := Unmarshal(testUDTInfo(), udtValue(udtA7), &got); err != nil {
			t.Fatal(err)
		}
		if got.A != 7 || got.B != "" {
			t.Fatalf("got %+v, want {A:7 B:}", got)
		}
	})

	t.Run("ElementUnmarshalError", func(t *testing.T) {
		t.Parallel()
		var got udtTagged
		// three bytes is not a valid int
		if err := Unmarshal(testUDTInfo(), udtValue([]byte{1, 2, 3}, udtBHi), &got); err == nil {
			t.Fatal("expected an error for a malformed int element")
		}
	})

	t.Run("TruncatedLengthHeader", func(t *testing.T) {
		t.Parallel()
		var got udtTagged
		err := Unmarshal(testUDTInfo(), []byte{0, 0}, &got)
		if err == nil || !strings.Contains(err.Error(), "field [0]a: unexpected eof") {
			t.Fatalf("err = %v, want unexpected eof on field a", err)
		}
	})

	t.Run("NonPointer", func(t *testing.T) {
		t.Parallel()
		err := Unmarshal(testUDTInfo(), udtFull, udtTagged{})
		if err == nil || !strings.Contains(err.Error(), "non-pointer") {
			t.Fatalf("err = %v, want a non-pointer error", err)
		}
	})

	t.Run("PointerToNonStruct", func(t *testing.T) {
		t.Parallel()
		var n int
		if err := Unmarshal(testUDTInfo(), udtFull, &n); err == nil {
			t.Fatal("expected an error unmarshalling a UDT into *int")
		}
	})

	t.Run("NilStructPointer", func(t *testing.T) {
		t.Parallel()
		var p *udtTagged
		if err := Unmarshal(testUDTInfo(), udtFull, p); err == nil {
			t.Fatal("expected an error unmarshalling into a nil *struct")
		}
	})

	// **T allocates the struct for a value and leaves it nil for null.
	t.Run("PointerToPointer", func(t *testing.T) {
		t.Parallel()
		var p *udtTagged
		if err := Unmarshal(testUDTInfo(), udtFull, &p); err != nil {
			t.Fatal(err)
		}
		if p == nil || *p != (udtTagged{A: 7, B: "hi"}) {
			t.Fatalf("got %+v, want &{A:7 B:hi}", p)
		}
		if err := Unmarshal(testUDTInfo(), nil, &p); err != nil {
			t.Fatal(err)
		}
		if p != nil {
			t.Fatalf("got %+v, want nil for a null UDT", p)
		}
	})
}

func TestUnmarshalUDTIntoMap(t *testing.T) {
	t.Parallel()

	t.Run("Value", func(t *testing.T) {
		t.Parallel()
		var got map[string]any
		if err := Unmarshal(testUDTInfo(), udtFull, &got); err != nil {
			t.Fatal(err)
		}
		if want := map[string]any{"a": 7, "b": "hi"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("got %#v, want %#v", got, want)
		}
	})

	t.Run("NullValue", func(t *testing.T) {
		t.Parallel()
		got := map[string]any{"stale": 1}
		if err := Unmarshal(testUDTInfo(), nil, &got); err != nil {
			t.Fatal(err)
		}
		if got != nil {
			t.Fatalf("got %#v, want nil map", got)
		}
	})

	t.Run("NullElementIsZero", func(t *testing.T) {
		t.Parallel()
		var got map[string]any
		if err := Unmarshal(testUDTInfo(), udtValue(nil, udtBHi), &got); err != nil {
			t.Fatal(err)
		}
		if want := map[string]any{"a": 0, "b": "hi"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("got %#v, want %#v", got, want)
		}
	})

	t.Run("FewerElementsThanDefinition", func(t *testing.T) {
		t.Parallel()
		var got map[string]any
		if err := Unmarshal(testUDTInfo(), udtValue(udtA7), &got); err != nil {
			t.Fatal(err)
		}
		if want := map[string]any{"a": 7}; !reflect.DeepEqual(got, want) {
			t.Fatalf("got %#v, want %#v", got, want)
		}
	})

	t.Run("TruncatedLengthHeader", func(t *testing.T) {
		t.Parallel()
		var got map[string]any
		err := Unmarshal(testUDTInfo(), []byte{0, 0}, &got)
		if err == nil || !strings.Contains(err.Error(), "unexpected eof") {
			t.Fatalf("err = %v, want unexpected eof", err)
		}
	})

	t.Run("ElementUnmarshalError", func(t *testing.T) {
		t.Parallel()
		var got map[string]any
		if err := Unmarshal(testUDTInfo(), udtValue([]byte{1, 2, 3}), &got); err == nil {
			t.Fatal("expected an error for a malformed int element")
		}
	})

	// An element type with no Go equivalent cannot be put in a map.
	t.Run("ElementWithoutGoType", func(t *testing.T) {
		t.Parallel()
		info := testUDTInfo()
		info.Elements[0].Type = NewCustomType(protoVersion4, TypeCustom, "com.example.Unknown")
		var got map[string]any
		err := Unmarshal(info, udtFull, &got)
		if err == nil || !strings.Contains(err.Error(), "can not unmarshal ks.u") {
			t.Fatalf("err = %v, want an error naming the UDT", err)
		}
	})

	t.Run("IntoInterfaceNull", func(t *testing.T) {
		t.Parallel()
		var got any = "stale"
		if err := Unmarshal(testUDTInfo(), nil, &got); err != nil {
			t.Fatal(err)
		}
		if got != nil {
			t.Fatalf("got %#v, want nil", got)
		}
	})

	t.Run("IntoInterfaceError", func(t *testing.T) {
		t.Parallel()
		var got any
		if err := Unmarshal(testUDTInfo(), []byte{0, 0}, &got); err == nil {
			t.Fatal("expected unexpected eof")
		}
	})
}

// recordingUDTUnmarshaler records each UnmarshalUDT call and can fail on a
// chosen field.
type recordingUDTUnmarshaler struct {
	calls  []string
	data   [][]byte
	failOn string
}

var errUDTField = errors.New("udt field failed")

func (r *recordingUDTUnmarshaler) UnmarshalUDT(name string, info TypeInfo, data []byte) error {
	r.calls = append(r.calls, name+":"+info.Type().String())
	r.data = append(r.data, data)
	if name == r.failOn {
		return errUDTField
	}
	return nil
}

func TestUnmarshalUDTIntoUDTUnmarshaler(t *testing.T) {
	t.Parallel()

	t.Run("CalledPerElementInOrder", func(t *testing.T) {
		t.Parallel()
		r := &recordingUDTUnmarshaler{}
		if err := Unmarshal(testUDTInfo(), udtValue(udtA7, nil), r); err != nil {
			t.Fatal(err)
		}
		if want := []string{"a:int", "b:varchar"}; !reflect.DeepEqual(r.calls, want) {
			t.Fatalf("calls = %v, want %v", r.calls, want)
		}
		if !reflect.DeepEqual(r.data[0], udtA7) || r.data[1] != nil {
			t.Fatalf("data = %v, want [%v nil]", r.data, udtA7)
		}
	})

	t.Run("NullValueMakesNoCalls", func(t *testing.T) {
		t.Parallel()
		r := &recordingUDTUnmarshaler{}
		if err := Unmarshal(testUDTInfo(), nil, r); err != nil {
			t.Fatal(err)
		}
		if len(r.calls) != 0 {
			t.Fatalf("calls = %v, want none", r.calls)
		}
	})

	t.Run("FewerElementsThanDefinition", func(t *testing.T) {
		t.Parallel()
		r := &recordingUDTUnmarshaler{}
		if err := Unmarshal(testUDTInfo(), udtValue(udtA7), r); err != nil {
			t.Fatal(err)
		}
		if want := []string{"a:int"}; !reflect.DeepEqual(r.calls, want) {
			t.Fatalf("calls = %v, want %v", r.calls, want)
		}
	})

	t.Run("ErrorStopsAndPropagates", func(t *testing.T) {
		t.Parallel()
		r := &recordingUDTUnmarshaler{failOn: "a"}
		if err := Unmarshal(testUDTInfo(), udtFull, r); !errors.Is(err, errUDTField) {
			t.Fatalf("err = %v, want %v", err, errUDTField)
		}
		if len(r.calls) != 1 {
			t.Fatalf("calls = %v, want only a", r.calls)
		}
	})

	t.Run("TruncatedLengthHeader", func(t *testing.T) {
		t.Parallel()
		r := &recordingUDTUnmarshaler{}
		err := Unmarshal(testUDTInfo(), append(udtField(udtA7), 0, 0), r)
		if err == nil || !strings.Contains(err.Error(), "field [1]b: unexpected eof") {
			t.Fatalf("err = %v, want unexpected eof on field b", err)
		}
	})
}

// wholeUDTUnmarshaler implements Unmarshaler, which takes precedence over
// every UDT-specific path.
type wholeUDTUnmarshaler struct {
	udtTagged
	raw []byte
}

func (w *wholeUDTUnmarshaler) UnmarshalCQL(_ TypeInfo, data []byte) error {
	w.raw = data
	return nil
}

func TestUnmarshalUDTIntoUnmarshaler(t *testing.T) {
	t.Parallel()

	var w wholeUDTUnmarshaler
	if err := unmarshalUDT(testUDTInfo(), udtFull, &w); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(w.raw, udtFull) || w.A != 0 {
		t.Fatalf("got %+v, want the raw value handed over and no field decoding", w)
	}
}

// A length header that claims more bytes than remain must be an error, not a
// slice-bounds panic in the caller's Scan.
func TestUnmarshalTruncatedElement(t *testing.T) {
	t.Parallel()

	// field a claims 10 bytes but only 2 follow
	truncated := []byte{0, 0, 0, 10, 1, 2}

	tuple := TupleTypeInfo{
		NativeType: NativeType{proto: protoVersion4, typ: TypeTuple},
		Elems: []TypeInfo{
			NativeType{proto: protoVersion4, typ: TypeInt},
			NativeType{proto: protoVersion4, typ: TypeVarchar},
		},
	}

	var (
		a int
		b string
	)
	tests := []struct {
		name string
		info TypeInfo
		dest any
	}{
		{"UDTStruct", testUDTInfo(), &udtTagged{}},
		{"UDTMap", testUDTInfo(), &map[string]any{}},
		{"UDTInterface", testUDTInfo(), new(any)},
		{"UDTUnmarshaler", testUDTInfo(), &recordingUDTUnmarshaler{}},
		{"TupleValues", tuple, []any{&a, &b}},
		{"TupleStruct", tuple, &struct {
			A int
			B string
		}{}},
		{"TupleSlice", tuple, &[]any{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := Unmarshal(tc.info, truncated, tc.dest)
			if err == nil || !strings.Contains(err.Error(), "unexpected eof") {
				t.Fatalf("err = %v, want unexpected eof", err)
			}
		})
	}
}

func TestReadBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		in        []byte
		wantValue []byte
		wantRest  []byte
		wantOK    bool
	}{
		{"Value", []byte{0, 0, 0, 2, 'h', 'i', 9}, []byte("hi"), []byte{9}, true},
		{"Empty", []byte{0, 0, 0, 0, 9}, []byte{}, []byte{9}, true},
		{"Null", []byte{0xff, 0xff, 0xff, 0xff, 9}, nil, []byte{9}, true},
		{"ShortHeader", []byte{0, 0, 0}, nil, []byte{0, 0, 0}, false},
		{"ShortValue", []byte{0, 0, 0, 3, 'h', 'i'}, nil, []byte("hi"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			value, rest, ok := readBytes(tc.in)
			if ok != tc.wantOK || !reflect.DeepEqual(value, tc.wantValue) || !reflect.DeepEqual(rest, tc.wantRest) {
				t.Fatalf("readBytes(%v) = %v, %v, %v; want %v, %v, %v",
					tc.in, value, rest, ok, tc.wantValue, tc.wantRest, tc.wantOK)
			}
		})
	}
}
