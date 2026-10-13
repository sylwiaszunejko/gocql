//go:build unit
// +build unit

package gocql

import (
	"reflect"
	"strings"
	"testing"
)

func scanTestIter(actualColCount int, cols ...ColumnInfo) *Iter {
	return &Iter{meta: resultMetadata{columns: cols, actualColCount: actualColCount}}
}

func scanTestTuple(elems ...Type) TupleTypeInfo {
	tuple := TupleTypeInfo{NativeType: NativeType{proto: protoVersion4, typ: TypeTuple}}
	for _, e := range elems {
		tuple.Elems = append(tuple.Elems, NativeType{proto: protoVersion4, typ: e})
	}
	return tuple
}

// A tuple column is scanned as one value per element, named col[i].
func TestIterScanValuesExpandTuples(t *testing.T) {
	t.Parallel()

	iter := scanTestIter(4,
		ColumnInfo{Name: "id", TypeInfo: NativeType{proto: protoVersion4, typ: TypeInt}},
		ColumnInfo{Name: "pair", TypeInfo: scanTestTuple(TypeVarchar, TypeBigInt)},
		ColumnInfo{Name: "ok", TypeInfo: NativeType{proto: protoVersion4, typ: TypeBoolean}},
	)

	cols, err := iter.getScanColumns()
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"id", "pair[0]", "pair[1]", "ok"}; !reflect.DeepEqual(cols, want) {
		t.Fatalf("columns = %v, want %v", cols, want)
	}
	again, _ := iter.getScanColumns()
	if &again[0] != &cols[0] {
		t.Fatal("getScanColumns did not reuse its cached slice")
	}

	vals, err := iter.newScanValues()
	if err != nil {
		t.Fatal(err)
	}
	want := []any{new(int), new(string), new(int64), new(bool)}
	if len(vals) != len(want) {
		t.Fatalf("got %d values, want %d", len(vals), len(want))
	}
	for i := range want {
		if reflect.TypeOf(vals[i]) != reflect.TypeOf(want[i]) {
			t.Fatalf("value %d is %T, want %T", i, vals[i], want[i])
		}
	}
	// Scan writes through these pointers, so each call must allocate afresh.
	vals2, _ := iter.newScanValues()
	if vals2[0] == vals[0] {
		t.Fatal("newScanValues reused a pointer between calls")
	}
}

// actualColCount comes from the server's metadata; columns that disagree with
// it must fail the iterator rather than index out of range.
func TestIterScanValuesCountMismatch(t *testing.T) {
	t.Parallel()

	intCol := ColumnInfo{Name: "a", TypeInfo: NativeType{proto: protoVersion4, typ: TypeInt}}
	tupleCol := ColumnInfo{Name: "t", TypeInfo: scanTestTuple(TypeInt, TypeInt)}

	tests := []struct {
		name    string
		iter    func() *Iter
		wantErr string
	}{
		{"MoreColumnsThanPredicted", func() *Iter { return scanTestIter(1, intCol, intCol) }, "overflow"},
		{"MoreTupleElementsThanPredicted", func() *Iter { return scanTestIter(1, tupleCol) }, "overflow"},
		{"FewerColumnsThanPredicted", func() *Iter { return scanTestIter(3, intCol) }, "mismatch"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			iter := tc.iter()
			if _, err := iter.getScanColumns(); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("getScanColumns error = %v, want %s", err, tc.wantErr)
			}
			if iter.err == nil {
				t.Fatal("getScanColumns did not record the error on the iterator")
			}

			iter = tc.iter()
			if _, err := iter.newScanValues(); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("newScanValues error = %v, want %s", err, tc.wantErr)
			}
			if iter.err == nil {
				t.Fatal("newScanValues did not record the error on the iterator")
			}
		})
	}
}

// A column type with no Go equivalent cannot be scanned generically.
func TestIterScanValuesUnsupportedType(t *testing.T) {
	t.Parallel()

	unknown := NewCustomType(protoVersion4, TypeCustom, "com.example.Unknown")
	for _, col := range []ColumnInfo{
		{Name: "c", TypeInfo: unknown},
		{Name: "t", TypeInfo: TupleTypeInfo{NativeType: NativeType{proto: protoVersion4, typ: TypeTuple}, Elems: []TypeInfo{unknown}}},
	} {
		iter := scanTestIter(1, col)
		if _, err := iter.newScanValues(); err == nil || iter.err == nil {
			t.Fatalf("%s: newScanValues error = %v, want one recorded on the iterator", col.Name, err)
		}
	}
}
