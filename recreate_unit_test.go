//go:build unit
// +build unit

/*
 * Copyright (C) 2026 ScyllaDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// KeyspaceMetadata.ToCQL and everything it calls had no coverage in either
// lane. TestRecreateSchema appears to cover them, but it does not: ToCQL
// short-circuits on a non-empty CreateStmts, which any server supporting
// DESCRIBE KEYSPACE ... WITH INTERNALS populates, so on a modern cluster the
// generator never runs. These tests drive it directly from hand-built
// metadata, which also means no cluster and no dependence on how a particular
// server reports its schema.

func col(name, typ string, kind ColumnKind) *ColumnMetadata {
	return &ColumnMetadata{Name: name, Type: typ, Kind: kind}
}

// TestToCQLUsesServerStatementsWhenPresent pins the short-circuit itself: a
// keyspace that already carries CreateStmts is echoed back untouched, and the
// generator is not consulted. This is the branch that hides all the others.
func TestToCQLUsesServerStatementsWhenPresent(t *testing.T) {
	t.Parallel()

	ks := &KeyspaceMetadata{
		Name:        "ks",
		CreateStmts: "CREATE KEYSPACE server_said_so;",
		// Deliberately inconsistent with CreateStmts: if the generator ran, the
		// output would mention this table.
		Tables: map[string]*TableMetadata{"t": {
			Name:           "t",
			OrderedColumns: []string{"id"},
			Columns:        map[string]*ColumnMetadata{"id": col("id", "int", ColumnPartitionKey)},
			PartitionKey:   []*ColumnMetadata{col("id", "int", ColumnPartitionKey)},
		}},
	}

	got, err := ks.ToCQL()
	if err != nil {
		t.Fatalf("ToCQL: %v", err)
	}
	if got != "CREATE KEYSPACE server_said_so;" {
		t.Errorf("ToCQL returned %q, want the stored CreateStmts verbatim", got)
	}
	if strings.Contains(got, "CREATE TABLE") {
		t.Error("the generator ran even though CreateStmts was populated")
	}
}

func TestKeyspaceToCQL(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		ks    *KeyspaceMetadata
		wants []string
	}{
		{
			name: "network topology strategy is emitted unqualified",
			ks: &KeyspaceMetadata{
				Name:            "ks",
				DurableWrites:   true,
				StrategyClass:   "org.apache.cassandra.locator.NetworkTopologyStrategy",
				StrategyOptions: map[string]any{"dc1": "3"},
			},
			// fixStrategy drops the org.apache.cassandra.locator. prefix.
			wants: []string{
				"CREATE KEYSPACE ks WITH replication = {",
				"'class': 'NetworkTopologyStrategy'",
				"'dc1': '3'",
				"durable_writes = true",
			},
		},
		{
			name: "durable_writes false is emitted explicitly",
			ks: &KeyspaceMetadata{
				Name:            "ks",
				DurableWrites:   false,
				StrategyClass:   "SimpleStrategy",
				StrategyOptions: map[string]any{"replication_factor": "1"},
			},
			wants: []string{"'class': 'SimpleStrategy'", "durable_writes = false"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := tc.ks.ToCQL()
			if err != nil {
				t.Fatalf("ToCQL: %v", err)
			}
			for _, want := range tc.wants {
				if !strings.Contains(got, want) {
					t.Errorf("output is missing %q\n--- got ---\n%s", want, got)
				}
			}
		})
	}
}

func TestTypesSortedTopologically(t *testing.T) {
	t.Parallel()

	// inner is referenced by outer, so it has to be declared first or the
	// generated CQL will not replay.
	inner := &TypeMetadata{Keyspace: "ks", Name: "inner", FieldNames: []string{"a"}, FieldTypes: []string{"int"}}
	outer := &TypeMetadata{Keyspace: "ks", Name: "outer", FieldNames: []string{"b"}, FieldTypes: []string{"frozen<inner>"}}

	// typesSortedTopologically ranges ks.Types, which is a map, so the order it
	// starts from is random per call. One of the two possible orders already
	// satisfies the assertion below, so a single round would let a sort that
	// does nothing through about half the time. Sample the input order instead,
	// and assert the property rather than one exact sequence.
	const rounds = 50
	for i := 0; i < rounds; i++ {
		ks := &KeyspaceMetadata{Types: map[string]*TypeMetadata{"outer": outer, "inner": inner}}

		sorted := ks.typesSortedTopologically()
		if len(sorted) != 2 {
			t.Fatalf("round %d: got %d types, want 2", i, len(sorted))
		}

		pos := make(map[string]int, len(sorted))
		order := make([]string, 0, len(sorted))
		for j, tm := range sorted {
			pos[tm.Name] = j
			order = append(order, tm.Name)
		}
		for _, tm := range sorted {
			for _, ft := range tm.FieldTypes {
				for name, at := range pos {
					if name != tm.Name && strings.Contains(ft, name) && at > pos[tm.Name] {
						t.Fatalf("round %d: %s embeds %s but is declared first: %v",
							i, tm.Name, name, order)
					}
				}
			}
		}
	}
}

func TestTableColumnToCQL(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		tm   *TableMetadata
		want string
	}{
		{
			name: "single partition key is declared inline",
			tm: &TableMetadata{
				OrderedColumns: []string{"id", "v"},
				Columns: map[string]*ColumnMetadata{
					"id": col("id", "int", ColumnPartitionKey),
					"v":  col("v", "text", ColumnRegular),
				},
				PartitionKey: []*ColumnMetadata{col("id", "int", ColumnPartitionKey)},
			},
			want: "id int PRIMARY KEY,\n    v text",
		},
		{
			name: "static column carries the static keyword",
			tm: &TableMetadata{
				OrderedColumns: []string{"id", "c", "s"},
				Columns: map[string]*ColumnMetadata{
					"id": col("id", "int", ColumnPartitionKey),
					"c":  col("c", "int", ColumnClusteringKey),
					"s":  col("s", "text", ColumnStatic),
				},
				PartitionKey:      []*ColumnMetadata{col("id", "int", ColumnPartitionKey)},
				ClusteringColumns: []*ColumnMetadata{col("c", "int", ColumnClusteringKey)},
			},
			want: "s text static",
		},
		{
			name: "composite partition key moves to a PRIMARY KEY clause",
			tm: &TableMetadata{
				OrderedColumns: []string{"a", "b", "v"},
				Columns: map[string]*ColumnMetadata{
					"a": col("a", "int", ColumnPartitionKey),
					"b": col("b", "int", ColumnPartitionKey),
					"v": col("v", "text", ColumnRegular),
				},
				PartitionKey: []*ColumnMetadata{
					col("a", "int", ColumnPartitionKey),
					col("b", "int", ColumnPartitionKey),
				},
			},
			want: "PRIMARY KEY ((a, b))",
		},
		{
			name: "clustering columns follow the partition key",
			tm: &TableMetadata{
				OrderedColumns: []string{"a", "c", "d", "v"},
				Columns: map[string]*ColumnMetadata{
					"a": col("a", "int", ColumnPartitionKey),
					"c": col("c", "int", ColumnClusteringKey),
					"d": col("d", "int", ColumnClusteringKey),
					"v": col("v", "text", ColumnRegular),
				},
				PartitionKey: []*ColumnMetadata{col("a", "int", ColumnPartitionKey)},
				// Two clustering columns, so the separator arm of
				// partitionKeyString runs as well as the first-column arm.
				ClusteringColumns: []*ColumnMetadata{
					col("c", "int", ColumnClusteringKey),
					col("d", "int", ColumnClusteringKey),
				},
			},
			want: "PRIMARY KEY (a, c, d)",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := cqlHelpers.tableColumnToCQL(tc.tm)
			if !strings.Contains(got, tc.want) {
				t.Errorf("column clause is missing %q\n--- got ---\n%s", tc.want, got)
			}
		})
	}
}

func TestTablePropertiesToCQL(t *testing.T) {
	t.Parallel()

	opts := TableMetadataOptions{
		Comment:        "hi",
		GcGraceSeconds: 864000,
		Caching:        map[string]string{"keys": "ALL"},
		Compaction:     map[string]string{"class": "SizeTieredCompactionStrategy"},
		Compression:    map[string]string{"sstable_compression": "LZ4Compressor"},
	}

	t.Run("clustering order leads the property list", func(t *testing.T) {
		t.Parallel()

		cks := []*ColumnMetadata{{Name: "c", ClusteringOrder: "DESC"}}
		got, err := cqlHelpers.tablePropertiesToCQL(cks, opts, nil)
		if err != nil {
			t.Fatalf("tablePropertiesToCQL: %v", err)
		}
		if !strings.HasPrefix(got, "CLUSTERING ORDER BY (c DESC)") {
			t.Errorf("clustering order is not first\n--- got ---\n%s", got)
		}
		for _, want := range []string{"comment = 'hi'", "gc_grace_seconds = 864000", "caching = "} {
			if !strings.Contains(got, want) {
				t.Errorf("properties missing %q\n--- got ---\n%s", want, got)
			}
		}
	})

	t.Run("no clustering columns means no order clause", func(t *testing.T) {
		t.Parallel()

		got, err := cqlHelpers.tablePropertiesToCQL(nil, opts, nil)
		if err != nil {
			t.Fatalf("tablePropertiesToCQL: %v", err)
		}
		if strings.Contains(got, "CLUSTERING ORDER") {
			t.Errorf("emitted a clustering order for a table with none\n--- got ---\n%s", got)
		}
	})
}

func TestToCQLHelpers(t *testing.T) {
	t.Parallel()

	t.Run("escape", func(t *testing.T) {
		t.Parallel()

		for _, tc := range []struct {
			in   any
			want string
		}{
			{"plain", "'plain'"},
			{"it's", "'it''s'"}, // the injection-relevant case
			{42, "42"},
			{1.5, "1.5"},
			{true, "true"},
			{false, "false"},
			{[]byte("raw"), "raw"},
			{struct{}{}, ""}, // unsupported types render empty
		} {
			if got := cqlHelpers.escape(tc.in); got != tc.want {
				t.Errorf("escape(%#v) = %q, want %q", tc.in, got, tc.want)
			}
		}
	})

	t.Run("stripFrozen", func(t *testing.T) {
		t.Parallel()

		if got := cqlHelpers.stripFrozen("frozen<tuple<int, double>>"); got != "tuple<int, double>" {
			t.Errorf("stripFrozen = %q", got)
		}
		if got := cqlHelpers.stripFrozen("int"); got != "int" {
			t.Errorf("stripFrozen left a plain type alone as %q", got)
		}
	})

	t.Run("fixStrategy", func(t *testing.T) {
		t.Parallel()

		if got := cqlHelpers.fixStrategy("org.apache.cassandra.locator.SimpleStrategy"); got != "SimpleStrategy" {
			t.Errorf("fixStrategy = %q", got)
		}
	})

	t.Run("fixQuote", func(t *testing.T) {
		t.Parallel()

		if got := cqlHelpers.fixQuote(`{"a": "b"}`); got != `{'a': 'b'}` {
			t.Errorf("fixQuote = %q", got)
		}
	})

	t.Run("zip", func(t *testing.T) {
		t.Parallel()

		got := cqlHelpers.zip([]string{"a", "b"}, []string{"int", "text"})
		if len(got) != 2 || got[0][0] != "a" || got[0][1] != "int" || got[1][1] != "text" {
			t.Errorf("zip = %v", got)
		}
	})
}

// TestToCQLGeneratesWholeKeyspace drives ToCQL's orchestration: every section
// it emits -- keyspace, types, tables, indexes, functions, aggregates and views
// -- plus the ordering constraints that make the output replayable.
//
// The per-section rendering is asserted separately above; what this adds is
// that ToCQL actually reaches each loop, and in an order a server can replay.
func TestToCQLGeneratesWholeKeyspace(t *testing.T) {
	t.Parallel()

	ks := &KeyspaceMetadata{
		Name:            "ks",
		DurableWrites:   true,
		StrategyClass:   "SimpleStrategy",
		StrategyOptions: map[string]any{"replication_factor": "1"},
		Types: map[string]*TypeMetadata{
			"addr": {Keyspace: "ks", Name: "addr", FieldNames: []string{"street", "zip"}, FieldTypes: []string{"text", "int"}},
		},
		Tables: map[string]*TableMetadata{
			"users": {
				Name:           "users",
				OrderedColumns: []string{"id", "home"},
				Columns: map[string]*ColumnMetadata{
					"id":   col("id", "uuid", ColumnPartitionKey),
					"home": col("home", "frozen<addr>", ColumnRegular),
				},
				PartitionKey: []*ColumnMetadata{col("id", "uuid", ColumnPartitionKey)},
				Options:      TableMetadataOptions{GcGraceSeconds: 864000},
			},
		},
		Indexes: map[string]*IndexMetadata{
			"users_by_home": {
				Name: "users_by_home", KeyspaceName: "ks", TableName: "users",
				Options: map[string]string{"target": "home"},
			},
		},
		Functions: map[string]*FunctionMetadata{
			"fsum": {
				Keyspace: "ks", Name: "fsum",
				ArgumentNames: []string{"a"}, ArgumentTypes: []string{"int"},
				ReturnType: "int", Language: "lua", Body: "return a", CalledOnNullInput: true,
			},
		},
		Aggregates: map[string]*AggregateMetadata{
			"total": {
				Keyspace: "ks", Name: "total",
				ArgumentTypes: []string{"int"}, ReturnType: "int", StateType: "int",
				InitCond:  "0",
				StateFunc: FunctionMetadata{Name: "fsum"},
			},
		},
		Views: map[string]*ViewMetadata{
			"users_by_zip": {
				KeyspaceName: "ks", ViewName: "users_by_zip", BaseTableName: "users",
				WhereClause:    "home IS NOT NULL",
				OrderedColumns: []string{"id", "home"},
				PartitionKey:   []*ColumnMetadata{col("id", "uuid", ColumnPartitionKey)},
			},
		},
	}

	got, err := ks.ToCQL()
	if err != nil {
		t.Fatalf("ToCQL: %v", err)
	}

	for _, want := range []string{
		"CREATE KEYSPACE ks WITH replication = {",
		"CREATE TYPE ks.addr",
		"street text",
		"CREATE TABLE ks.users",
		"id uuid PRIMARY KEY",
		"home frozen<addr>",
		"gc_grace_seconds = 864000",
		"CREATE INDEX users_by_home ON ks.users (home);",
		"CREATE FUNCTION ks.fsum",
		"CREATE AGGREGATE ks.total(",
		"CREATE MATERIALIZED VIEW ks.users_by_zip AS",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("generated schema is missing %q\n--- got ---\n%s", want, got)
		}
	}

	// Ordering constraints a server enforces on replay.
	for _, ord := range []struct{ first, then, why string }{
		{"CREATE TYPE ks.addr", "CREATE TABLE ks.users", "a type must precede the table using it"},
		{"CREATE TABLE ks.users", "CREATE INDEX users_by_home", "an index needs its base table"},
		{"CREATE TABLE ks.users", "CREATE MATERIALIZED VIEW ks.users_by_zip", "a view needs its base table"},
		{"CREATE FUNCTION ks.fsum", "CREATE AGGREGATE ks.total(", "an aggregate needs its state function"},
	} {
		// Index returns -1 for a statement that was never emitted, and -1 is
		// less than any real offset, so comparing the two alone would let a
		// missing first silently satisfy the constraint.
		i, j := strings.Index(got, ord.first), strings.Index(got, ord.then)
		if i < 0 {
			t.Errorf("%q was never emitted, so %s cannot be checked\n--- got ---\n%s", ord.first, ord.why, got)
			continue
		}
		if j < 0 {
			t.Errorf("%q was never emitted, so %s cannot be checked\n--- got ---\n%s", ord.then, ord.why, got)
			continue
		}
		if i > j {
			t.Errorf("%q comes after %q: %s\n--- got ---\n%s", ord.first, ord.then, ord.why, got)
		}
	}

	// ToCQL caches its own output in CreateStmts.
	if ks.CreateStmts != got {
		t.Error("ToCQL did not cache the generated schema in CreateStmts")
	}
}

func TestTableExtensionsToCQL(t *testing.T) {
	t.Parallel()

	blob, err := os.ReadFile("testdata/recreate/scylla_encryption_options.bin")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("encryption options are decoded and requoted", func(t *testing.T) {
		t.Parallel()

		got, err := cqlHelpers.tableExtensionsToCQL(map[string]any{"scylla_encryption_options": blob})
		if err != nil {
			t.Fatalf("tableExtensionsToCQL: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("got %d properties, want 1: %q", len(got), got)
		}
		if !strings.HasPrefix(got[0], "scylla_encryption_options = ") {
			t.Errorf("unexpected property: %q", got[0])
		}
		// fixQuote rewrites the marshalled JSON's double quotes as CQL single
		// quotes; a surviving double quote would not parse.
		if strings.Contains(got[0], `"`) {
			t.Errorf("double quotes survived into CQL: %s", got[0])
		}
	})

	t.Run("unknown extensions are dropped", func(t *testing.T) {
		t.Parallel()

		got, err := cqlHelpers.tableExtensionsToCQL(map[string]any{"something_else": []byte("x")})
		if err != nil {
			t.Fatalf("tableExtensionsToCQL: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("unknown extension rendered as %q", got)
		}
	})

	t.Run("no extensions yields no properties", func(t *testing.T) {
		t.Parallel()

		got, err := cqlHelpers.tableExtensionsToCQL(nil)
		if err != nil {
			t.Fatalf("tableExtensionsToCQL: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("got %q, want none", got)
		}
	})
}

func TestTableOptionsToCQLScyllaExtras(t *testing.T) {
	t.Parallel()

	// Options render as "name = value", so a prefix match is what identifies
	// one. Local to this test: nothing else in the package needs it, and the
	// test binary spans enough files that a name like this does not belong in
	// package scope.
	hasOption := func(in []string, prefix string) bool {
		return slices.ContainsFunc(in, func(s string) bool {
			return strings.HasPrefix(s, prefix)
		})
	}

	t.Run("cdc is emitted when set", func(t *testing.T) {
		t.Parallel()

		got, err := cqlHelpers.tableOptionsToCQL(TableMetadataOptions{CDC: map[string]string{"enabled": "true"}})
		if err != nil {
			t.Fatalf("tableOptionsToCQL: %v", err)
		}
		if !hasOption(got, "cdc = ") {
			t.Errorf("cdc missing from %q", got)
		}
	})

	t.Run("cdc is omitted when unset", func(t *testing.T) {
		t.Parallel()

		got, err := cqlHelpers.tableOptionsToCQL(TableMetadataOptions{})
		if err != nil {
			t.Fatalf("tableOptionsToCQL: %v", err)
		}
		if hasOption(got, "cdc = ") {
			t.Errorf("cdc emitted for a table that has none: %q", got)
		}
	})

	t.Run("in_memory is emitted only when true", func(t *testing.T) {
		t.Parallel()

		on, err := cqlHelpers.tableOptionsToCQL(TableMetadataOptions{InMemory: true})
		if err != nil {
			t.Fatalf("tableOptionsToCQL: %v", err)
		}
		if !hasOption(on, "in_memory = ") {
			t.Errorf("in_memory missing from %q", on)
		}

		off, err := cqlHelpers.tableOptionsToCQL(TableMetadataOptions{})
		if err != nil {
			t.Fatalf("tableOptionsToCQL: %v", err)
		}
		if hasOption(off, "in_memory = ") {
			t.Errorf("in_memory emitted for a non in-memory table: %q", off)
		}
	})
}

func TestIndexToCQL(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		im      *IndexMetadata
		want    string
		wantNot string
	}{
		{
			name: "plain target is emitted as-is",
			im: &IndexMetadata{
				Name: "users_by_email", KeyspaceName: "ks", TableName: "users",
				Options: map[string]string{"target": "email"},
			},
			want: "CREATE INDEX users_by_email ON ks.users (email);",
		},
		{
			name: "a JSON target is expanded into partition and clustering keys",
			im: &IndexMetadata{
				Name: "idx", KeyspaceName: "ks", TableName: "t",
				Options: map[string]string{"target": `{"pk":["a","b"],"ck":["c"]}`},
			},
			want: "CREATE INDEX idx ON ks.t ((a,b), c);",
		},
		{
			name: "custom indexes are skipped -- Scylla does not support them",
			im: &IndexMetadata{
				Name: "custom", KeyspaceName: "ks", TableName: "t", Kind: IndexKindCustom,
				Options: map[string]string{"target": "x"},
			},
			wantNot: "CREATE INDEX",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var sb strings.Builder
			ks := &KeyspaceMetadata{Name: "ks"}
			if err := ks.indexToCQL(&sb, tc.im); err != nil {
				t.Fatalf("indexToCQL: %v", err)
			}
			got := sb.String()
			if tc.want != "" && !strings.Contains(got, tc.want) {
				t.Errorf("missing %q\n--- got ---\n%s", tc.want, got)
			}
			if tc.wantNot != "" && strings.Contains(got, tc.wantNot) {
				t.Errorf("emitted %q when it should have been skipped\n--- got ---\n%s", tc.wantNot, got)
			}
		})
	}
}

func TestViewToCQL(t *testing.T) {
	t.Parallel()

	base := &KeyspaceMetadata{Name: "ks"}

	t.Run("named columns are listed", func(t *testing.T) {
		t.Parallel()

		vm := &ViewMetadata{
			KeyspaceName: "ks", ViewName: "v", BaseTableName: "t",
			WhereClause:    "id IS NOT NULL",
			OrderedColumns: []string{"id", "name"},
			PartitionKey:   []*ColumnMetadata{col("id", "int", ColumnPartitionKey)},
		}
		var sb strings.Builder
		if err := base.viewToCQL(&sb, vm); err != nil {
			t.Fatalf("viewToCQL: %v", err)
		}
		got := sb.String()
		for _, want := range []string{
			"CREATE MATERIALIZED VIEW ks.v AS",
			"FROM ks.t",
			"WHERE id IS NOT NULL",
			"name",
		} {
			if !strings.Contains(got, want) {
				t.Errorf("missing %q\n--- got ---\n%s", want, got)
			}
		}
	})

	t.Run("IncludeAllColumns selects star", func(t *testing.T) {
		t.Parallel()

		vm := &ViewMetadata{
			KeyspaceName: "ks", ViewName: "v", BaseTableName: "t",
			WhereClause: "id IS NOT NULL", IncludeAllColumns: true,
			OrderedColumns: []string{"id"},
			PartitionKey:   []*ColumnMetadata{col("id", "int", ColumnPartitionKey)},
		}
		var sb strings.Builder
		if err := base.viewToCQL(&sb, vm); err != nil {
			t.Fatalf("viewToCQL: %v", err)
		}
		if got := sb.String(); !strings.Contains(got, "SELECT *") {
			t.Errorf("expected SELECT *\n--- got ---\n%s", got)
		}
	})
}

func TestFunctionAndAggregateToCQL(t *testing.T) {
	t.Parallel()

	ks := &KeyspaceMetadata{Name: "ks"}

	fn := &FunctionMetadata{
		Keyspace: "ks", Name: "avgfinal",
		ArgumentNames: []string{"state"}, ArgumentTypes: []string{"frozen<tuple<int, double>>"},
		ReturnType: "double", Language: "lua", Body: "return 1", CalledOnNullInput: true,
	}

	t.Run("function", func(t *testing.T) {
		t.Parallel()

		var sb strings.Builder
		if err := ks.functionToCQL(&sb, "ks", fn); err != nil {
			t.Fatalf("functionToCQL: %v", err)
		}
		got := sb.String()
		for _, want := range []string{
			"CREATE FUNCTION ks.avgfinal",
			"state",
			// stripFrozen unwraps the argument type.
			"tuple<int, double>",
			"CALLED ON NULL INPUT",
			"RETURNS double",
			"LANGUAGE lua",
			"$$return 1$$",
		} {
			if !strings.Contains(got, want) {
				t.Errorf("missing %q\n--- got ---\n%s", want, got)
			}
		}
	})

	t.Run("function that returns null on null input", func(t *testing.T) {
		t.Parallel()

		strict := *fn
		strict.CalledOnNullInput = false
		var sb strings.Builder
		if err := ks.functionToCQL(&sb, "ks", &strict); err != nil {
			t.Fatalf("functionToCQL: %v", err)
		}
		if got := sb.String(); !strings.Contains(got, "RETURNS NULL ON NULL INPUT") {
			t.Errorf("expected RETURNS NULL ON NULL INPUT\n--- got ---\n%s", got)
		}
	})

	t.Run("aggregate", func(t *testing.T) {
		t.Parallel()

		am := &AggregateMetadata{
			Keyspace: "ks", Name: "average",
			ArgumentTypes: []string{"double"},
			ReturnType:    "double",
			StateType:     "frozen<tuple<int, double>>",
			InitCond:      "(0, 0)",
			StateFunc:     FunctionMetadata{Name: "avgstate"},
			FinalFunc:     FunctionMetadata{Name: "avgfinal"},
		}
		var sb strings.Builder
		if err := ks.aggregateToCQL(&sb, am); err != nil {
			t.Fatalf("aggregateToCQL: %v", err)
		}
		got := sb.String()
		for _, want := range []string{
			"CREATE AGGREGATE ks.average(",
			"SFUNC avgstate",
			// The template applies stripFrozen to StateType, so this renders
			// "tuple<int, double>" where
			// testdata/recreate/aggregates_golden.cql has
			// "frozen<tuple<int, double>>". Both are valid CQL: a tuple is
			// intrinsically frozen, so the wrapper is redundant rather than
			// required. Checked against ScyllaDB 2026.3.0 -- it accepts either
			// spelling and echoes back the frozen one, which is why the golden,
			// captured from DESCRIBE, has it. A cosmetic divergence, not a
			// defect.
			"STYPE tuple<int, double>",
			"FINALFUNC avgfinal",
			"INITCOND (0, 0)",
		} {
			if !strings.Contains(got, want) {
				t.Errorf("missing %q\n--- got ---\n%s", want, got)
			}
		}
	})
}

// TestScyllaEncryptionOptionsUnmarshalBinary exercises the blob decoder that
// tableExtensionsToCQL depends on. It was TestScyllaEncryptionOptionsUnmarshaller
// in recreate_test.go, which is integration-tagged, so the decoder had no
// coverage in the lane that runs on every PR -- even though it only reads two
// files and unmarshals, and needs no cluster. Moved here rather than copied, so
// there is no counterpart to keep in sync.
func TestScyllaEncryptionOptionsUnmarshalBinary(t *testing.T) {
	t.Parallel()

	blob, err := os.ReadFile("testdata/recreate/scylla_encryption_options.bin")
	if err != nil {
		t.Fatal(err)
	}
	goldenBuf, err := os.ReadFile("testdata/recreate/scylla_encryption_options_golden.json")
	if err != nil {
		t.Fatal(err)
	}

	want := &scyllaEncryptionOptions{}
	if err := json.Unmarshal(goldenBuf, want); err != nil {
		t.Fatal(err)
	}

	got := &scyllaEncryptionOptions{}
	if err := got.UnmarshalBinary(blob); err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("decoded blob differs from the golden (-want +got):\n%s", diff)
	}
}
