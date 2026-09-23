//go:build integration
// +build integration

package gocql

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	frm "github.com/gocql/gocql/internal/frame"
)

// SELECT without a FROM clause (scylladb/scylladb#24070, PR #29028) resolves the
// select list against a virtual system.one_row table holding a single row. It is
// a ScyllaDB feature, first released in 2026.4.0.

// selectWithoutFromOnce guards the one probe query behind
// selectWithoutFromSupported.
var (
	selectWithoutFromOnce      sync.Once
	selectWithoutFromSupported bool
	selectWithoutFromProbeErr  error
)

// skipUnlessSelectWithoutFrom skips the calling test when the server under test
// cannot run a SELECT without a FROM clause.
//
// The feature ships in ScyllaDB 2026.4.0 and has no Cassandra equivalent, so the
// version comparison alone would keep these tests dark on every currently
// released server. The probe is what decides: it runs `SELECT 1` once and reads
// the answer off the wire. A syntax error means the server does not have the
// feature and the tests skip; any other error is a real failure and is reported
// as one, rather than being silently turned into a skip. See
// probeSelectWithoutFrom for why only a syntax error qualifies.
func skipUnlessSelectWithoutFrom(tb testing.TB) {
	tb.Helper()

	if *flagDistribution == "cassandra" {
		tb.Skip("SELECT without FROM is a ScyllaDB feature; not supported by Cassandra")
	}

	selectWithoutFromOnce.Do(probeSelectWithoutFrom)

	if selectWithoutFromProbeErr != nil {
		tb.Fatalf("probing for SELECT without FROM support failed: %v", selectWithoutFromProbeErr)
	}
	if !selectWithoutFromSupported {
		tb.Skip("SELECT without FROM is not supported by this server (needs ScyllaDB 2026.4.0 or later)")
	}
}

// probeSelectWithoutFrom runs the probe query once for the whole package.
//
// Unlike probeTabletsSupported in common_test.go this does not panic: an
// unsupported server has to skip cleanly, not take the test binary down with it.
func probeSelectWithoutFrom() {
	cluster := createCluster()
	session, err := cluster.CreateSession()
	if err != nil {
		selectWithoutFromProbeErr = err
		return
	}
	defer session.Close()

	var probe int
	err = session.Query("SELECT 1").Scan(&probe)
	if err == nil {
		selectWithoutFromSupported = true
		return
	}

	// A server without the feature cannot parse the statement at all, so it
	// fails in the grammar and answers with a syntax error. Only that means
	// "unsupported".
	//
	// An invalid-query error must not be accepted here. That is what a server
	// that *does* support the feature returns for a statement it parsed and then
	// refused, so treating it as "unsupported" would silently skip every
	// assertion in this file. Anything else -- a timeout, an unavailable
	// replica, a decoding bug -- is a failure worth surfacing.
	if serverErrorCode(err) == ErrCodeSyntax {
		return
	}

	selectWithoutFromProbeErr = err
}

// serverErrorCode returns the CQL error code the server answered with, or 0 if
// err did not come from the server.
//
// The frame error arrives wrapped in a *QueryError, so this has to unwrap rather
// than type-assert.
func serverErrorCode(err error) int {
	var errFrame frm.ErrorFrame
	if !errors.As(err, &errFrame) {
		return 0
	}
	return errFrame.Code
}

// isRejectedByServer reports whether err is the server refusing a statement it
// understood well enough to judge, rather than anything having gone wrong.
//
// Both codes are accepted here, unlike in the probe: a refusal can come from the
// grammar (DISTINCT, a cast spelling) or from a semantic check (an unknown
// column, an untypeable bind marker).
func isRejectedByServer(err error) bool {
	switch serverErrorCode(err) {
	case ErrCodeSyntax, ErrCodeInvalid:
		return true
	}
	return false
}

// createSessionWithoutKeyspace builds a session with no keyspace set at all.
//
// Every helper in common_test.go assigns ClusterConfig.Keyspace, which is
// exactly what these tests must not rely on: a SELECT without FROM touches no
// keyspace and no table, so it has to work on a session that never picked one.
func createSessionWithoutKeyspace(tb testing.TB) *Session {
	tb.Helper()

	cluster := createCluster()
	if cluster.Keyspace != "" {
		tb.Fatalf("createCluster set a keyspace (%q); this test needs a session without one", cluster.Keyspace)
	}

	session, err := cluster.CreateSession()
	if err != nil {
		tb.Fatalf("failed to create session: %v", err)
	}
	return session
}

// logColumns records the column metadata the server sent for a no-FROM result.
//
// The generated column names are chosen by the server and are not part of any
// contract the driver can lean on, so this logs them rather than asserting
// anything. Callers assert whatever shape they need.
func logColumns(tb testing.TB, stmt string, cols []ColumnInfo) {
	tb.Helper()

	for i, col := range cols {
		tb.Logf("%s: column %d = %s", stmt, i, col)
	}
}

// TestSelectWithoutFromLiteral covers the headline case from the ticket:
// `SELECT 1` returns one row holding one decodable value, with no keyspace or
// table involved anywhere.
func TestSelectWithoutFromLiteral(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)

	session := createSessionWithoutKeyspace(t)
	defer session.Close()

	iter := session.Query("SELECT 1").Iter()

	cols := iter.Columns()
	logColumns(t, "SELECT 1", cols)
	if len(cols) != 1 {
		t.Fatalf("got %d columns, want 1", len(cols))
	}
	if typ := cols[0].TypeInfo.Type(); typ != TypeInt {
		t.Errorf("got column type %s, want %s", typ, TypeInt)
	}

	if n := iter.NumRows(); n != 1 {
		t.Errorf("got %d rows in the page, want 1: a no-FROM SELECT reads one virtual row", n)
	}

	var got int
	if !iter.Scan(&got) {
		t.Fatalf("no row returned: %v", iter.Close())
	}
	if got != 1 {
		t.Errorf("got %d, want 1", got)
	}

	var extra int
	if iter.Scan(&extra) {
		t.Errorf("a second row was returned (%d); the virtual table holds exactly one", extra)
	}
	if err := iter.Close(); err != nil {
		t.Fatalf("iterator closed with an error: %v", err)
	}
}

// TestSelectWithoutFromFilteredOut covers the case that makes "exactly one row"
// wrong: a WHERE clause is applied to the backing row and can exclude it.
//
// The predicate references "system$dummy", an internal column of the virtual
// table rather than part of the feature's surface. This test does not endorse
// depending on that name; it is the only way to reach an empty no-FROM result,
// and what is being pinned is the driver's half -- an empty result decodes as
// ErrNotFound and a clean iterator, not as an error or a phantom row.
func TestSelectWithoutFromFilteredOut(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)

	session := createSessionWithoutKeyspace(t)
	defer session.Close()

	const (
		kept     = `SELECT 1 WHERE "system$dummy" = ''`
		filtered = `SELECT 1 WHERE "system$dummy" = 'no'`
	)

	t.Run("predicate keeps the row", func(t *testing.T) {
		var got int
		if err := session.Query(kept).Scan(&got); err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if got != 1 {
			t.Errorf("got %d, want 1", got)
		}
	})

	t.Run("predicate removes the row", func(t *testing.T) {
		iter := session.Query(filtered).Iter()

		// The column metadata still describes the select list even though no row
		// comes back, so a caller can inspect it before finding the result empty.
		if cols := iter.Columns(); len(cols) != 1 {
			t.Errorf("got %d columns, want 1 even for an empty result", len(cols))
		}
		if n := iter.NumRows(); n != 0 {
			t.Errorf("got %d rows, want 0: the predicate excludes the backing row", n)
		}

		var got int
		if iter.Scan(&got) {
			t.Errorf("a row was returned (%d); the predicate excludes it", got)
		}
		if err := iter.Close(); err != nil {
			t.Fatalf("iterator closed with an error: %v", err)
		}

		// Through Query.Scan the same emptiness surfaces as ErrNotFound, which is
		// what the docs tell callers to expect.
		err := session.Query(filtered).Scan(&got)
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("got %v, want ErrNotFound", err)
		}
	})
}

// TestSelectWithoutFromNow covers the ticket's second named case, `SELECT
// now()`: a function selector whose value the driver has to decode as a
// timeuuid.
func TestSelectWithoutFromNow(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)

	session := createSessionWithoutKeyspace(t)
	defer session.Close()

	before := time.Now()

	iter := session.Query("SELECT now()").Iter()

	cols := iter.Columns()
	logColumns(t, "SELECT now()", cols)
	if len(cols) != 1 {
		t.Fatalf("got %d columns, want 1", len(cols))
	}
	if typ := cols[0].TypeInfo.Type(); typ != TypeTimeUUID {
		t.Errorf("got column type %s, want %s", typ, TypeTimeUUID)
	}

	var got UUID
	if !iter.Scan(&got) {
		t.Fatalf("no row returned: %v", iter.Close())
	}
	if err := iter.Close(); err != nil {
		t.Fatalf("iterator closed with an error: %v", err)
	}

	after := time.Now()

	if v := got.Version(); v != 1 {
		t.Errorf("got UUID version %d, want 1 (timeuuid)", v)
	}
	// A generous window: this only has to catch a value that decoded into
	// nonsense, not clock skew between the test host and the server.
	if ts := got.Time(); ts.Before(before.Add(-time.Hour)) || ts.After(after.Add(time.Hour)) {
		t.Errorf("now() decoded to %s, which is not within an hour of the test run (%s..%s)", ts, before, after)
	}
}

// TestSelectWithoutFromFunctionCast covers a nested function selector, where the
// column type is inferred through a conversion rather than read off a literal.
func TestSelectWithoutFromFunctionCast(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)

	session := createSessionWithoutKeyspace(t)
	defer session.Close()

	before := time.Now()

	iter := session.Query("SELECT toTimestamp(now())").Iter()

	cols := iter.Columns()
	logColumns(t, "SELECT toTimestamp(now())", cols)
	if len(cols) != 1 {
		t.Fatalf("got %d columns, want 1", len(cols))
	}
	if typ := cols[0].TypeInfo.Type(); typ != TypeTimestamp {
		t.Errorf("got column type %s, want %s", typ, TypeTimestamp)
	}

	var got time.Time
	if !iter.Scan(&got) {
		t.Fatalf("no row returned: %v", iter.Close())
	}
	if err := iter.Close(); err != nil {
		t.Fatalf("iterator closed with an error: %v", err)
	}

	after := time.Now()

	if got.Before(before.Add(-time.Hour)) || got.After(after.Add(time.Hour)) {
		t.Errorf("toTimestamp(now()) decoded to %s, which is not within an hour of the test run (%s..%s)", got, before, after)
	}
}

// TestSelectWithoutFromMultipleSelectors covers a select list of more than one
// value. This is the case that exercises the multi-column half of
// parseResultMetadata: when the server does not set GLOBAL_TABLES_SPEC, the
// keyspace and table are read from column 0 and skipped for the rest, so a
// mismatch here shows up as a misdecoded value rather than a clean error.
func TestSelectWithoutFromMultipleSelectors(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)

	session := createSessionWithoutKeyspace(t)
	defer session.Close()

	const stmt = "SELECT 1, 'a', true"

	iter := session.Query(stmt).Iter()

	cols := iter.Columns()
	logColumns(t, stmt, cols)
	if len(cols) != 3 {
		t.Fatalf("got %d columns, want 3", len(cols))
	}
	wantTypes := []Type{TypeInt, TypeVarchar, TypeBoolean}
	for i, want := range wantTypes {
		if typ := cols[i].TypeInfo.Type(); typ != want {
			t.Errorf("column %d: got type %s, want %s", i, typ, want)
		}
	}

	var (
		gotInt  int
		gotText string
		gotBool bool
	)
	if !iter.Scan(&gotInt, &gotText, &gotBool) {
		t.Fatalf("no row returned: %v", iter.Close())
	}
	if err := iter.Close(); err != nil {
		t.Fatalf("iterator closed with an error: %v", err)
	}

	if gotInt != 1 || gotText != "a" || gotBool != true {
		t.Errorf("got (%d, %q, %t), want (1, \"a\", true)", gotInt, gotText, gotBool)
	}
}

// TestSelectWithoutFromAlias covers AS, which is the only way a caller gets to
// choose a column name.
//
// Without it the name is the selector's own text, which is awkward to use as a
// map key and is not something an application should depend on. This pins down
// that AS overrides it and that unaliased selectors alongside it are unaffected.
func TestSelectWithoutFromAlias(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)

	session := createSessionWithoutKeyspace(t)
	defer session.Close()

	const stmt = "SELECT 1, 'hi' AS greeting, now()"

	iter := session.Query(stmt).Iter()

	cols := iter.Columns()
	logColumns(t, stmt, cols)

	var names []string
	for _, col := range cols {
		names = append(names, col.Name)
	}
	want := []string{"1", "greeting", "system.now()"}
	if !slices.Equal(names, want) {
		t.Errorf("got column names %q, want %q", names, want)
	}

	var (
		one      int
		greeting string
		now      UUID
	)
	if !iter.Scan(&one, &greeting, &now) {
		t.Fatalf("no row returned: %v", iter.Close())
	}
	if err := iter.Close(); err != nil {
		t.Fatalf("iterator closed with an error: %v", err)
	}
	if one != 1 || greeting != "hi" {
		t.Errorf("got (%d, %q), want (1, \"hi\")", one, greeting)
	}

	// The alias is what MapScan keys on, which is the point of using one.
	// Query.MapScan closes the iterator and returns the underlying error, which
	// Iter.MapScan reduces to a bare false.
	row := make(map[string]any)
	if err := session.Query(stmt).MapScan(row); err != nil {
		t.Fatalf("MapScan failed: %v", err)
	}
	if _, ok := row["greeting"]; !ok {
		t.Errorf("MapScan keys are %v; want one named %q", row, "greeting")
	}
}

// execMode is one of the three ways a SELECT can reach the server, each of which
// decodes its result metadata through a different path.
type execMode struct {
	name string
	// apply returns the query to run. It may reach into unexported fields.
	apply func(q *Query) *Query
}

// execModes covers every way the driver can issue a no-FROM SELECT.
//
// The distinction is not cosmetic. A prepared statement normally sets
// SKIP_METADATA on EXECUTE, so the columns come from the metadata the PREPARE
// response carried (parseResultPrepared). NoSkipMetadata forces the server to
// send the metadata with the rows instead, and a plain QUERY frame always does
// -- both of which land in parseResultMetadata, whose keyspace/table handling is
// the part with the single-table assumption. All three must agree.
var execModes = []execMode{
	{
		name:  "prepared",
		apply: func(q *Query) *Query { return q },
	},
	{
		name:  "prepared with metadata",
		apply: func(q *Query) *Query { return q.NoSkipMetadata() },
	},
	{
		name: "not prepared",
		apply: func(q *Query) *Query {
			// SELECT is DML, so shouldPrepare is always true and there is no
			// exported way to ask for a plain QUERY frame. Conn.querySystem
			// sets the same field for the same reason.
			q.skipPrepare = true
			return q
		},
	},
}

// TestSelectWithoutFromExecutionModes checks that a no-FROM SELECT decodes
// identically however it reaches the server: prepared, prepared with the result
// metadata forced back on, and unprepared.
func TestSelectWithoutFromExecutionModes(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)

	session := createSessionWithoutKeyspace(t)
	defer session.Close()

	const stmt = "SELECT 1, 'a', true"

	type result struct {
		cols []ColumnInfo
		i    int
		s    string
		b    bool
	}
	results := make(map[string]result, len(execModes))

	for _, mode := range execModes {
		t.Run(mode.name, func(t *testing.T) {
			iter := mode.apply(session.Query(stmt)).Iter()

			var got result
			got.cols = iter.Columns()
			logColumns(t, stmt+" ("+mode.name+")", got.cols)

			// Pinned absolutely, not just compared across modes: agreeing on a
			// wrong or empty pair would satisfy the comparison below and still be
			// wrong. The docs state this pair, so it needs a test behind it.
			for i, col := range got.cols {
				if col.Keyspace != "system" || col.Table != "one_row" {
					t.Errorf("column %d is %s.%s, want system.one_row", i, col.Keyspace, col.Table)
				}
			}

			if !iter.Scan(&got.i, &got.s, &got.b) {
				t.Fatalf("no row returned: %v", iter.Close())
			}
			if err := iter.Close(); err != nil {
				t.Fatalf("iterator closed with an error: %v", err)
			}
			if got.i != 1 || got.s != "a" || got.b != true {
				t.Errorf("got (%d, %q, %t), want (1, \"a\", true)", got.i, got.s, got.b)
			}

			results[mode.name] = got
		})
	}

	// Compare every mode against the first one that ran. A disagreement here is
	// the interesting failure: it means one of the two metadata parsers reads a
	// no-FROM result differently from the other.
	base, ok := results[execModes[0].name]
	if !ok {
		t.Fatalf("%s did not record a result", execModes[0].name)
	}
	for _, mode := range execModes[1:] {
		got, ok := results[mode.name]
		if !ok {
			continue // that subtest already failed
		}
		if len(got.cols) != len(base.cols) {
			t.Errorf("%s: got %d columns, %s got %d", mode.name, len(got.cols), execModes[0].name, len(base.cols))
			continue
		}
		for i := range got.cols {
			if got.cols[i].Name != base.cols[i].Name {
				t.Errorf("%s: column %d named %q, %s named it %q", mode.name, i, got.cols[i].Name, execModes[0].name, base.cols[i].Name)
			}
			if got.cols[i].TypeInfo.Type() != base.cols[i].TypeInfo.Type() {
				t.Errorf("%s: column %d has type %s, %s had %s", mode.name, i, got.cols[i].TypeInfo.Type(), execModes[0].name, base.cols[i].TypeInfo.Type())
			}
			if got.cols[i].Keyspace != base.cols[i].Keyspace || got.cols[i].Table != base.cols[i].Table {
				t.Errorf("%s: column %d is %s.%s, %s had %s.%s", mode.name, i,
					got.cols[i].Keyspace, got.cols[i].Table, execModes[0].name, base.cols[i].Keyspace, base.cols[i].Table)
			}
		}
	}
}

// TestSelectWithoutFromBindMarker covers a bind marker in the select list, which
// the server PR allows.
//
// A marker needs a type and a no-FROM select list has no columns to infer one
// from, so the server accepts one only where something else fixes the type: a
// function signature, or a clause such as LIMIT. The spellings that look right
// and are refused are pinned too, since they are what a caller reaches for
// first. What a bind marker does to routing is TestSelectWithoutFromRoutingKey.
func TestSelectWithoutFromBindMarker(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)

	session := createSessionWithoutKeyspace(t)
	defer session.Close()

	// A bare marker has nothing to infer a type from and the server refuses it,
	// so the marker has to sit inside a call whose signature pins the type.
	t.Run("value round trip", func(t *testing.T) {
		var got int
		if err := session.Query("SELECT blobAsInt(?)", []byte{0, 0, 0, 42}).Scan(&got); err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if got != 42 {
			t.Errorf("got %d, want 42", got)
		}
	})

	t.Run("marker types the column", func(t *testing.T) {
		iter := session.Query("SELECT intAsBlob(?)", 42).Iter()

		cols := iter.Columns()
		logColumns(t, "SELECT intAsBlob(?)", cols)
		if len(cols) != 1 {
			t.Fatalf("got %d columns, want 1", len(cols))
		}
		if typ := cols[0].TypeInfo.Type(); typ != TypeBlob {
			t.Errorf("got column type %s, want %s", typ, TypeBlob)
		}

		var got []byte
		if !iter.Scan(&got) {
			t.Fatalf("no row returned: %v", iter.Close())
		}
		if err := iter.Close(); err != nil {
			t.Fatalf("iterator closed with an error: %v", err)
		}
		if want := []byte{0, 0, 0, 42}; !bytes.Equal(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	// LIMIT fixes the type of a marker too, without any function involved.
	t.Run("marker in LIMIT", func(t *testing.T) {
		var got int
		if err := session.Query("SELECT 1 LIMIT ?", 1).Scan(&got); err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if got != 1 {
			t.Errorf("got %d, want 1", got)
		}
	})

	// The spellings that do not work. These are server limitations rather than
	// driver ones, but they are the ones a caller reaches for first, so the
	// rejection is pinned down rather than left to be rediscovered.
	t.Run("untyped markers rejected", func(t *testing.T) {
		for _, stmt := range []string{"SELECT ?", "SELECT (int)?", "SELECT CAST(? AS int)"} {
			err := session.Query(stmt, 42).Exec()
			if err == nil {
				t.Errorf("%q was accepted; it was expected to be rejected for want of a type", stmt)
				continue
			}
			t.Logf("%s: %v", stmt, err)
			if !isRejectedByServer(err) {
				t.Errorf("%q: got %T (%v), want a syntax or invalid-query error", stmt, err, err)
			}
		}
	})
}

// TestSelectWithoutFromRoutingKey pins down what the routing-key machinery makes
// of a no-FROM statement.
//
// A no-FROM SELECT has no partition key, so the only correct answer is "no
// routing key", and that is what callers get. How the driver arrives there
// differs by statement, and the difference is a real (if invisible) cost:
//
//   - With no bind markers the prepared metadata has no columns, so
//     Session.routingKeyInfo returns early with no key and no error, and the
//     result is cached.
//   - With a bind marker it goes further. resolveRoutingKeyspaceTable reads the
//     bind-variable metadata, which carries table "one_row" but no keyspace, so
//     the keyspace falls back to the session's -- or stays empty when the
//     session has none. Either way the TableMetadata lookup that follows fails,
//     because system.one_row is virtual and no such table exists under the
//     substituted keyspace. routingKeyInfo returns that error and does not cache
//     it, so every execution repeats the lookup. The prepare it goes through
//     first is usually a stmtsLRU hit and stays local, but that cache is keyed
//     per host and the connection comes from getConn rather than from the policy,
//     so a cold or evicted entry sends a PREPARE -- possibly to a host that is
//     not the one running the query.
//
// Neither case reaches the caller: tokenAwareHostPolicy.Pick falls back on a nil
// key and on an error alike (see TestSelectWithoutFromTokenAware). This test
// exists to keep that true, and to notice if the error ever turns into something
// callers can see.
func TestSelectWithoutFromRoutingKey(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)

	for _, tc := range []struct {
		name string
		stmt string
		// withKeyspace picks which session to run on, which decides whether the
		// keyspace substituted for the missing one is empty or the session's.
		withKeyspace bool
		values       []any
		// wantErr is the error the routing lookup is expected to fail with, or
		// nil when it is expected to succeed. The two failures differ because the
		// substituted keyspace does: with none, TableMetadata rejects the empty
		// keyspace outright; with one, it goes looking for a table that is not
		// there. Naming them keeps an unrelated prepare or timeout failure from
		// passing as the documented behaviour.
		wantErr error
	}{
		{name: "no bind markers", stmt: "SELECT 1"},
		{name: "no bind markers, with keyspace", stmt: "SELECT 1", withKeyspace: true},
		{name: "bind marker", stmt: "SELECT intAsBlob(?)", values: []any{42}, wantErr: ErrNoKeyspace},
		{name: "bind marker, with keyspace", stmt: "SELECT intAsBlob(?)", values: []any{42}, withKeyspace: true, wantErr: ErrNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var session *Session
			if tc.withKeyspace {
				session = createSession(t)
			} else {
				session = createSessionWithoutKeyspace(t)
			}
			defer session.Close()

			query := session.Query(tc.stmt, tc.values...)

			routingKey, err := query.GetRoutingKey()
			t.Logf("%s: GetRoutingKey() = %v, %v", tc.stmt, routingKey, err)

			if routingKey != nil {
				t.Errorf("got routing key %v, want none: a no-FROM SELECT has no partition key", routingKey)
			}
			switch {
			case tc.wantErr != nil && !errors.Is(err, tc.wantErr):
				// Covers err == nil too: the virtual table has no schema metadata
				// to find, so the lookup is expected to fail, and to fail this way.
				t.Errorf("GetRoutingKey returned %v, want %v", err, tc.wantErr)
			case tc.wantErr == nil && err != nil:
				t.Errorf("GetRoutingKey failed: %v", err)
			}

			// Whatever GetRoutingKey decided, the query itself must still run.
			if err := query.Exec(); err != nil {
				t.Fatalf("query failed after GetRoutingKey: %v", err)
			}
		})
	}
}

// TestSelectWithoutFromTokenAware runs a no-FROM SELECT under a token-aware
// policy, which is the configuration most users have.
//
// Pick calls GetRoutingKey and falls back to the wrapped policy on a nil key or
// an error, so this must succeed either way. It is here to prove that the
// fallback is reached rather than the error escaping to the caller.
//
// Both branches need a statement of their own. Without a bind marker
// routingKeyInfo returns early, so Pick sees a nil key and no error; with one it
// reaches the failed schema lookup, so Pick sees the error branch. A statement of
// the first kind alone would leave the branch this file actually documents
// untested.
func TestSelectWithoutFromTokenAware(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)

	cluster := createCluster()
	cluster.PoolConfig.HostSelectionPolicy = TokenAwareHostPolicy(RoundRobinHostPolicy())

	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}
	defer session.Close()

	for _, tc := range []struct {
		name   string
		stmt   string
		values []any
		want   int
	}{
		{name: "nil routing key", stmt: "SELECT 1", want: 1},
		{name: "routing lookup error", stmt: "SELECT blobAsInt(?)", values: []any{[]byte{0, 0, 0, 42}}, want: 42},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Run more than once: the routing-key cache behaves differently on the
			// first execution than on the rest, and a panic or a wedged cache entry
			// would show up on the second.
			for i := 0; i < 3; i++ {
				var got int
				if err := session.Query(tc.stmt, tc.values...).Scan(&got); err != nil {
					t.Fatalf("execution %d failed: %v", i, err)
				}
				if got != tc.want {
					t.Errorf("execution %d: got %d, want %d", i, got, tc.want)
				}
			}
		})
	}
}

// TestSelectWithoutFromMapScan covers the name-keyed scanning helpers.
//
// These key on the column names the server generates for a no-FROM select list,
// which is the one piece of a no-FROM result a caller cannot predict. If two
// selectors are given the same generated name, the map silently keeps one of
// them -- so the column count is checked against the map size.
func TestSelectWithoutFromMapScan(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)

	session := createSessionWithoutKeyspace(t)
	defer session.Close()

	const stmt = "SELECT 1, now()"

	t.Run("MapScan", func(t *testing.T) {
		iter := session.Query(stmt).Iter()
		cols := iter.Columns()
		logColumns(t, stmt, cols)

		row := make(map[string]any)
		if !iter.MapScan(row) {
			t.Fatalf("no row returned: %v", iter.Close())
		}
		if err := iter.Close(); err != nil {
			t.Fatalf("iterator closed with an error: %v", err)
		}

		t.Logf("%s: MapScan keys = %v", stmt, row)
		if len(row) != len(cols) {
			t.Errorf("got %d map entries for %d columns; the server generated a duplicate column name", len(row), len(cols))
		}
	})

	t.Run("SliceMap", func(t *testing.T) {
		rows, err := session.Query(stmt).Iter().SliceMap()
		if err != nil {
			t.Fatalf("SliceMap failed: %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("got %d rows, want 1", len(rows))
		}
		t.Logf("%s: SliceMap row = %v", stmt, rows[0])
	})
}

// TestSelectWithoutFromAggregate covers an aggregate applied to the virtual
// single row. The server PR follows PostgreSQL here: count(5) is 1, not 5.
func TestSelectWithoutFromAggregate(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)

	session := createSessionWithoutKeyspace(t)
	defer session.Close()

	var got int64
	if err := session.Query("SELECT count(5)").Scan(&got); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if got != 1 {
		t.Errorf("got count(5) = %d, want 1 (one virtual row)", got)
	}
}

// TestSelectWithoutFromJSON covers SELECT JSON, which the server PR supports for
// no-FROM statements.
func TestSelectWithoutFromJSON(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)

	session := createSessionWithoutKeyspace(t)
	defer session.Close()

	if session.cfg.ProtoVersion < protoVersion4 {
		t.Skip("skipping JSON support on proto < 4")
	}

	var got string
	if err := session.Query("SELECT JSON 1").Scan(&got); err != nil {
		// No skip here. The probe has already established that this server
		// supports no-FROM SELECTs, and SELECT JSON is part of that feature, so a
		// rejection is a regression rather than a reason to drop the coverage.
		t.Fatalf("query failed: %v", err)
	}
	t.Logf("SELECT JSON 1 = %s", got)

	// Checking only that the document is non-empty would pass on "{}" or on
	// something that is not JSON at all.
	var doc map[string]any
	if err := json.Unmarshal([]byte(got), &doc); err != nil {
		t.Fatalf("result %q is not valid JSON: %v", got, err)
	}
	if len(doc) != 1 {
		t.Fatalf("got %d keys in %s, want 1", len(doc), got)
	}
	// Keyed by the generated column name, as an ordinary result would be.
	value, ok := doc["1"]
	if !ok {
		t.Fatalf("got %s, want a key named %q", got, "1")
	}
	if value != float64(1) {
		t.Errorf("got %v for key %q, want 1", value, "1")
	}
}

// TestSelectWithoutFromWithKeyspace runs the statement on a session that does
// have a keyspace, confirming it decodes the same as on a session without one:
// the select list resolves against system.one_row either way.
//
// That holds for this statement, not for every no-FROM statement.
// TestSelectWithoutFromUnqualifiedUDF covers the name resolution that does
// depend on the session keyspace, and TestSelectWithoutFromRoutingKey the
// routing lookup that substitutes it.
func TestSelectWithoutFromWithKeyspace(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)

	session := createSession(t)
	defer session.Close()

	if session.cfg.Keyspace == "" {
		t.Fatal("createSession did not set a keyspace; this test needs one")
	}

	var got int
	if err := session.Query("SELECT 1").Scan(&got); err != nil {
		t.Fatalf("query failed on a session using keyspace %q: %v", session.cfg.Keyspace, err)
	}
	if got != 1 {
		t.Errorf("got %d, want 1", got)
	}
}

// TestSelectWithoutFromUnqualifiedUDF pins the one part of a no-FROM SELECT that
// does depend on the session keyspace.
//
// Unqualified function names resolve against it. Native functions live in system
// and so resolve from any session -- most of the tests here run on a session with
// no keyspace at all -- but a user-defined function does not, and the statement
// fails with "Unknown function". Qualifying the name, or using a session that has
// a keyspace, both work.
func TestSelectWithoutFromUnqualifiedUDF(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)

	keyspaced := createSession(t)
	defer keyspaced.Close()

	// Created through createTable rather than a bare Exec: the subtests below
	// query through their own sessions, which may reach a different node, so the
	// DDL has to reach schema agreement first or the function is transiently
	// unknown there.
	const fn = "select_without_from_plus_one"
	ddl := fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s.%s(x int)
		RETURNS NULL ON NULL INPUT RETURNS int
		LANGUAGE lua AS 'return x + 1'`, keyspaced.cfg.Keyspace, fn)
	if err := createTable(keyspaced, ddl); err != nil {
		// User-defined functions are a server-side option rather than part of this
		// feature, so a cluster that refuses to create one skips. Only an outright
		// refusal counts: a timeout or a connection failure is a real failure and
		// must not be dressed up as a missing server option.
		if isRejectedByServer(err) {
			t.Skipf("cannot create a user-defined function on this cluster: %v", err)
		}
		t.Fatalf("creating the user-defined function failed: %v", err)
	}

	t.Run("unqualified without a session keyspace", func(t *testing.T) {
		session := createSessionWithoutKeyspace(t)
		defer session.Close()

		var got int
		err := session.Query(fmt.Sprintf("SELECT %s(1)", fn)).Scan(&got)
		if err == nil {
			t.Fatalf("got %d; the function is unqualified and the session has no keyspace to resolve it against", got)
		}
		t.Logf("%s(1) with no session keyspace: %v", fn, err)
		if !isRejectedByServer(err) {
			t.Errorf("got %T (%v), want the server refusing to resolve the name", err, err)
		}
	})

	t.Run("qualified without a session keyspace", func(t *testing.T) {
		session := createSessionWithoutKeyspace(t)
		defer session.Close()

		var got int
		stmt := fmt.Sprintf("SELECT %s.%s(1)", keyspaced.cfg.Keyspace, fn)
		if err := session.Query(stmt).Scan(&got); err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if got != 2 {
			t.Errorf("got %d, want 2", got)
		}
	})

	t.Run("unqualified with a session keyspace", func(t *testing.T) {
		var got int
		if err := keyspaced.Query(fmt.Sprintf("SELECT %s(1)", fn)).Scan(&got); err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if got != 2 {
			t.Errorf("got %d, want 2", got)
		}
	})
}

// TestSelectWithoutFromRejected covers the forms the server PR explicitly
// refuses.
//
// The value here is less in the rejection itself than in what follows it: a
// misparsed error response leaves the connection out of step with the server,
// which shows up as the *next* query on that connection failing rather than
// this one. So each rejection is followed by a query that has to succeed.
//
// Both queries are pinned to one connection, the way Conn.querySystem pins its
// own. Routed through the session they would each pick a host and a connection
// independently -- NumConns defaults to 2 per host -- so the follow-up could
// land on a connection that never saw the rejection and report health that was
// never in question.
func TestSelectWithoutFromRejected(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)

	session := createSessionWithoutKeyspace(t)
	defer session.Close()

	conn := session.getConn()
	if conn == nil {
		t.Fatal("no connection available to pin the queries to")
	}

	// pinned builds a query that runs on conn instead of being routed.
	pinned := func(stmt string, values ...any) *Query {
		q := session.Query(stmt, values...)
		q.conn = conn
		return q
	}

	for _, tc := range []struct {
		name string
		stmt string
	}{
		{name: "star", stmt: "SELECT *"},
		{name: "column reference", stmt: "SELECT some_column"},
		{name: "distinct", stmt: "SELECT DISTINCT 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := pinned(tc.stmt).Exec()
			if err == nil {
				t.Fatalf("%q was accepted; the server is documented to reject it", tc.stmt)
			}
			t.Logf("%s: %v", tc.stmt, err)
			if !isRejectedByServer(err) {
				t.Errorf("got %T (%v), want a syntax or invalid-query error from the server", err, err)
			}

			// That same connection has to still be usable afterwards.
			var got int
			if err := pinned("SELECT 1").Scan(&got); err != nil {
				t.Fatalf("the connection is unusable after %q was rejected: %v", tc.stmt, err)
			}
			if got != 1 {
				t.Errorf("got %d after a rejected statement, want 1", got)
			}
		})
	}
}
