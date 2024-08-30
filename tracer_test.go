//go:build all || cassandra || scylla
// +build all cassandra scylla

package gocql

import "testing"

func TestTracingNewAPI(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, `CREATE TABLE gocql_test.trace2 (id int primary key)`); err != nil {
		t.Fatal("create:", err)
	}

	trace := NewTracer(session)
	if err := session.Query(`INSERT INTO trace2 (id) VALUES (?)`, 42).Trace(trace).Exec(); err != nil {
		t.Fatal("insert:", err)
	}

	var value int
	if err := session.Query(`SELECT id FROM trace2 WHERE id = ?`, 42).Trace(trace).Scan(&value); err != nil {
		t.Fatal("select:", err)
	} else if value != 42 {
		t.Fatalf("value: expected %d, got %d", 42, value)
	}

	for _, traceID := range trace.AllTraceIDs() {
		var (
			isReady bool
			err     error
		)
		for !isReady {
			isReady, err = trace.IsReady(traceID)
			if err != nil {
				t.Fatal("Error: ", err)
			}
		}
		activities, err := trace.GetActivities(traceID)
		if err != nil {
			t.Fatal(err)
		}
		coordinator, _, err := trace.GetCoordinatorTime(traceID)
		if err != nil {
			t.Fatal(err)
		}
		if len(activities) == 0 {
			t.Fatal("Failed to obtain any tracing for tradeID: ", traceID)
		} else if coordinator == "" {
			t.Fatal("Failed to obtain coordinator for traceID: ", traceID)
		}
	}
}
