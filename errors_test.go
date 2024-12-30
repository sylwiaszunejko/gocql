//go:build all || cassandra
// +build all cassandra

package gocql

import (
	"errors"
	"testing"
)

func TestErrorsParse(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, `CREATE TABLE gocql_test.errors_parse (id int primary key)`); err != nil {
		t.Fatal("create:", err)
	}

	if err := createTable(session, `CREATE TABLE gocql_test.errors_parse (id int primary key)`); err == nil {
		t.Fatal("Should have gotten already exists error from cassandra server.")
	} else {
		e := &RequestErrAlreadyExists{}
		if errors.As(err, &e) {
			if e.Table != "errors_parse" {
				t.Fatalf("expected error table to be 'errors_parse' but was %q", e.Table)
			}
		} else {
			t.Fatalf("expected to get RequestErrAlreadyExists instead got %T", e)
		}
	}
}
