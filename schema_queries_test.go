//go:build integration && scylla
// +build integration,scylla

package gocql

import (
	"testing"
)

func TestSchemaQueries(t *testing.T) {
	cluster := createCluster()

	fallback := RoundRobinHostPolicy()
	cluster.PoolConfig.HostSelectionPolicy = TokenAwareHostPolicy(fallback)

	session := createSessionFromCluster(cluster, t)
	defer session.Close()

	assertTrue(t, "keyspace present in schemaDescriber", session.schemaDescriber.cache["gocql_test"].Name == "gocql_test")
}
