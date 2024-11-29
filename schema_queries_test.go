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

	keyspaceMetadata, err := session.metadataDescriber.getSchema("gocql_test")
	if err != nil {
		t.Fatal("unable to get keyspace metadata for keyspace: ", err)
	}
	assertTrue(t, "keyspace present in metadataDescriber", keyspaceMetadata.Name == "gocql_test")
}
