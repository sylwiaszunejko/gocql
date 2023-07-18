//go:build tablet
// +build tablet

package gocql

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"
)

// Check if TokenAwareHostPolicy works correctly when using tablets
func TestTablets(t *testing.T) {
	cluster := createMultiNodeCluster()

	fallback := RoundRobinHostPolicy()
	cluster.PoolConfig.HostSelectionPolicy = TokenAwareHostPolicy(fallback)

	session := createSessionFromMultiNodeCluster(cluster, t)
	defer session.Close()

	if err := createTable(session, fmt.Sprintf(`CREATE TABLE %s.%s (pk int, ck int, v int, PRIMARY KEY (pk, ck));
	`, "test1", "table1")); err != nil {
		panic(fmt.Sprintf("unable to create table: %v", err))
	}

	hosts, _, err := session.hostSource.GetHosts()
	assertTrue(t, "err == nil", err == nil)

	hostAddresses := []string{}
	for _, host := range hosts {
		hostAddresses = append(hostAddresses, host.connectAddress.String())
	}

	ctx := context.Background()

	time.Sleep(2 * time.Second)

	i := 0
	for i < 50 {
		i = i + 1
		err = session.Query(`INSERT INTO test1.table1 (pk, ck, v) VALUES (?, ?, ?);`, i, i%5, i%2).WithContext(ctx).RoutingKey([]byte{}).Exec()
		assertTrue(t, "err == nil", err == nil)
	}

	i = 0
	for i < 50 {
		i = i + 1

		var pk int
		var ck int
		var v int

		buf := &bytes.Buffer{}
		trace := NewTraceWriter(session, buf)

		err = session.Query(`SELECT pk, ck, v FROM test1.table1 WHERE pk = ?;`, i).WithContext(ctx).Consistency(One).Trace(trace).Scan(&pk, &ck, &v)
		assertTrue(t, "err == nil", err == nil)

		queriedHosts := 0
		for _, hostAddress := range hostAddresses {
			if strings.Contains(buf.String(), hostAddress) {
				queriedHosts = queriedHosts + 1
			}
		}

		assertEqual(t, "queriedHosts", 1, queriedHosts)
	}
}

// Check if shard awareness works correctly when using tablets
func TestTabletsShardAwareness(t *testing.T) {
	cluster := createMultiNodeCluster()

	fallback := RoundRobinHostPolicy()
	cluster.PoolConfig.HostSelectionPolicy = TokenAwareHostPolicy(fallback)

	session := createSessionFromMultiNodeCluster(cluster, t)
	defer session.Close()

	if err := createTable(session, fmt.Sprintf(`CREATE TABLE %s.%s (pk int, ck int, v int, PRIMARY KEY (pk, ck));
	`, "test1", "table_shard")); err != nil {
		panic(fmt.Sprintf("unable to create table: %v", err))
	}

	ctx := context.Background()

	time.Sleep(2 * time.Second)

	i := 0
	for i < 50 {
		i = i + 1
		err := session.Query(`INSERT INTO test1.table_shard (pk, ck, v) VALUES (?, ?, ?);`, i, i%5, i%2).WithContext(ctx).RoutingKey([]byte{}).Exec()
		assertTrue(t, "err == nil", err == nil)
	}

	i = 0
	for i < 50 {
		i = i + 1

		var pk int
		var ck int
		var v int

		buf := &bytes.Buffer{}
		trace := NewTraceWriter(session, buf)

		err := session.Query(`SELECT pk, ck, v FROM test1.table_shard WHERE pk = ?;`, i).WithContext(ctx).Consistency(One).Trace(trace).Scan(&pk, &ck, &v)
		assertTrue(t, "err == nil", err == nil)

		re := regexp.MustCompile(`\[shard .*\]`)

		shards := re.FindAllString(buf.String(), -1)

		// find duplicates to check how many shards are used
		allShards := make(map[string]bool)
		shardList := []string{}
		for _, item := range shards {
			if _, value := allShards[item]; !value {
				allShards[item] = true
				shardList = append(shardList, item)
			}
		}

		assertTrue(t, "len(shardList) == 1", len(shardList) == 1)
	}
}

// Check if adding new table changes tablets table
func TestTabletsRefresh(t *testing.T) {
	cluster := createMultiNodeCluster()

	fallback := RoundRobinHostPolicy()
	cluster.PoolConfig.HostSelectionPolicy = TokenAwareHostPolicy(fallback)

	session := createSessionFromMultiNodeCluster(cluster, t)
	defer session.Close()

	time.Sleep(1 * time.Second)
	tablets1 := session.getTablets()

	if err := createTable(session, fmt.Sprintf(`CREATE TABLE %s.%s (pk int, ck int, v int, PRIMARY KEY (pk, ck));
	`, "test1", "table2")); err != nil {
		panic(fmt.Sprintf("unable to create table: %v", err))
	}

	time.Sleep(1 * time.Second)
	tablets2 := session.getTablets()

	assertTrue(t, "len(tablet1) < len(tablet2)", len(tablets1) < len(tablets2))
}
