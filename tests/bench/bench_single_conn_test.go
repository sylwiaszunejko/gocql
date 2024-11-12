package bench_test

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/dialer/recorder"
	"github.com/gocql/gocql/dialer/replayer"
)

func InitializeCluster() {
	cluster := gocql.NewCluster("192.168.100.11")
	cluster.Consistency = gocql.Quorum

	fallback := gocql.RoundRobinHostPolicy()
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback)

	executor, err := gocql.NewSingleHostQueryExecutor(cluster)
	if err != nil {
		panic(err)
	}
	defer executor.Close()

	keyspace := "single_conn_bench"

	err = executor.Exec(`DROP KEYSPACE IF EXISTS ` + keyspace)
	if err != nil {
		panic(fmt.Sprintf("unable to drop keyspace: %v", err))
	}

	err = executor.Exec(fmt.Sprintf(`CREATE KEYSPACE %s WITH replication = {'class' : 'NetworkTopologyStrategy','replication_factor' : 1}`, keyspace))

	if err != nil {
		panic(fmt.Sprintf("unable to create keyspace: %v", err))
	}

	if err = executor.Exec(fmt.Sprintf(`CREATE TABLE %s.%s (pk int, ck int, v text, PRIMARY KEY (pk));
	`, keyspace, "table1")); err != nil {
		panic(fmt.Sprintf("unable to create table: %v", err))
	}
}

func RecordSelectTraffic(size int, dir string) {
	cluster := gocql.NewCluster("192.168.100.11")
	cluster.Consistency = gocql.Quorum

	cluster.Dialer = recorder.NewRecordDialer(dir)

	fallback := gocql.RoundRobinHostPolicy()
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback)

	executor, err := gocql.NewSingleHostQueryExecutor(cluster)
	if err != nil {
		panic(err)
	}
	defer executor.Close()

	for i := 0; i < size; i++ {
		iter := executor.Iter(`SELECT v FROM single_conn_bench.table1 WHERE pk = ?;`, i)
		var name string
		for iter.Scan(&name) {
			if name[:4] != "Name" {
				panic("Wrong value")
			}
		}
		if err := iter.Close(); err != nil {
			panic(err)
		}
	}
}

func RecordInsertTraffic(size int, dir string) {
	cluster := gocql.NewCluster("192.168.100.11")
	cluster.Consistency = gocql.Quorum

	cluster.Dialer = recorder.NewRecordDialer(dir)

	fallback := gocql.RoundRobinHostPolicy()
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback)

	executor, err := gocql.NewSingleHostQueryExecutor(cluster)
	if err != nil {
		panic(err)
	}
	defer executor.Close()

	for i := 0; i < size; i++ {
		err = executor.Exec(`INSERT INTO single_conn_bench.table1 (pk, ck, v) VALUES (?, ?, ?);`, i, i%5, fmt.Sprintf("Name_%d", i))
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkSingleConnectionSelect(b *testing.B) {
	cluster := gocql.NewCluster("192.168.100.11")
	cluster.Consistency = gocql.Quorum

	cluster.Dialer = replayer.NewReplayDialer("rec_select")

	fallback := gocql.RoundRobinHostPolicy()
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback)

	executor, err := gocql.NewSingleHostQueryExecutor(cluster)
	if err != nil {
		panic(err)
	}
	defer executor.Close()

	b.Run("Select", func(b *testing.B) {
		for i := 0; i < 10; i++ {
			b.Run("Case"+strconv.Itoa(i), func(b *testing.B) {
				for j := 0; j < b.N; j++ {
					_ = executor.Iter(`SELECT v FROM single_conn_bench.table1 WHERE pk = ?;`, i)
				}
			})
		}
	})
}

func BenchmarkSingleConnectionInsert(b *testing.B) {
	cluster := gocql.NewCluster("192.168.100.11")
	cluster.Consistency = gocql.Quorum

	cluster.Dialer = replayer.NewReplayDialer("rec_insert")

	fallback := gocql.RoundRobinHostPolicy()
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback)

	executor, err := gocql.NewSingleHostQueryExecutor(cluster)
	if err != nil {
		panic(err)
	}
	defer executor.Close()

	b.Run("Insert", func(b *testing.B) {
		for i := 0; i < 10; i++ {
			b.Run("Case"+strconv.Itoa(i), func(b *testing.B) {
				for j := 0; j < b.N; j++ {
					err = executor.Exec(`INSERT INTO single_conn_bench.table1 (pk, ck, v) VALUES (?, ?, ?);`, i, i%5, fmt.Sprintf("Name_%d", i))
					if err != nil {
						panic(err)
					}
				}
			})
		}
	})
}

func TestMain(m *testing.M) {
	update := flag.Bool("update-golden", false, "Update golden files")
	flag.Parse()
	if *update {
		InitializeCluster()
		RecordInsertTraffic(10, "rec_insert")
		RecordSelectTraffic(10, "rec_select")
	}
	os.Exit(m.Run())
}
