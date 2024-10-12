module github.com/gocql/gocql/bench

go 1.22

require (
	github.com/brianvoe/gofakeit/v6 v6.28.0
	github.com/gocql/gocql v1.7.0
)

require (
	github.com/golang/snappy v0.0.3 // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
)

replace github.com/gocql/gocql => github.com/scylladb/gocql v1.14.4
