//go:build all || unit
// +build all unit

package gocql

import (
	"sync"
	"testing"
)

var tablets = []*TabletInfo{
	{
		sync.RWMutex{},
		"test1",
		TimeUUID(),
		-6917529027641081857,
		"table1",
		8,
		nil,
		[]ReplicaInfo{{TimeUUID(), 9}},
		"",
	},
	{
		sync.RWMutex{},
		"test1",
		TimeUUID(),
		-4611686018427387905,
		"table1",
		8,
		nil,
		[]ReplicaInfo{{TimeUUID(), 8}},
		"",
	},
	{
		sync.RWMutex{},
		"test1",
		TimeUUID(),
		-2305843009213693953,
		"table1",
		8,
		nil,
		[]ReplicaInfo{{TimeUUID(), 9}},
		"",
	},
	{
		sync.RWMutex{},
		"test1",
		TimeUUID(),
		-1,
		"table1",
		8,
		nil,
		[]ReplicaInfo{{TimeUUID(), 8}},
		"",
	},
	{
		sync.RWMutex{},
		"test1",
		TimeUUID(),
		2305843009213693951,
		"table1",
		8,
		nil,
		[]ReplicaInfo{{TimeUUID(), 3}},
		"",
	},
	{
		sync.RWMutex{},
		"test1",
		TimeUUID(),
		4611686018427387903,
		"table1",
		8,
		nil,
		[]ReplicaInfo{{TimeUUID(), 3}},
		"",
	},
	{
		sync.RWMutex{},
		"test1",
		TimeUUID(),
		6917529027641081855,
		"table1",
		8,
		nil,
		[]ReplicaInfo{{TimeUUID(), 7}},
		"",
	},
	{
		sync.RWMutex{},
		"test1",
		TimeUUID(),
		9223372036854775807,
		"table1",
		8,
		nil,
		[]ReplicaInfo{{TimeUUID(), 7}},
		"",
	},
	{
		sync.RWMutex{},
		"test2",
		TimeUUID(),
		-6917529027641081857,
		"table1",
		8,
		nil,
		[]ReplicaInfo{{TimeUUID(), 9}},
		"",
	},
	{
		sync.RWMutex{},
		"test2",
		TimeUUID(),
		-4611686018427387905,
		"table1",
		8,
		nil,
		[]ReplicaInfo{{TimeUUID(), 8}},
		"",
	},
	{
		sync.RWMutex{},
		"test2",
		TimeUUID(),
		-2305843009213693953,
		"table1",
		8,
		nil,
		[]ReplicaInfo{{TimeUUID(), 9}},
		"",
	},
	{
		sync.RWMutex{},
		"test2",
		TimeUUID(),
		-1,
		"table1",
		8,
		nil,
		[]ReplicaInfo{{TimeUUID(), 8}},
		"",
	},
	{
		sync.RWMutex{},
		"test2",
		TimeUUID(),
		2305843009213693951,
		"table1",
		8,
		nil,
		[]ReplicaInfo{{TimeUUID(), 3}},
		"",
	},
	{
		sync.RWMutex{},
		"test2",
		TimeUUID(),
		4611686018427387903,
		"table1",
		8,
		nil,
		[]ReplicaInfo{{TimeUUID(), 3}},
		"",
	},
	{
		sync.RWMutex{},
		"test2",
		TimeUUID(),
		6917529027641081855,
		"table1",
		8,
		nil,
		[]ReplicaInfo{{TimeUUID(), 7}},
		"",
	},
	{
		sync.RWMutex{},
		"test2",
		TimeUUID(),
		9223372036854775807,
		"table1",
		8,
		nil,
		[]ReplicaInfo{{TimeUUID(), 7}},
		"",
	},
}

func TestFindTablets(t *testing.T) {
	id := findTablets(tablets, "test1", "table1")
	assertEqual(t, "id", 0, id)

	id = findTablets(tablets, "test2", "table1")
	assertEqual(t, "id", 8, id)

	id = findTablets(tablets, "test3", "table1")
	assertEqual(t, "id", -1, id)
}

func TestFindTabletForToken(t *testing.T) {
	tablet := findTabletForToken(tablets, parseInt64Token("0"), 0)
	assertTrue(t, "tablet.lastToken == 2305843009213693951", tablet.lastToken == 2305843009213693951)

	tablet = findTabletForToken(tablets, parseInt64Token("9223372036854775807"), 0)
	assertTrue(t, "tablet.lastToken == 9223372036854775807", tablet.lastToken == 9223372036854775807)

	tablet = findTabletForToken(tablets, parseInt64Token("-4611686018427387904"), 0)
	assertTrue(t, "tablet.lastToken == -2305843009213693953", tablet.lastToken == -2305843009213693953)
}
