//go:build unit
// +build unit

package gocql

import (
	"testing"
)

var tablets = TabletInfoList{
	{
		"test1",
		"table1",
		-7917529027641081857,
		-6917529027641081857,
		[]ReplicaInfo{{TimeUUID(), 9}},
	},
	{
		"test1",
		"table1",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{{TimeUUID(), 8}},
	},
	{
		"test1",
		"table1",
		-4611686018427387905,
		-2305843009213693953,
		[]ReplicaInfo{{TimeUUID(), 9}},
	},
	{
		"test1",
		"table1",
		-2305843009213693953,
		-1,
		[]ReplicaInfo{{TimeUUID(), 8}},
	},
	{
		"test1",
		"table1",
		-1,
		2305843009213693951,
		[]ReplicaInfo{{TimeUUID(), 3}},
	},
	{
		"test1",
		"table1",
		2305843009213693951,
		4611686018427387903,
		[]ReplicaInfo{{TimeUUID(), 3}},
	},
	{
		"test1",
		"table1",
		4611686018427387903,
		6917529027641081855,
		[]ReplicaInfo{{TimeUUID(), 7}},
	},
	{
		"test1",
		"table1",
		6917529027641081855,
		9223372036854775807,
		[]ReplicaInfo{{TimeUUID(), 7}},
	},
	{
		"test2",
		"table1",
		-7917529027641081857,
		-6917529027641081857,
		[]ReplicaInfo{{TimeUUID(), 9}},
	},
	{
		"test2",
		"table1",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{{TimeUUID(), 8}},
	},
	{
		"test2",
		"table1",
		-4611686018427387905,
		-2305843009213693953,
		[]ReplicaInfo{{TimeUUID(), 9}},
	},
	{
		"test2",
		"table1",
		-2305843009213693953,
		-1,
		[]ReplicaInfo{{TimeUUID(), 8}},
	},
	{
		"test2",
		"table1",
		-1,
		2305843009213693951,
		[]ReplicaInfo{{TimeUUID(), 3}},
	},
	{
		"test2",
		"table1",
		2305843009213693951,
		4611686018427387903,
		[]ReplicaInfo{{TimeUUID(), 3}},
	},
	{
		"test2",
		"table1",
		4611686018427387903,
		6917529027641081855,
		[]ReplicaInfo{{TimeUUID(), 7}},
	},
	{
		"test2",
		"table1",
		6917529027641081855,
		9223372036854775807,
		[]ReplicaInfo{{TimeUUID(), 7}},
	},
}

func TestFindTablets(t *testing.T) {
	id, id2 := tablets.findTablets("test1", "table1")
	assertEqual(t, "id", 0, id)
	assertEqual(t, "id2", 7, id2)

	id, id2 = tablets.findTablets("test2", "table1")
	assertEqual(t, "id", 8, id)
	assertEqual(t, "id2", 15, id2)

	id, id2 = tablets.findTablets("test3", "table1")
	assertEqual(t, "id", -1, id)
	assertEqual(t, "id2", -1, id2)
}

func TestFindTabletForToken(t *testing.T) {
	tablet := tablets.findTabletForToken(parseInt64Token("0"), 0, 7)
	assertTrue(t, "tablet.lastToken == 2305843009213693951", tablet.lastToken == 2305843009213693951)

	tablet = tablets.findTabletForToken(parseInt64Token("9223372036854775807"), 0, 7)
	assertTrue(t, "tablet.lastToken == 9223372036854775807", tablet.lastToken == 9223372036854775807)

	tablet = tablets.findTabletForToken(parseInt64Token("-4611686018427387904"), 0, 7)
	assertTrue(t, "tablet.lastToken == -2305843009213693953", tablet.lastToken == -2305843009213693953)
}

func CompareRanges(tablets TabletInfoList, ranges [][]int64) bool {
	if len(tablets) != len(ranges) {
		return false
	}

	for idx, tablet := range tablets {
		if tablet.FirstToken() != ranges[idx][0] || tablet.LastToken() != ranges[idx][1] {
			return false
		}
	}
	return true
}
func TestAddTabletToEmptyTablets(t *testing.T) {
	tablets := TabletInfoList{}

	tablets = tablets.addTabletToTabletsList(&TabletInfo{
		"test_ks",
		"test_tb",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{},
	})

	assertTrue(t, "Token range in tablets table not correct", CompareRanges(tablets, [][]int64{{-6917529027641081857, -4611686018427387905}}))
}

func TestAddTabletAtTheBeggining(t *testing.T) {
	tablets := TabletInfoList{{
		"test_ks",
		"test_tb",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{},
	}}

	tablets = tablets.addTabletToTabletsList(&TabletInfo{
		"test_ks",
		"test_tb",
		-8611686018427387905,
		-7917529027641081857,
		[]ReplicaInfo{},
	})

	assertTrue(t, "Token range in tablets table not correct",
		CompareRanges(tablets, [][]int64{{-8611686018427387905, -7917529027641081857}, {-6917529027641081857, -4611686018427387905}}))
}

func TestAddTabletAtTheEnd(t *testing.T) {
	tablets := TabletInfoList{{
		"test_ks",
		"test_tb",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{},
	}}

	tablets = tablets.addTabletToTabletsList(&TabletInfo{
		"test_ks",
		"test_tb",
		-1,
		2305843009213693951,
		[]ReplicaInfo{},
	})

	assertTrue(t, "Token range in tablets table not correct", CompareRanges(tablets, [][]int64{{-6917529027641081857, -4611686018427387905},
		{-1, 2305843009213693951}}))
}

func TestAddTabletInTheMiddle(t *testing.T) {
	tablets := TabletInfoList{{
		"test_ks",
		"test_tb",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{},
	}, {
		"test_ks",
		"test_tb",
		-1,
		2305843009213693951,
		[]ReplicaInfo{},
	}}

	tablets = tablets.addTabletToTabletsList(&TabletInfo{
		"test_ks",
		"test_tb",
		-4611686018427387905,
		-2305843009213693953,
		[]ReplicaInfo{},
	})

	assertTrue(t, "Token range in tablets table not correct", CompareRanges(tablets, [][]int64{{-6917529027641081857, -4611686018427387905},
		{-4611686018427387905, -2305843009213693953},
		{-1, 2305843009213693951}}))
}

func TestAddTabletIntersecting(t *testing.T) {
	tablets := TabletInfoList{{
		"test_ks",
		"test_tb",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{},
	}, {
		"test_ks",
		"test_tb",
		-4611686018427387905,
		-2305843009213693953,
		[]ReplicaInfo{},
	}, {
		"test_ks",
		"test_tb",
		-2305843009213693953,
		-1,
		[]ReplicaInfo{},
	}, {
		"test_ks",
		"test_tb",
		-1,
		2305843009213693951,
		[]ReplicaInfo{},
	}}

	tablets = tablets.addTabletToTabletsList(&TabletInfo{
		"test_ks",
		"test_tb",
		-3611686018427387905,
		-6,
		[]ReplicaInfo{},
	})

	assertTrue(t, "Token range in tablets table not correct",
		CompareRanges(tablets, [][]int64{{-6917529027641081857, -4611686018427387905},
			{-3611686018427387905, -6},
			{-1, 2305843009213693951}}))
}

func TestAddTabletIntersectingWithFirst(t *testing.T) {
	tablets := TabletInfoList{{
		"test_ks",
		"test_tb",
		-8611686018427387905,
		-7917529027641081857,
		[]ReplicaInfo{},
	}, {
		"test_ks",
		"test_tb",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{},
	}}

	tablets = tablets.addTabletToTabletsList(&TabletInfo{
		"test_ks",
		"test_tb",
		-8011686018427387905,
		-7987529027641081857,
		[]ReplicaInfo{},
	})

	assertTrue(t, "Token range in tablets table not correct", CompareRanges(tablets, [][]int64{{-8011686018427387905, -7987529027641081857},
		{-6917529027641081857, -4611686018427387905}}))
}

func TestAddTabletIntersectingWithLast(t *testing.T) {
	tablets := TabletInfoList{{
		"test_ks",
		"test_tb",
		-8611686018427387905,
		-7917529027641081857,
		[]ReplicaInfo{},
	}, {
		"test_ks",
		"test_tb",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{},
	}}

	tablets = tablets.addTabletToTabletsList(&TabletInfo{
		"test_ks",
		"test_tb",
		-5011686018427387905,
		-2987529027641081857,
		[]ReplicaInfo{},
	})

	assertTrue(t, "Token range in tablets table not correct", CompareRanges(tablets, [][]int64{{-8611686018427387905, -7917529027641081857},
		{-5011686018427387905, -2987529027641081857}}))
}

func TestRemoveTabletsWithHost(t *testing.T) {
	removed_host_id := TimeUUID()

	tablets := TabletInfoList{{
		"test_ks",
		"test_tb",
		-8611686018427387905,
		-7917529027641081857,
		[]ReplicaInfo{{TimeUUID(), 9}, {TimeUUID(), 8}, {TimeUUID(), 3}},
	}, {
		"test_ks",
		"test_tb",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{{removed_host_id, 9}, {TimeUUID(), 8}, {TimeUUID(), 3}},
	}, {
		"test_ks",
		"test_tb",
		-4611686018427387905,
		-2305843009213693953,
		[]ReplicaInfo{{TimeUUID(), 9}, {removed_host_id, 8}, {TimeUUID(), 3}},
	}}

	tablets = tablets.removeTabletsWithHostFromTabletsList(&HostInfo{
		hostId: removed_host_id.String(),
	})

	assertEqual(t, "TabletsList length", 1, len(tablets))
}

func TestRemoveTabletsWithKeyspace(t *testing.T) {
	tablets := TabletInfoList{{
		"removed_ks",
		"test_tb",
		-8611686018427387905,
		-7917529027641081857,
		[]ReplicaInfo{{TimeUUID(), 9}, {TimeUUID(), 8}, {TimeUUID(), 3}},
	}, {
		"removed_ks",
		"test_tb",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{{TimeUUID(), 9}, {TimeUUID(), 8}, {TimeUUID(), 3}},
	}, {
		"test_ks",
		"test_tb",
		-4611686018427387905,
		-2305843009213693953,
		[]ReplicaInfo{{TimeUUID(), 9}, {TimeUUID(), 8}, {TimeUUID(), 3}},
	}}

	tablets = tablets.removeTabletsWithKeyspaceFromTabletsList("removed_ks")

	assertEqual(t, "TabletsList length", 1, len(tablets))
}

func TestRemoveTabletsWithTable(t *testing.T) {
	tablets := TabletInfoList{{
		"test_ks",
		"test_tb",
		-8611686018427387905,
		-7917529027641081857,
		[]ReplicaInfo{{TimeUUID(), 9}, {TimeUUID(), 8}, {TimeUUID(), 3}},
	}, {
		"test_ks",
		"test_tb",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{{TimeUUID(), 9}, {TimeUUID(), 8}, {TimeUUID(), 3}},
	}, {
		"test_ks",
		"removed_tb",
		-4611686018427387905,
		-2305843009213693953,
		[]ReplicaInfo{{TimeUUID(), 9}, {TimeUUID(), 8}, {TimeUUID(), 3}},
	}}

	tablets = tablets.removeTabletsWithTableFromTabletsList("test_ks", "removed_tb")

	assertEqual(t, "TabletsList length", 2, len(tablets))
}
