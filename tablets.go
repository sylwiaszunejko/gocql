package gocql

import (
	"sync"
	"sync/atomic"
)

// cowTabletList implements a copy on write tablet list, its equivalent type is TabletInfoList
type cowTabletList struct {
	list atomic.Value
	mu   sync.Mutex
}

func (c *cowTabletList) get() TabletInfoList {
	l, ok := c.list.Load().(TabletInfoList)
	if !ok {
		return nil
	}
	return l
}

func (c *cowTabletList) set(tablets TabletInfoList) {
	c.mu.Lock()
	defer c.mu.Unlock()

	n := len(tablets)
	t := make(TabletInfoList, n)
	for i := 0; i < n; i++ {
		t[i] = tablets[i]
	}

	c.list.Store(t)
}
