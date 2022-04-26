package simplekv

import (
	rbtree "github.com/pedrogao/RbTree"
)

// SizedMap map with size
type SizedMap struct {
	inner     *rbtree.Tree // 内部索引
	totalSize int
}

// NewSizedMap new sized map
func NewSizedMap() *SizedMap {
	return &SizedMap{
		inner:     rbtree.NewTree(),
		totalSize: 0,
	}
}

// Get k
func (m *SizedMap) Get(key string) interface{} {
	return m.inner.Find(keyType(key))
}

// Set k->v
func (m *SizedMap) Set(key string, v interface{}) {
	old := m.Get(key)
	if old != nil {
		m.totalSize -= len(key) + sizeof(old)
	}
	size := len(key) + sizeof(v)
	m.totalSize += size
	m.inner.Insert(keyType(key), v)
}

// Contains k
func (m *SizedMap) Contains(key string) bool {
	return m.inner.Contains(keyType(key))
}

// GetTotalSize total size of kvs
func (m *SizedMap) GetTotalSize() int {
	return m.totalSize
}
