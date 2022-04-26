package simplekv

import (
	"fmt"

	jsoniter "github.com/json-iterator/go"
)

type bloomMetadata struct {
	FalsePositivePob        float64
	BitArraySize, HashCount int
	NumItems                int
	Bit                     []uint
}

// treeMetadata 元数据
type treeMetadata struct {
	Segments       []string
	CurrentSegment string
	Index          map[string]*indexItem
	BloomFilter    string
}

func (m *treeMetadata) load(bytes []byte) error {
	err := jsoniter.Unmarshal(bytes, m)
	if err != nil {
		return fmt.Errorf("json unmarshal err: %s", err)
	}
	return nil
}

func (m *treeMetadata) dump() ([]byte, error) {
	bytes, err := jsoniter.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("json marshal err: %s", err)
	}
	return bytes, nil
}
