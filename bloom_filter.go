package simplekv

import (
	"fmt"
	"math"

	jsoniter "github.com/json-iterator/go"
)

// For an explanation of the math, visit
// https://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives.
// A condensed explanation can be found here:
// https://stackoverflow.com/questions/658439/how-many-hash-functions-does-my-bloom-filter-need

// BloomFilter 布隆过滤器
type BloomFilter struct {
	falsePositivePob        float64
	bitArraySize, hashCount int
	numItems                int
	bit                     *BitArray
}

// NewBloomFilter 新建布隆过滤器
func NewBloomFilter(numItems int, falsePositivePob float64) *BloomFilter {
	bf := &BloomFilter{
		falsePositivePob: falsePositivePob,
		numItems:         numItems,
		bit:              NewBitArray(numItems),
	}
	bitArraySize := bf.calculateBitArraySize(numItems, falsePositivePob)
	hashCount := bf.calculateHashCount(bitArraySize, numItems)
	bf.hashCount = hashCount
	bf.bitArraySize = bitArraySize
	return bf
}

func (f *BloomFilter) Add(item string) {
	data := []byte(item)
	for i := 0; i < f.hashCount; i++ {
		digest := int(Murmur332(data, uint32(i))) % f.bitArraySize
		f.bit.Add(digest)
	}
}

func (f *BloomFilter) Check(item string) bool {
	data := []byte(item)
	for i := 0; i < f.hashCount; i++ {
		digest := int(Murmur332(data, uint32(i))) % f.bitArraySize
		if !f.bit.Has(digest) {
			return false
		}
	}
	return true
}

func (f *BloomFilter) calculateBitArraySize(numItems int,
	probability float64) int {
	// m = -(n * lg(p)) / (lg(2)^2)
	m := -(float64(numItems) * math.Log(probability) / (math.Pow(math.Log(2), 2)))
	return int(m)
}

func (f *BloomFilter) calculateHashCount(bitArraySize, numItems int) int {
	// k = (m/n) * lg(2)
	k := (float64(bitArraySize) / float64(numItems)) * math.Log(2)
	return int(k)
}

func (f *BloomFilter) Pack() string {
	str, err := jsoniter.MarshalToString(&bloomMetadata{
		FalsePositivePob: f.falsePositivePob,
		BitArraySize:     f.bitArraySize,
		HashCount:        f.hashCount,
		Bit:              f.bit.data,
		NumItems:         f.numItems,
	})
	if err != nil {
		panic(err)
	}

	return str
}

func (f *BloomFilter) UnPack(data string) error {
	meta := &bloomMetadata{}
	var err error
	err = jsoniter.UnmarshalFromString(data, &meta)
	if err != nil {
		return fmt.Errorf("unmarshal bloom filter err: %s", err)
	}
	f.falsePositivePob = meta.FalsePositivePob
	f.bitArraySize = meta.BitArraySize
	f.hashCount = meta.HashCount
	f.numItems = meta.NumItems
	bit := &BitArray{}
	bit.data = meta.Bit
	f.bit = bit

	return nil
}
