package simplekv

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"unsafe"

	rbtree "github.com/pedrogao/RbTree"
)

// Tree LSM tree(og structure tree)
type Tree struct {
	appendLog   *AppendLog
	bloomFilter *BloomFilter
	segments    []string
	index       *rbtree.Tree // 磁盘文件稀疏索引
	memtable    *SizedMap

	threshold         int
	sparsityFactor    int
	segmentsDirectory string
	walBasename       string
	currentSegment    string
}

type indexItem struct {
	Segment string
	Offset  int64
	Val     any
}

// NewTree Initialize a new LSM tree
// - A first segment called segment_basename
// - A segments directory called segments_directory
// - A memtable write ahead log (WAL) called wal_basename
func NewTree(segmentBasename, segmentsDirectory,
	walBasename string) (*Tree, error) {
	// create lsm tree
	tree := &Tree{
		segments:          make([]string, 0),
		index:             rbtree.NewTree(),
		memtable:          NewSizedMap(),
		threshold:         1000000,
		sparsityFactor:    100,
		segmentsDirectory: segmentsDirectory,
		walBasename:       walBasename,
		currentSegment:    segmentBasename,
	}

	// create bloom filter
	bloomFilter := NewBloomFilter(100, 0.2)
	tree.bloomFilter = bloomFilter

	// create the segments directory
	if _, err := os.Stat(segmentsDirectory); err != nil && os.IsNotExist(err) {
		// directory not exist
		err = os.MkdirAll(segmentsDirectory, 0777)
		if err != nil {
			return nil, fmt.Errorf("make dir: %s err: %s", segmentsDirectory, err)
		}
	}

	// create write ahead log.
	appendLog, err := NewAppendLog(tree.memtableWalPath())
	if err != nil {
		return nil, fmt.Errorf("new wal: %s err: %s", tree.memtableWalPath(), err)
	}
	tree.appendLog = appendLog

	err = tree.loadMetadata()
	if err != nil {
		return nil, err
	}
	err = tree.restoreMemtable()
	if err != nil {
		return nil, err
	}
	return tree, nil
}

func (t *Tree) Set(key, value string) error {
	entry := t.toLogEntry(key, value)
	node := t.memtable.Get(key)
	if node != nil {
		if err := t.appendLog.WriteString(entry); err != nil {
			return err
		}
		t.memtable.Set(key, value)
		return nil
	}
	additionalSize := len(key) + len(value)
	if t.memtable.GetTotalSize()+additionalSize > t.threshold {
		err := t.compact()
		if err != nil {
			return fmt.Errorf("compact err: %s", err)
		}
		err = t.flushMemtableToDisk(t.currentSegmentPath())
		if err != nil {
			return fmt.Errorf("flushMemtableToDisk err: %s", err)
		}
		t.memtable = NewSizedMap()
		if err := t.appendLog.Clear(); err != nil {
			return err
		}
		t.segments = append(t.segments, t.currentSegment)
		t.currentSegment = t.incrementedSegmentName()
	}
	if err := t.appendLog.WriteString(entry); err != nil {
		return err
	}
	t.memtable.Set(key, value)
	return nil
}

func (t *Tree) Get(key string) (string, error) {
	if got := t.memtable.Get(key); got != nil {
		return got.(string), nil
	}

	if !t.bloomFilter.Check(key) {
		return "", nil
	}
	// 1. floor key => key1
	// 2. key1 => val1
	floorKey := t.index.FloorKey(keyType(key))
	if floorKey == nil {
		return t.searchAllSegments(key)
	}
	val := t.index.Find(floorKey)
	if val == nil {
		return t.searchAllSegments(key)
	}
	item := val.(*indexItem)
	segment := item.Segment
	offset := item.Offset
	path := t.segmentPath(segment)

	reader, err := NewLineReader(path, offset)
	if err != nil {
		return "", fmt.Errorf("can't open segment file: %s", err)
	}
	defer reader.Close()

	for {
		ikey, ival, err := reader.ReadLineKV()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return "", fmt.Errorf("read segment file err: %s", err)
		}
		if ikey == key {
			return ival, nil
		}
	}
	return t.searchAllSegments(key)
}

func (t *Tree) searchAllSegments(key string) (string, error) {
	// TODO 优化，缓存 segments 文件
	for _, segment := range t.segments {
		val, err := t.binarySearchSegment(key, segment)
		if err != nil {
			return "", err
		}
		if val != "" {
			return val, nil
		}
	}
	return "", nil
}

func (t *Tree) searchSegment(key, segment string) (string, error) {
	path := t.segmentPath(segment)
	val := ""
	err := t.iterLineOfSegmentFile(path, func(ikey, ival string) (bool, error) {
		if ikey == key {
			val = ival
			return true, nil
		}
		return false, nil
	})
	return val, err
}

// binarySearchSegment searchSegment 优化版
func (t *Tree) binarySearchSegment(key, segment string) (string, error) {
	// 一次性全部读出来然后二分，因为 segment 文件是有序的
	path := t.segmentPath(segment)
	file, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return "", fmt.Errorf("open file err: %s", err)
	}
	defer file.Close()

	allBytes, err := ioutil.ReadAll(file)
	if err != nil {
		return "", fmt.Errorf("read file err: %s", err)
	}
	// bytes to string without memory copy
	content := *(*string)(unsafe.Pointer(&allBytes))
	lines := strings.Split(content, "\n")
	if len(lines) == 0 {
		return "", nil
	}
	lines = lines[:len(lines)-1] // remove last ""

	for len(lines) > 0 {
		ptr := (len(lines) - 1) / 2
		parts := strings.Split(lines[ptr], ",")
		if len(parts) != 2 {
			return "", fmt.Errorf("segment file data format err, %v", parts)
		}
		if parts[0] == key {
			return parts[1], nil
		}

		if key < parts[0] {
			lines = lines[0:ptr]
		} else {
			lines = lines[ptr+1:]
		}
	}

	return "", nil
}

type iterFunc func(key, val string) (bool, error)

func (t *Tree) iterLineOfSegmentFile(path string, callback iterFunc) error {
	reader, err := NewLineReader(path, 0)
	if err != nil {
		return err
	}

	defer reader.Close()

	for {
		line, err := reader.ReadLine()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("read file err: %s", err)
		}
		parts := strings.Split(line, ",")
		if len(parts) != 2 {
			return fmt.Errorf("segment file data err: %v", parts)
		}
		done, err := callback(parts[0], parts[1])
		if err != nil {
			return err
		}
		if done {
			break
		}
	}

	return nil
}

func (t *Tree) compact() error {
	keysOnDisk := map[string]struct{}{}
	if !t.memtable.inner.Empty() {
		iter := t.memtable.inner.Iterator()
		for iter != nil {
			k := iter.Key.(keyType)
			if t.bloomFilter.Check(string(k)) {
				keysOnDisk[string(k)] = struct{}{}
			}
			iter = iter.Next()
		}
	}

	return t.deleteKeysFromSegments(keysOnDisk, t.segments)
}

func (t *Tree) deleteKeysFromSegments(deletionKeys map[string]struct{},
	segments []string) error {
	for _, segment := range segments {
		segmentPath := t.segmentPath(segment)
		err := t.deleteKeysFromSegment(deletionKeys, segmentPath)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *Tree) deleteKeysFromSegment(deletionKeys map[string]struct{},
	segmentPath string) error {
	tempPath := segmentPath + "_temp"
	output, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("open segment temp file err: %s", err)
	}

	err = t.iterLineOfSegmentFile(segmentPath, func(ikey, ival string) (bool, error) {
		_, ok := deletionKeys[ikey]
		if !ok {
			_, err = output.WriteString(t.toLogEntry(ikey, ival))
			if err != nil {
				return false, fmt.Errorf("write segment temp file err: %s", err)
			}
		}
		return false, nil
	})
	if err != nil {
		return err
	}

	err = output.Sync()
	if err != nil {
		return fmt.Errorf("flush file err: %s", err)
	}
	err = output.Close()
	if err != nil {
		return fmt.Errorf("close file err: %s", err)
	}

	err = os.Remove(segmentPath)
	if err != nil {
		return fmt.Errorf("remove segment file err: %s", err)
	}
	err = os.Rename(tempPath, segmentPath)
	if err != nil {
		return fmt.Errorf("rename segment file err: %s", err)
	}
	return nil
}

func (t *Tree) flushMemtableToDisk(path string) error {
	sparsityCounter := t.sparsity()
	var keyOffset int64 = 0
	file, err := os.Create(path) // 0666
	if err != nil {
		return fmt.Errorf("open file: %s err: %s", path, err)
	}
	if !t.memtable.inner.Empty() {
		iter := t.memtable.inner.Iterator()
		for iter != nil {
			k := iter.Key.(keyType)
			v := iter.Value.(string)
			entry := t.toLogEntry(string(k), v)
			if sparsityCounter == 1 {
				if n := t.index.Find(k); n != nil {
					pre := n.(*indexItem)
					t.index.Insert(k, &indexItem{
						Segment: pre.Segment,
						Offset:  pre.Offset,
						Val:     v,
					})
				} else {
					t.index.Insert(k, &indexItem{
						Segment: t.currentSegment,
						Offset:  keyOffset,
						Val:     v,
					})
				}
				sparsityCounter = t.sparsity() + 1
			}
			t.bloomFilter.Add(string(k))
			_, err := file.WriteString(entry)
			if err != nil {
				return fmt.Errorf("write %s err: %s", path, err)
			}
			keyOffset += int64(len(entry))
			sparsityCounter -= 1

			iter = iter.Next()
		}
	}

	err = file.Sync()
	if err != nil {
		return fmt.Errorf("flush file err: %s", err)
	}
	err = file.Close()
	if err != nil {
		return fmt.Errorf("close file err: %s", err)
	}
	return nil
}

func (t *Tree) loadMetadata() error {
	path := t.metadataPath()
	if _, err := os.Stat(path); err != nil && os.IsNotExist(err) {
		return nil
	}
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read meta data err: %s", err)
	}
	meta := &treeMetadata{}
	err = meta.load(bytes)

	t.index = rbtree.NewTree()
	for k, v := range meta.Index {
		t.index.Insert(keyType(k), v)
	}

	t.bloomFilter = &BloomFilter{}
	err = t.bloomFilter.UnPack(meta.BloomFilter)
	if err != nil {
		return fmt.Errorf("bloom unpack err: %s", err)
	}

	t.segments = meta.Segments
	t.currentSegment = meta.CurrentSegment

	return nil
}

func (t *Tree) saveMetadata() error {
	indexMap := map[string]*indexItem{}
	if !t.index.Empty() {
		iter := t.index.Iterator()
		for iter != nil {
			k := iter.Key.(keyType)
			indexMap[string(k)] = iter.Value.(*indexItem)
			iter = iter.Next()
		}
	}
	bloom := t.bloomFilter.Pack()
	m := &treeMetadata{
		Segments:       t.segments,
		CurrentSegment: t.currentSegment,
		Index:          indexMap,
		BloomFilter:    bloom,
	}
	bytes, err := m.dump()
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(t.metadataPath(), bytes, 0666)
	if err != nil {
		return fmt.Errorf("write file err: %s", err)
	}
	return nil
}

func (t *Tree) restoreMemtable() error {
	path := t.memtableWalPath()
	if _, err := os.Stat(path); err != nil && os.IsNotExist(err) {
		return nil
	}

	err := t.iterLineOfSegmentFile(path, func(key, val string) (bool, error) {
		t.memtable.Set(key, val)
		return false, nil
	})

	return err
}

func (t *Tree) merge(segment1, segment2 string) error {
	path1 := t.segmentsDirectory + segment1
	path2 := t.segmentsDirectory + segment2
	newPath := t.segmentsDirectory + "temp"
	var (
		line1, line2 string
		err          error
	)
	writer, err := os.OpenFile(newPath, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return fmt.Errorf("open file err: %s", err)
	}

	reader1, err := NewLineReader(path1, 0)
	if err != nil {
		return err
	}
	reader2, err := NewLineReader(path2, 0)
	if err != nil {
		return err
	}
	line1, err = reader1.ReadLine()
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	line2, err = reader2.ReadLine()
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	for {
		if line1 == "" && line2 == "" {
			break
		}
		parts1 := strings.Split(line1, ",")
		parts2 := strings.Split(line2, ",")
		key1 := parts1[0]
		key2 := parts2[0]
		if key1 == "" || key1 == key2 {
			_, err = writer.WriteString(line2 + "\n")
			if err != nil {
				return fmt.Errorf("write file err: %s", err)
			}
			line1, err = reader1.ReadLine()
			if err != nil && !errors.Is(err, io.EOF) {
				return err
			}
			line2, err = reader2.ReadLine()
			if err != nil && !errors.Is(err, io.EOF) {
				return err
			}
		} else if key2 == "" || key1 < key2 {
			_, err = writer.WriteString(line1 + "\n")
			if err != nil && !errors.Is(err, io.EOF) {
				return fmt.Errorf("write file err: %s", err)
			}
			line1, err = reader1.ReadLine()
			if err != nil && !errors.Is(err, io.EOF) {
				return err
			}
		} else {
			_, err = writer.WriteString(line2 + "\n")
			if err != nil {
				return fmt.Errorf("write file err: %s", err)
			}
			line2, err = reader2.ReadLine()
			if err != nil && !errors.Is(err, io.EOF) {
				return err
			}
		}
	}

	err = writer.Sync()
	if err != nil {
		return fmt.Errorf("flush file err: %s", err)
	}
	err = writer.Close()
	if err != nil {
		return fmt.Errorf("close file err: %s", err)
	}
	err = reader1.Close()
	if err != nil {
		return fmt.Errorf("close file err: %s", err)
	}
	err = reader2.Close()
	if err != nil {
		return fmt.Errorf("close file err: %s", err)
	}

	err = os.Remove(path1)
	if err != nil {
		return fmt.Errorf("remove file err: %s", err)
	}
	err = os.Remove(path2)
	if err != nil {
		return fmt.Errorf("remove file err: %s", err)
	}
	err = os.Rename(newPath, path1)
	if err != nil {
		return fmt.Errorf("rename file err: %s", err)
	}
	return nil
}

func (t *Tree) repopulateIndex() error {
	t.index = rbtree.NewTree()
	for _, segment := range t.segments {
		path := t.segmentPath(segment)
		counter := t.sparsity()
		bytes := 0

		err := t.iterLineOfSegmentFile(path, func(key, val string) (bool, error) {
			if counter == 1 {
				t.index.Insert(keyType(key), &indexItem{
					Segment: segment,
					Offset:  int64(bytes),
					Val:     val,
				})
				counter = t.sparsity() + 1
			}
			bytes += sizeof(t.toLogEntry(key, val))
			counter -= 1
			return false, nil
		})

		if err != nil {
			return err
		}
	}
	return nil
}

func (t *Tree) incrementedSegmentName() string {
	parts := strings.Split(t.currentSegment, "-")
	if len(parts) != 2 {
		panic("segment name not valid")
	}
	num, err := strconv.Atoi(parts[1])
	if err != nil {
		panic("segment name not valid")
	}
	return fmt.Sprintf("%s-%d", parts[0], num+1)
}

func (t *Tree) setThreshold(threshold int) {
	t.threshold = threshold
}

func (t *Tree) setSparsityFactor(factor int) {
	t.sparsityFactor = factor
}

func (t *Tree) setBloomFilterNumItems(numItems int) {
	t.bloomFilter = NewBloomFilter(numItems, t.bloomFilter.falsePositivePob)
}

func (t *Tree) setBloomFilterFalsePosProb(probability float64) {
	t.bloomFilter = NewBloomFilter(t.bloomFilter.numItems, probability)
}

func (t *Tree) sparsity() int {
	return t.threshold / t.sparsityFactor
}

func (t *Tree) toLogEntry(key, value string) string {
	return key + "," + value + "\n"
}

// Returns the path to the memtable write ahead log.
func (t *Tree) memtableWalPath() string {
	return t.segmentsDirectory + t.walBasename
}

// Returns the path to the memtable write ahead log.
func (t *Tree) currentSegmentPath() string {
	return t.segmentsDirectory + t.currentSegment
}

// Returns the path to the given segment_name.
func (t *Tree) segmentPath(segmentName string) string {
	return t.segmentsDirectory + segmentName
}

// Returns the path to the treeMetadata backup file.
func (t *Tree) metadataPath() string {
	return t.segmentsDirectory + "database_metadata"
}
