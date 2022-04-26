package simplekv

import (
	"bytes"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	testFilename = "test_file-1"
	testBasePath = "test-segments/"
	bkupName     = "test_backup"
	testPath     = testBasePath + testFilename
)

func TestTreeOps(t *testing.T) {
	assert := assert.New(t)
	tree, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)

	n := 100000 // 0.61s
	//tree.threshold = 20
	// n := 200000 // 584.90s

	t.Logf("set begin\n")
	for i := 0; i < n; i++ {
		err := tree.Set(strconv.Itoa(i), strconv.Itoa(i))
		assert.Nil(err)
	}

	t.Logf("get begin\n")

	for i := 0; i < n; i++ {
		val, err := tree.Get(strconv.Itoa(i))
		assert.Nil(err)
		if i%1000 == 0 {
			t.Logf("%d iter, val: %s\n", i, val)
		}
		assert.Equal(val, strconv.Itoa(i))
	}
}

func cleanup() {
	err := os.RemoveAll(testBasePath)
	if err != nil {
		panic(err)
	}
}

func Test_Set_stores_pair_in_memtable(t *testing.T) {
	assert := assert.New(t)
	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)
	err = db.Set("1", "test1")
	assert.Nil(err)
	err = db.Set("2", "test2")
	assert.Nil(err)

	node1 := db.memtable.Get("1")
	node2 := db.memtable.Get("2")

	assert.Equal(node1, "test1")
	assert.Equal(node2, "test2")
}

func Test_Set_flushes_to_disk_past_threshold(t *testing.T) {
	assert := assert.New(t)
	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)
	db.threshold = 10

	err = db.Set("1", "test1")
	assert.Nil(err)
	err = db.Set("2", "test2")
	assert.Nil(err)
	err = db.Set("3", "cl")
	assert.Nil(err)

	bytes, err := ioutil.ReadFile(testPath)
	assert.Nil(err)

	lines := strings.Split(string(bytes), "\n")

	assert.Equal(len(lines), 2)
	assert.Equal(strings.TrimSpace(lines[0]), "1,test1")

	node2 := db.memtable.Get("2")
	assert.Equal(node2, "test2")
}

func Test_Set_writes_to_wal(t *testing.T) {
	assert := assert.New(t)
	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)
	err = db.appendLog.Clear()
	assert.Nil(err)

	file, err := os.Open(testBasePath + bkupName)
	assert.Nil(err)
	file.Close()

	err = db.Set("chris", "lessard")
	assert.Nil(err)
	err = db.Set("daniel", "lessard")
	assert.Nil(err)

	bytes, err := ioutil.ReadFile(testBasePath + bkupName)
	assert.Nil(err)

	lines := strings.Split(string(bytes), "\n")

	assert.Equal(len(lines), 3)
}

func Test_Set_key_update_increment_memtable_total_bytes(t *testing.T) {
	assert := assert.New(t)
	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)

	db.Set("mr", "bean")
	assert.Equal(db.memtable.GetTotalSize(), 6)
	db.Set("mr", "toast")
	assert.Equal(db.memtable.GetTotalSize(), 7)
}

func Test_memtable_in_order_traversal(t *testing.T) {
	assert := assert.New(t)
	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)
	db.memtable.Set("chris", "lessard")
	db.memtable.Set("daniel", "lessard")
	db.memtable.Set("debra", "brown")
	db.memtable.Set("antony", "merchy")

	nodes := db.memtable.inner

	assert.Equal(nodes.Size(), 4)
}

func Test_flush_memtable_to_disk_flushes_memtable_to_disk(t *testing.T) {
	assert := assert.New(t)
	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)
	db.memtable.Set("chris", "lessard")
	db.memtable.Set("daniel", "lessard")
	err = db.flushMemtableToDisk(testPath)
	assert.Nil(err)

	lines := readFileLines(testPath)
	expectedLines := []string{"chris,lessard\n", "daniel,lessard\n"}
	assert.Equal(lines, expectedLines)
}

func Test_Get_does_single_val_retrieval(t *testing.T) {
	assert := assert.New(t)
	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)
	db.Set("chris", "lessard")
	val, err := db.Get("chris")
	assert.Nil(err)
	assert.Equal(val, "lessard")
}

func Test_Get_gets_when_threshold_is_low(t *testing.T) {
	assert := assert.New(t)
	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)
	pairs := [][]string{
		{"chris", "lessard"},
		{"daniel", "lessard"},
		{"charles", "lessard"},
		{"adrian", "lessard"},
	}
	db.threshold = 20

	for _, pair := range pairs {
		db.Set(pair[0], pair[1])
	}
	val, err := db.Get("chris")
	assert.Nil(err)
	assert.Equal(val, "lessard")
	val, err = db.Get("chris")
	assert.Nil(err)
	assert.Equal(val, "lessard")
	val, err = db.Get("chris")
	assert.Nil(err)
	assert.Equal(val, "lessard")
	val, err = db.Get("chris")
	assert.Nil(err)
	assert.Equal(val, "lessard")
}

func Test_Get_handles_miss(t *testing.T) {
	assert := assert.New(t)
	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)
	db.threshold = 20
	db.Set("chris", "lessard")
	db.Set("daniel", "lessard")
	db.Set("charles", "lessard")
	db.Set("adrian", "lessard")

	val, err := db.Get("debra")
	assert.Nil(err)
	assert.Equal(val, "")
}

func Test_Get_retrieves_most_recent_val(t *testing.T) {
	assert := assert.New(t)
	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)
	pairs := [][]string{
		{"chris", "lessard"},
		{"chris", "martinez"},
	}

	for _, pair := range pairs {
		db.Set(pair[0], pair[1])
	}
	val, err := db.Get("chris")
	assert.Nil(err)
	assert.Equal(val, "martinez")
}

func Test_db_get_retrieves_value_when_multiple_segments_exist(t *testing.T) {
	assert := assert.New(t)
	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)
	db.threshold = 10

	db.Set("chris", "lessard")
	db.Set("daniel", "lessard")

	db.Set("chris", "martinez")
	db.Set("a", "b")
	db.Set("a", "c")

	val, err := db.Get("chris")
	assert.Nil(err)
	assert.Equal(val, "martinez")
	val, err = db.Get("daniel")
	assert.Nil(err)
	assert.Equal(val, "lessard")
	val, err = db.Get("a")
	assert.Nil(err)
	assert.Equal(val, "c")

}

func Test_segment_path_gets_segment_path(t *testing.T) {
	assert := assert.New(t)
	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)
	assert.Equal(db.segmentPath("segment1"),
		testBasePath+"segment1")
	assert.Equal(db.segmentPath("segment5"),
		testBasePath+"segment5")
}

func Test_Set_uses_new_segments(t *testing.T) {
	assert := assert.New(t)
	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)
	db.threshold = 10
	db.Set("abc", "cba")
	db.Set("def", "fed")

	assert.Equal(db.memtable.GetTotalSize(), 6)
	assert.Equal(db.currentSegment, "test_file-2")
}

func Test_search_segment_key_present1(t *testing.T) {
	assert := assert.New(t)
	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)
	pairs := [][]string{
		{"chris", "lessard"},
		{"daniel", "lessard"},
		{"charles", "lessard"},
		{"adrian", "lessard"},
	}
	file, err := os.Create(testBasePath + testFilename)
	assert.Nil(err)
	for _, pair := range pairs {
		file.WriteString(pair[0] + "," + pair[1] + "\n")
	}
	db.segments = []string{testFilename}
	val, err := db.searchSegment("daniel", testFilename)
	assert.Nil(err)
	assert.Equal(val, "lessard")
}

func Test_binary_search_segment_key_present1(t *testing.T) {
	assert := assert.New(t)
	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)
	pairs := [][]string{
		{"chris", "lessard"},
		{"daniel", "lessard"},
		{"charles", "lessard"},
		{"adrian", "lessard"},
	}
	file, err := os.Create(testBasePath + testFilename)
	assert.Nil(err)
	for _, pair := range pairs {
		file.WriteString(pair[0] + "," + pair[1] + "\n")
	}
	db.segments = []string{testFilename}
	val, err := db.binarySearchSegment("daniel", testFilename)
	assert.Nil(err)
	assert.Equal(val, "lessard")
}

func Test_search_segment_key_present2(t *testing.T) {
	assert := assert.New(t)
	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)

	pairs := [][]string{
		{"chris", "lessard"},
		{"daniel", "lessard"},
		{"charles", "lessard"},
		{"adrian", "lessard"},
	}
	file, err := os.Create(testBasePath + testFilename)
	assert.Nil(err)
	for _, pair := range pairs {
		file.WriteString(pair[0] + "," + pair[1] + "\n")
	}

	db.segments = []string{testFilename}
	val, err := db.searchSegment("steve", testFilename)
	assert.Nil(err)
	assert.Equal(val, "")
}

func Test_binary_search_segment_key_present2(t *testing.T) {
	assert := assert.New(t)
	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)

	pairs := [][]string{
		{"chris", "lessard"},
		{"daniel", "lessard"},
		{"charles", "lessard"},
		{"adrian", "lessard"},
	}
	file, err := os.Create(testBasePath + testFilename)
	assert.Nil(err)
	for _, pair := range pairs {
		file.WriteString(pair[0] + "," + pair[1] + "\n")
	}

	db.segments = []string{testFilename}
	val, err := db.binarySearchSegment("steve", testFilename)
	assert.Nil(err)
	assert.Equal(val, "")
}

func Test_merge_merges_two_segments(t *testing.T) {
	assert := assert.New(t)
	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)

	segments := []string{"test_file-1", "test_file-2"}
	s, err := os.Create(testBasePath + segments[0])
	assert.Nil(err)

	s.WriteString("1,test1\n")
	s.WriteString("2,test2\n")
	s.WriteString("4,test6\n")

	s, err = os.Create(testBasePath + segments[1])
	assert.Nil(err)

	s.WriteString("1,test5\n")
	s.WriteString("2,test6\n")
	s.WriteString("3,test5\n")

	db.segments = segments

	err = db.merge(segments[0], segments[1])
	assert.Nil(err)

	segmentLines := readFileLines(testBasePath + "test_file-1")
	expectedContents := []string{"1,test5\n", "2,test6\n", "3,test5\n", "4,test6\n"}
	assert.Equal(segmentLines, expectedContents)

	assert.Equal(exists(testFilename+segments[1]), false)
}

func Test_save_metadata_saves_metadata(t *testing.T) {
	assert := assert.New(t)
	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)
	segments := []string{"segment-1", "segment-2", "segment-3"}
	db.segments = segments

	db.Set("chris", "lessard")
	db.Set("daniel", "lessard")
	db.bloomFilter.falsePositivePob = 0.5
	db.bloomFilter.numItems = 100
	err = db.saveMetadata()
	assert.Nil(err)

	bytes, err := ioutil.ReadFile(db.segmentsDirectory + "database_metadata")
	assert.Nil(err)
	meta := &treeMetadata{}
	err = meta.load(bytes)
	assert.Nil(err)

	assert.Equal(meta.CurrentSegment, testFilename)
	assert.Equal(meta.Segments, segments)

	b := &BloomFilter{}
	err = b.UnPack(meta.BloomFilter)
	assert.Nil(err)

	assert.Equal(b.falsePositivePob, 0.5)
	assert.Equal(b.numItems, 100)
	assert.NotNil(meta.Index)
}

func Test_load_metadata_loads_segments_at_init_time(t *testing.T) {
	assert := assert.New(t)
	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)
	segments := []string{"segment-1", "segment-2", "segment-3"}
	db.segments = segments
	db.currentSegment = segments[len(segments)-1]

	db.Set("chris", "lessard")
	db.Set("daniel", "lessard")
	db.bloomFilter.falsePositivePob = 0.5
	db.bloomFilter.numItems = 100
	db.index.Insert(keyType("john"), &indexItem{
		Segment: "segment-1",
		Offset:  5,
		Val:     nil,
	})

	err = db.saveMetadata()
	assert.Nil(err)

	db, err = NewTree(testFilename, testBasePath, bkupName)
	assert.Nil(err)
	err = db.loadMetadata()
	assert.Nil(err)

	assert.Equal(db.segments, segments)
	assert.Equal(db.currentSegment, segments[len(segments)-1])
	assert.Equal(db.bloomFilter.falsePositivePob, 0.5)
	assert.Equal(db.bloomFilter.numItems, 100)
	assert.True(db.index.Contains(keyType("john")))
}

func Test_restore_memtable_loads_memtable_from_wal(t *testing.T) {
	assert := assert.New(t)
	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)

	err = db.appendLog.Clear()
	assert.Nil(err)
	db.Set("sad", "mad")
	db.Set("pad", "tad")

	db, err = NewTree(testFilename, testBasePath, bkupName)
	assert.Nil(err)
	db.memtable = NewSizedMap()

	err = db.restoreMemtable()
	assert.Nil(err)
	assert.Equal(db.memtable.Contains("sad"), true)
	assert.Equal(db.memtable.Contains("pad"), true)
	assert.Equal(db.memtable.GetTotalSize(), 12)
}

func Test_flush_memtable_to_disk_populates_index(t *testing.T) {
	assert := assert.New(t)
	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)
	db.threshold = 100
	db.sparsityFactor = 25

	db.Set("abc", "123")
	db.Set("def", "456")
	db.Set("ghi", "789")
	db.Set("jkl", "012")
	db.Set("mno", "345")
	db.Set("pqr", "678")
	db.Set("stu", "901")
	db.Set("vwx", "234")

	err = db.flushMemtableToDisk(testPath)
	assert.Nil(err)

	assert.Equal(db.index.Size(), 2)
	assert.True(db.index.Contains(keyType("jkl")))
	assert.True(db.index.Contains(keyType("vwx")))
}

func Test_flush_memtable_to_disk_writes_most_recent_keys(t *testing.T) {
	assert := assert.New(t)
	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)
	db.threshold = 100

	db.Set("abc", "123")
	db.Set("abc", "ABC")
	db.Set("def", "345")
	db.Set("def", "DEF")
	db.Set("ghi", "567")
	db.Set("ghi", "GHI")

	err = db.flushMemtableToDisk(testPath)
	assert.Nil(err)

	lines := readFileLines(testPath)

	assert.Equal(len(lines), 3)
	assert.Equal(lines[0], "abc,ABC\n")
	assert.Equal(lines[1], "def,DEF\n")
	assert.Equal(lines[2], "ghi,GHI\n")
}

func Test_flush_memtable_to_disk_stores_segment_in_index(t *testing.T) {
	assert := assert.New(t)
	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)
	db.threshold = 100
	db.sparsityFactor = 25

	db.Set("abc", "123")
	db.Set("def", "456")
	db.Set("ghi", "789")
	db.Set("jkl", "012")

	err = db.flushMemtableToDisk(testPath)
	assert.Nil(err)

	db.Set("mno", "345")
	db.Set("pqr", "678")
	db.Set("stu", "901")
	db.Set("vwx", "234")

	db.segments = []string{"test_file-1", "test_file-2"}
	db.currentSegment = "test_file-2"
	err = db.flushMemtableToDisk(testPath)
	assert.Nil(err)

	segment1 := db.index.Find(keyType("jkl")).(*indexItem).Segment
	segment2 := db.index.Find(keyType("vwx")).(*indexItem).Segment

	assert.Equal(segment1, "test_file-1") // TODO fix?
	assert.Equal(segment2, "test_file-2")
}

func Test_flush_memtable_to_disk_stores_correct_index_offsets(t *testing.T) {
	assert := assert.New(t)
	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)
	db.threshold = 100
	db.sparsityFactor = 25

	db.Set("abc", "123")
	db.Set("def", "456")
	db.Set("ghi", "789")
	db.Set("jkl", "012")
	db.Set("mno", "345")
	db.Set("pqr", "678")
	db.Set("stu", "901")
	db.Set("vwx", "234")

	err = db.flushMemtableToDisk(testPath)
	assert.Nil(err)

	offset1 := db.index.Find(keyType("jkl")).(*indexItem).Offset
	offset2 := db.index.Find(keyType("vwx")).(*indexItem).Offset

	assert.Equal(offset1, int64(24))
	assert.Equal(offset2, int64(56))

	file, err := os.Open(testPath)
	assert.Nil(err)
	buf := make([]byte, 10)

	file.Seek(offset1, 0)
	_, err = file.Read(buf)
	assert.Nil(err)
	assert.Equal(string(buf[:bytes.IndexByte(buf, '\n')]), "jkl,012")

	file.Seek(offset2, 0)
	_, err = file.Read(buf)
	assert.Nil(err)
	assert.Equal(string(buf[:bytes.IndexByte(buf, '\n')]), "vwx,234")
}

func Test_db_get_uses_index_with_floor(t *testing.T) {
	assert := assert.New(t)
	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)

	db.bloomFilter.Add("chris")
	db.bloomFilter.Add("christian")
	db.bloomFilter.Add("daniel")

	s, err := os.Create(testBasePath + "segment2")
	assert.Nil(err)

	s.WriteString("chris,lessard\n")
	s.WriteString("christian,dior\n")
	s.WriteString("daniel,lessard\n")

	db.index.Insert(keyType("chris"), &indexItem{
		Segment: "segment2",
		Offset:  0,
		Val:     "lessard",
	})

	got, err := db.Get("christian")
	assert.Nil(err)
	assert.Equal(got, "dior")

	got, err = db.Get("daniel")
	assert.Nil(err)
	assert.Equal(got, "lessard")
}

func Test_repopulate_index_stores_correst_offsets(t *testing.T) {
	assert := assert.New(t)
	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)
	db.index.Insert(keyType("chris"), &indexItem{
		Offset: 10,
	})
	db.index.Insert(keyType("lessard"), &indexItem{
		Offset: 52,
	})
	db.segments = []string{"segment1", "segment2"}

	db.threshold = 10
	db.sparsityFactor = 5

	s, err := os.Create(testBasePath + "segment1")
	assert.Nil(err)

	s.WriteString("red,1\n")
	s.WriteString("blue,2\n")
	s.WriteString("green,3\n")
	s.WriteString("purple,4\n")

	s, err = os.Create(testBasePath + "segment2")
	assert.Nil(err)

	s.WriteString("cyan,5\n")
	s.WriteString("magenta,6\n")
	s.WriteString("yellow,7\n")
	s.WriteString("black,8\n")

	err = db.repopulateIndex()
	assert.Nil(err)

	blueNode := db.index.Find(keyType("blue"))
	assert.Equal(blueNode.(*indexItem).Offset, int64(6))

	s, err = os.Open(testBasePath + blueNode.(*indexItem).Segment)
	assert.Nil(err)

	s.Seek(blueNode.(*indexItem).Offset, 0)
	buf := make([]byte, 10)
	s.Read(buf)
	line := string(buf[:bytes.IndexByte(buf, '\n')])
	assert.Equal(line, "blue,2")

	magentaNode := db.index.Find(keyType("magenta"))
	assert.Equal(magentaNode.(*indexItem).Offset, int64(7))

	s, err = os.Open(testBasePath + magentaNode.(*indexItem).Segment)
	assert.Nil(err)

	s.Seek(magentaNode.(*indexItem).Offset, 0)
	buf = make([]byte, 10)
	s.Read(buf)
	line = string(buf[:bytes.IndexByte(buf, '\n')])
	assert.Equal(line, "magenta,6")
}

func Test_delete_keys_from_segment_deletes_one_key_from_file(t *testing.T) {
	assert := assert.New(t)
	lines := []string{"red,1\n", "blue,2\n", "green,3\n", "yellow,4\n"}
	keys := map[string]struct{}{
		"green": {},
	}
	file := testBasePath + "test_file-1"
	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)

	s, err := os.Create(file)
	assert.Nil(err)

	for _, line := range lines {
		s.WriteString(line)
	}

	err = db.deleteKeysFromSegment(keys, file)
	assert.Nil(err)

	alteredLines := readFileLines(file)
	assert.Equal(alteredLines, []string{"red,1\n", "blue,2\n", "yellow,4\n"})
}

func Test_delete_keys_from_segment_deletes_multiple_keys_from_file(t *testing.T) {
	assert := assert.New(t)
	lines := []string{"red,1\n", "blue,2\n", "green,3\n", "yellow,4\n"}
	keys := map[string]struct{}{
		"green": {},
		"blue":  {},
	}

	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)
	file := testBasePath + "test_file-1"

	s, err := os.Create(file)
	assert.Nil(err)

	for _, line := range lines {
		s.WriteString(line)
	}

	err = db.deleteKeysFromSegment(keys, file)
	assert.Nil(err)

	alteredLines := readFileLines(file)
	assert.Equal(alteredLines, []string{"red,1\n", "yellow,4\n"})
}

func Test_delete_keys_from_segments_deletes_one_key(t *testing.T) {
	assert := assert.New(t)
	lines := []string{"red,1\n", "blue,2\n", "green,3\n", "yellow,4\n"}
	keys := map[string]struct{}{
		"green": {},
	}
	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)
	files := []string{"test_file-1", "test_file-2", "test_file-3"}

	for _, file := range files {
		s, err := os.Create(testBasePath + file)
		assert.Nil(err)
		for _, line := range lines {
			s.WriteString(line)
		}
	}

	err = db.deleteKeysFromSegments(keys, files)
	assert.Nil(err)

	expectedLines := []string{
		"red,1\n",
		"blue,2\n",
		"yellow,4\n",
	}

	for _, file := range files {
		l := readFileLines(testBasePath + file)
		assert.Equal(l, expectedLines)
	}
}

func Test_delete_keys_from_segments_deletes_many_keys(t *testing.T) {
	assert := assert.New(t)
	lines := []string{"red,1\n", "blue,2\n", "green,3\n", "yellow,4\n"}
	files := []string{"test_file-1", "test_file-2", "test_file-3"}
	keys := map[string]struct{}{
		"red":   {},
		"green": {},
	}
	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)

	for _, file := range files {
		s, err := os.Create(testBasePath + file)
		assert.Nil(err)
		for _, line := range lines {
			s.WriteString(line)
		}
	}

	err = db.deleteKeysFromSegments(keys, files)
	assert.Nil(err)

	expectedLines := []string{
		"blue,2\n",
		"yellow,4\n",
	}
	for _, file := range files {
		l := readFileLines(testBasePath + file)
		assert.Equal(l, expectedLines)
	}
}

func Test_compact_dropes_one_key(t *testing.T) {
	assert := assert.New(t)
	lines := []string{"red,1\n", "blue,2\n", "green,3\n", "yellow,4\n"}
	files := []string{"test_file-1", "test_file-2", "test_file-3"}

	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)

	for _, file := range files {
		s, err := os.Create(testBasePath + file)
		assert.Nil(err)
		for _, line := range lines {
			s.WriteString(line)
		}
	}

	db.segments = files[:]

	for _, line := range lines {
		parts := strings.Split(line, ",")
		db.bloomFilter.Add(parts[0])
	}

	db.memtable.Set("green", "5")

	err = db.compact()
	assert.Nil(err)
	expectedLines := []string{"red,1\n", "blue,2\n", "yellow,4\n"}
	for _, file := range files {
		l := readFileLines(testBasePath + file)
		assert.Equal(l, expectedLines)
	}
}

func Test_compact_drops_multiple_keys(t *testing.T) {
	assert := assert.New(t)
	lines := []string{"red,1\n", "blue,2\n", "green,3\n", "yellow,4\n"}
	files := []string{"test_file-1", "test_file-2", "test_file-3"}

	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)

	for _, file := range files {
		s, err := os.Create(testBasePath + file)
		assert.Nil(err)
		for _, line := range lines {
			s.WriteString(line)
		}
	}

	db.segments = files[:]

	for _, line := range lines {
		parts := strings.Split(line, ",")
		db.bloomFilter.Add(parts[0])
	}

	db.memtable.Set("green", "5")
	db.memtable.Set("blue", "5")
	db.memtable.Set("red", "5")

	err = db.compact()
	assert.Nil(err)
	expectedLines := []string{"yellow,4\n"}
	for _, file := range files {
		l := readFileLines(testBasePath + file)
		assert.Equal(l, expectedLines)
	}
}

func Test_db_set_calls_compaction_algorithm(t *testing.T) {
	assert := assert.New(t)
	db, err := NewTree(testFilename, testBasePath, bkupName)
	defer cleanup()
	assert.Nil(err)

	db.threshold = 20

	db.Set("green", "green")
	db.Set("meant", "rents")

	db.Set("fring", "rings")
	db.Set("sides", "seeds")

	db.Set("scoop", "merps")
	db.Set("harps", "sterm")

	db.Set("fring", "boots")
	db.Set("scrap", "pracs")

	db.Set("scoon", "coons")

	lines := readFileLines(testBasePath + "test_file-2")
	assert.Equal(lines, []string{"sides,seeds\n"})
}
