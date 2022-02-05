package raftylog

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

var config = Config{0, 0}

func testDir(t *testing.T) (string, func()) {
	dir, err := ioutil.TempDir("", t.Name())
	if err != nil {
		t.Fatal(err)
	}
	return dir, func() {
		if t.Failed() {
			t.Logf("Test failed, db state left in %v", dir)
		} else {
			os.RemoveAll(dir)
		}
	}
}

func Test_Segment(t *testing.T) {
	dir, err := ioutil.TempDir("", "*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		os.RemoveAll(dir)
	}()
	seg, err := newSegment(dir, &config, 1)
	if err != nil {
		t.Errorf("Error creating segment %v", err)
	}
	data1 := []byte{1, 2, 3, 4}
	write(t, seg, data1, 1)
	data2 := []byte{5, 6, 7, 8}
	write(t, seg, data2, 2)
	data3 := make([]byte, 5000)
	write(t, seg, data3, 3)

	read(t, &seg.reader, 1, data1)
	read(t, &seg.reader, 3, data3)
	read(t, &seg.reader, 2, data2)

	data4 := []byte{4}
	write(t, seg, data4, 4)
	read(t, &seg.reader, 1, data1)
	read(t, &seg.reader, 3, data3)
	read(t, &seg.reader, 2, data2)
	read(t, &seg.reader, 4, data4)

	seg.reader.close()

	files, err := os.ReadDir(dir)
	segr, err := openSegment(dir, files[0].Name())
	if err != nil {
		t.Fatalf("Failed to open existing segment %v", err)
	}
	if segr.lastIndex != 4 {
		t.Errorf("Last Index should be 4 but was %d", segr.lastIndex)
	}
	read(t, segr, 3, data3)
	read(t, segr, 1, data1)
	read(t, segr, 4, data4)
	read(t, segr, 2, data2)
}

func Test_NotFirstSegment(t *testing.T) {
	dir, err := ioutil.TempDir("", "*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		os.RemoveAll(dir)
	}()
	seg, err := newSegment(dir, &Config{MaxSegmentItems: 128}, 511)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 150)
	for i := 511; i <= 613; i++ {
		seg.append(data)
	}
	x, err := seg.reader.read(613)
	if err != nil {
		t.Fatal(err)
	}
	seg.finish()
	segr, err := openSegment(dir, fmt.Sprintf("%020d-%020d.seg", 511, 613))
	if err != nil {
		t.Fatal(err)
	}
	x, err = segr.read(613)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, x) {
		t.Fatal(x)
	}
}

func Test_WriteReadWrite(t *testing.T) {
	dir, cleanup := testDir(t)
	defer cleanup()
	seg, err := newSegment(dir, new(Config), 10)
	if err != nil {
		t.Fatal(err)
	}
	data1 := make([]byte, 100)
	data2 := make([]byte, 100)
	for i := byte(0); i < byte(len(data1)); i++ {
		data1[i] = i
		data2[i] = 200 - i
	}
	idx1, err := seg.append(data1)
	if err != nil {
		t.Fatal(err)
	}
	read1, err := seg.reader.read(idx1)
	if !bytes.Equal(read1, data1) {
		t.Fatalf("read1 wrong")
	}
	idx2, err := seg.append(data2)
	if err != nil {
		t.Fatal(err)
	}
	read1, err = seg.reader.read(idx1)
	if !bytes.Equal(read1, data1) {
		t.Fatalf("read1 wrong")
	}
	read2, err := seg.reader.read(idx2)
	if !bytes.Equal(read2, data2) {
		t.Fatalf("read2 wrong")
	}
}

func Test_SegmentRewind(t *testing.T) {
	dir, err := ioutil.TempDir("", "*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		os.RemoveAll(dir)
	}()
	seg, err := newSegment(dir, &config, 1)
	if err != nil {
		t.Errorf("Error creating segment %v", err)
	}
	for i := byte(0); i < 100; i++ {
		_, err := seg.append([]byte{i, i, i, i, i, i, i, i, i, i, i, i, i})
		if err != nil {
			t.Fatal(err)
		}
	}
	seg.rewindTo(Index(50))
	idx, err := seg.append([]byte{255})
	if err != nil {
		t.Fatal(err)
	}
	if idx != Index(50) {
		t.Errorf("Unexpected append index %d returned after rewind", idx)
	}
	for i := byte(0); i < 49; i++ {
		act, err := seg.reader.read(Index(i + 1))
		if err != nil {
			t.Fatal(err)
		}
		exp := []byte{i, i, i, i, i, i, i, i, i, i, i, i, i}
		if !bytes.Equal(exp, act) {
			t.Errorf("Index %d returned unexpected data of %v", i, act)
		}
	}
	act, err := seg.reader.read(idx)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal([]byte{255}, act) {
		t.Errorf("Index %d returned unexpected data of %v", idx, act)
	}
	seg.rewindTo(21)
	seg.finish()
	seg.reader.f.Close()

	files, err := os.ReadDir(dir)
	seg2, err := openSegment(dir, files[0].Name())
	if err != nil {
		t.Fatal(err)
	}
	if seg2.lastIndex != 20 {
		t.Errorf("Unexpected last index %d in rewound segment", seg2.lastIndex)
	}
	if err := seg2.rewindTo(Index(15)); err != nil {
		t.Fatal(err)
	}
	if seg2.lastIndex != Index(14) {
		t.Errorf("Unexpected last index %d in rewound segment", seg2.lastIndex)
	}
}

func read(t *testing.T, s *segmentReader, idx Index, expected []byte) {
	actual, err := s.read(idx)
	if err != nil {
		t.Errorf("Got error %v reading index %d from segment", err, idx)
	}
	if !bytes.Equal(actual, expected) {
		t.Errorf("Read of index %d returned unexpected results", idx)
	}
}

func write(t *testing.T, s *segmentReaderWriter, data []byte, expectedIdx Index) {
	idx, err := s.append(data)
	if err != nil {
		t.Errorf("Error writing to segment %v", err)
	}
	if idx != expectedIdx {
		t.Errorf("Unexpected index %d returned from segment.append (should be %d)", idx, expectedIdx)
	}
}
