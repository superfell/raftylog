package raftylog

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
)

var config = Config{0, 0}

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
	read(t, segr, 3, data3)
	read(t, segr, 1, data1)
	read(t, segr, 4, data4)
	read(t, segr, 2, data2)
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
