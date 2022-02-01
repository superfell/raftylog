package raftylog

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
)

func Test_Log(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if t.Failed() {
			t.Logf("Test failed, db state left in %v", dir)
		} else {
			os.RemoveAll(dir)
		}
	}()
	log, err := Open(dir, &Config{MaxSegmentItems: 3}, true)
	if err != nil {
		t.Fatal(err)
	}
	indexes := make([]Index, 0, 20)
	for i := byte(0); i < 20; i++ {
		idx, err := log.Append([]byte{i})
		if err != nil {
			t.Fatal(err)
		}
		if idx != log.LastIndex() {
			t.Errorf("LastIndex not updated after write")
		}
		indexes = append(indexes, idx)
	}
	for i := byte(0); i < 20; i++ {
		d, err := log.Read(indexes[i])
		if err != nil {
			t.Fatalf("log.Read failed for index %d with error %v", indexes[i], err)
		}
		if !bytes.Equal(d, []byte{i}) {
			t.Errorf("Unexpected data %v returned for index %d", d, indexes[i])
		}
	}
	if len(log.items) != 7 {
		t.Errorf("Expected 7 segments but have %d", len(log.items))
	}
	// open when the writer didn't clean up
	log2, err := Open(dir, &Config{MaxSegmentItems: 3}, true)
	if err != nil {
		t.Fatal(err)
	}
	for i := byte(0); i < 20; i++ {
		d, err := log2.Read(indexes[i])
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(d, []byte{i}) {
			t.Errorf("Unexpected data %v returned for index %d", d, indexes[i])
		}
	}
	log.Close()
	// open when the writer did cleanup
	log3, err := Open(dir, &Config{MaxSegmentItems: 3}, true)
	if err != nil {
		t.Fatal(err)
	}
	for i := byte(0); i < 20; i++ {
		d, err := log3.Read(indexes[i])
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(d, []byte{i}) {
			t.Errorf("Unexpected data %v returned for index %d", d, indexes[i])
		}
	}
	idx, err := log3.Append([]byte{'3'})
	if err != nil {
		t.Fatal(err)
	}
	act, err := log3.Read(idx)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(act, []byte{'3'}) {
		t.Errorf("Unexpected data %v", act)
	}

	if log3.FirstIndex() != 1 || log3.LastIndex() != 21 {
		t.Errorf("Log has unexpected range %d-%d", log3.FirstIndex(), log3.LastIndex())
	}
	log3.DeleteTo(11)
	if log3.FirstIndex() >= 11 {
		t.Errorf("DeleteTo removed too many entries")
	}
	if log3.FirstIndex() == 1 {
		t.Errorf("DeleteTo didn't remove any entries")
	}
	if err := log3.RewindTo(Index(21)); err != nil { // still in same segment
		t.Fatal(err)
	}
	t.Logf("segments after rewind to 21\n%v", log3.items)
	if log3.LastIndex() != Index(20) {
		t.Errorf("LastIndex is wrong after rewind")
	}
	if err := log3.RewindTo(Index(13)); err != nil {
		t.Fatal(err)
	}
	if log3.LastIndex() != Index(12) {
		t.Errorf("LastIndex is wrong after rewind, got %d should be 12", log3.LastIndex())
	}
	t.Log(log3.items)
	idx, err = log3.Append([]byte{'a'})
	if err != nil {
		t.Fatal(err)
	}
	if idx != Index(13) {
		t.Errorf("Append after rewind returned unexpected index of %d (should be 13)", idx)
	}
	t.Log(log3.items)
}
