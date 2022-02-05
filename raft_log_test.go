package raftylog

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func Test_RaftLog(t *testing.T) {
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
	log, err := OpenLog(dir, &Config{MaxSegmentItems: 100}, true)
	if err != nil {
		t.Fatal(err)
	}
	e := raft.Log{
		Index:      1,
		Term:       43,
		Type:       raft.LogCommand,
		Data:       []byte{1, 2, 3},
		Extensions: []byte{4},
		AppendedAt: time.Now(),
	}
	err = log.StoreLog(&e)
	if err != nil {
		t.Fatal(err)
	}
	read := raft.Log{}
	err = log.GetLog(1, &read)
	if err != nil {
		t.Fatal(err)
	}
	if !logEq(e, read) {
		t.Errorf("Log entries don't match\n%+v\n%+v", e, read)
	}
	e2 := e
	e2.Index = 2
	err = log.StoreLog(&e2)
	if err != nil {
		t.Fatal(err)
	}
	err = log.GetLog(1, &read)
	if err != nil {
		t.Fatal(err)
	}
	if !logEq(e, read) {
		t.Errorf("Log entries don't match\n%+v\n%+v", e, read)
	}
	err = log.GetLog(2, &read)
	if err != nil {
		t.Fatal(err)
	}
	if !logEq(e2, read) {
		t.Errorf("Log entries don't match\n%+v\n%+v", e2, read)
	}
}

func logEq(a, b raft.Log) bool {
	return a.Index == b.Index &&
		a.Term == b.Term &&
		a.Type == b.Type &&
		bytes.Equal(a.Data, b.Data) &&
		bytes.Equal(a.Extensions, b.Extensions)
}
