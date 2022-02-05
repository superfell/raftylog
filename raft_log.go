package raftylog

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"strings"
	"sync"

	"github.com/hashicorp/raft"
)

type RaftLog struct {
	log  *Log
	buf  bytes.Buffer
	lock sync.Mutex
}

func OpenLog(dir string, cfg *Config, createIfNeeded bool) (*RaftLog, error) {
	l, err := Open(dir, cfg, createIfNeeded)
	if err != nil {
		return nil, err
	}
	log := RaftLog{log: l}
	fmt.Printf("Opened raft log with available indexes %d-%d\n", l.FirstIndex(), l.LastIndex())
	return &log, nil
}

func (r *RaftLog) Close() error {
	return r.log.Close()
}

func (r *RaftLog) FirstIndex() (uint64, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	return uint64(r.log.FirstIndex()), nil
}

func (r *RaftLog) LastIndex() (uint64, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	return uint64(r.log.LastIndex()), nil
}

// GetLog gets a log entry at a given index.
func (r *RaftLog) GetLog(index uint64, log *raft.Log) error {
	r.lock.Lock()
	v, err := r.log.Read(Index(index))
	r.lock.Unlock()
	if err != nil {
		fmt.Printf("error reading log entry %d %v\n", index, err)
		if strings.Contains(err.Error(), "not available") {
			return raft.ErrLogNotFound
		}
		return err
	}
	return gob.NewDecoder(bytes.NewReader(v)).Decode(log)
}

// StoreLog stores a log entry.
func (r *RaftLog) StoreLog(log *raft.Log) error {
	r.lock.Lock()
	r.buf.Reset()
	if err := gob.NewEncoder(&r.buf).Encode(log); err != nil {
		r.lock.Unlock()
		return err
	}
	idx, err := r.log.Append(r.buf.Bytes())
	r.lock.Unlock()
	if err == nil && idx != Index(log.Index) {
		return fmt.Errorf("Log returned unexpected index of %d expecting %d", idx, log.Index)
	}
	//	fmt.Printf("Wrote log entry %d\n", idx)
	return err
}

// StoreLogs stores multiple log entries.
func (r *RaftLog) StoreLogs(logs []*raft.Log) error {
	for _, l := range logs {
		if err := r.StoreLog(l); err != nil {
			return err
		}
	}
	return nil
}

// DeleteRange deletes a range of log entries. The range is inclusive.
func (r *RaftLog) DeleteRange(min, max uint64) error {
	// range can either be at the start of the log or at the end of the log depending on what
	// the raft library is trying to do.
	r.lock.Lock()
	defer r.lock.Unlock()
	if min <= uint64(r.log.FirstIndex()) {
		return r.log.DeleteTo(Index(max + 1)) // max in inclusive, r.log is not
	}
	return r.log.RewindTo(Index(min) + 1) // min is inclusive, r.log is not
}
