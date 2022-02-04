package raftylog

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/hashicorp/raft"
)

type RaftLog struct {
	log *Log
	enc *gob.Encoder
	dec *gob.Decoder
	buf bytes.Buffer
	rdr bytes.Reader
}

func OpenLog(dir string, cfg *Config, createIfNeeded bool) (*RaftLog, error) {
	l, err := Open(dir, cfg, createIfNeeded)
	if err != nil {
		return nil, err
	}
	log := RaftLog{log: l}
	log.enc = gob.NewEncoder(&log.buf)
	log.dec = gob.NewDecoder(&log.rdr)
	return &log, nil
}

func (r *RaftLog) FirstIndex() (uint64, error) {
	return uint64(r.log.FirstIndex()), nil
}
func (r *RaftLog) LastIndex() (uint64, error) {
	return uint64(r.log.LastIndex()), nil
}

// GetLog gets a log entry at a given index.
func (r *RaftLog) GetLog(index uint64, log *raft.Log) error {
	v, err := r.log.Read(Index(index))
	if err != nil {
		return err
	}
	r.rdr.Reset(v)
	return r.dec.Decode(log)
}

// StoreLog stores a log entry.
func (r *RaftLog) StoreLog(log *raft.Log) error {
	r.buf.Reset()
	if err := r.enc.Encode(log); err != nil {
		return err
	}
	idx, err := r.log.Append(r.buf.Bytes())
	if err == nil && idx != Index(log.Index) {
		return fmt.Errorf("Log returned unexpected index of %d expecting %d", idx, log.Index)
	}
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
	if min == uint64(r.log.FirstIndex()) {
		return r.log.DeleteTo(Index(max + 1)) // max in inclusive, r.log is not
	}
	if max == uint64(r.log.LastIndex()) {
		return r.log.RewindTo(Index(max + 1)) // max is inclusive, r.log is not
	}
	return fmt.Errorf("Log range is %d-%d, can't make hole at %d-%d", r.log.FirstIndex(), r.log.LastIndex(), min, max)
}
