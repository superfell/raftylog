package raftylog

import (
	"errors"
	"fmt"
	"os"
	"sort"
)

type Index uint64

type Config struct {
	MaxSegmentFileSize int64
	MaxSegmentItems    int64
}

type Log struct {
	config Config
	dir    string
	items  []*segmentReader
	writer *segmentReaderWriter
}

func Open(dir string, config *Config, createIfMissing bool) (*Log, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 && !createIfMissing {
		return nil, errors.New("Directory doesn't contain a log")
	}
	log := Log{
		config: *config,
		dir:    dir,
		items:  make([]*segmentReader, 0, len(files)),
	}
	for _, f := range files {
		if f.IsDir() {
			continue // error?
		}
		seg, err := openSegment(dir, f.Name())
		if err != nil {
			return nil, err
		}
		log.items = append(log.items, seg)
	}
	sort.Slice(log.items, func(a, b int) bool {
		return log.items[a].firstIndex < log.items[b].firstIndex
	})
	// TODO assert log segments are contiguous
	return &log, nil
}

func (log *Log) Append(data []byte) (Index, error) {
	nextIndex := Index(1)
	var err error
	if log.writer != nil && log.writer.full() {
		if err = log.writer.finish(); err != nil {
			return 0, err
		}
		nextIndex = log.writer.nextIndex
		log.writer = nil
	}
	if log.writer == nil {
		if nextIndex == 1 && len(log.items) > 0 {
			nextIndex = log.items[len(log.items)-1].lastIndex + 1
		}
		log.writer, err = newSegment(log.dir, &log.config, nextIndex)
		if err != nil {
			return 0, err
		}
		log.items = append(log.items, &log.writer.reader)
	}
	return log.writer.append(data)
}

func (log *Log) Read(idx Index) ([]byte, error) {
	if log.items[0].firstIndex > idx {
		return nil, fmt.Errorf("Index %d not available, earliest available index is %d", idx, log.items[0].firstIndex)
	}
	segIdx := sort.Search(len(log.items), func(i int) bool {
		return log.items[i].lastIndex >= idx
	})
	if segIdx < len(log.items) && idx <= log.items[segIdx].lastIndex {
		return log.items[segIdx].read(idx)
	}
	return nil, fmt.Errorf("Index %d is after any available index", idx)
}

// Delete all log entries with an index < idx
func (log *Log) DeleteTo(idx Index) error {
	if idx >= log.LastIndex() {
		return errors.New("Can't delete entire log")
	}
	for len(log.items) > 0 && log.items[0].lastIndex < idx {
		log.items[0].close()
		err := os.Remove(log.items[0].filename)
		log.items = log.items[1:]
		if err != nil {
			return err
		}
	}
	return nil
}

// RewindTo truncates the end of the log making idx the next index to be written.
// You can't Rewind to before the current logs FirstIndex.
func (log *Log) RewindTo(idx Index) error {
	if idx <= log.FirstIndex() {
		return errors.New("Can't rewind that far back")
	}
	if idx > log.LastIndex() {
		return errors.New("Can't rewind past the end of the log")
	}
	// easy case, we want to rewind to a spot that's inside the current writer
	if log.writer != nil && idx >= log.writer.reader.firstIndex {
		return log.writer.rewindTo(idx)
	}
	// harder case, we want to rewind to a spot that in a previous segment
	log.writer = nil
	// the writer segment is in items as well, so that's dealt with in this loop
	for len(log.items) > 0 && log.items[len(log.items)-1].firstIndex >= idx {
		rdr := log.items[len(log.items)-1]
		err := rdr.delete()
		log.items = log.items[:len(log.items)-1]
		if err != nil {
			return err
		}
	}
	// now we need to split the segment on the idx boundary.
	if idx == log.LastIndex()+1 {
		// we may of ended exactly on an existing segment boundary. if so we're done
		return nil
	}
	return log.items[len(log.items)-1].rewindTo(idx)
	// the next write will deal with creating a new writer, we don't need to do it here
}

func (log *Log) Close() error {
	if log.writer != nil {
		if err := log.writer.finish(); err != nil {
			return err
		}
	}
	for _, item := range log.items {
		item.close()
	}
	log.items = nil
	log.writer = nil
	return nil
}

func (log *Log) FirstIndex() Index {
	return log.items[0].firstIndex
}

func (log *Log) LastIndex() Index {
	return log.items[len(log.items)-1].lastIndex
}
