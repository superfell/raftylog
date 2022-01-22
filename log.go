package raftylog

import (
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
	nextIndex := Index(1)
	if len(log.items) > 0 {
		nextIndex = log.items[len(log.items)-1].lastIndex + 1
	}
	log.writer, err = newSegment(dir, &log.config, nextIndex)
	if err != nil {
		return nil, err
	}
	return &log, nil
}
