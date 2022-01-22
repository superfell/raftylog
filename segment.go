package raftylog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"os"
	"path"
	"strconv"
	"strings"
)

type segmentReader struct {
	filename   string
	firstIndex Index
	lastIndex  Index
	f          *os.File
	offsets    []int64
}

type segmentReaderWriter struct {
	reader    segmentReader
	config    Config
	nextIndex Index
	fileSize  int64
}

func openSegment(dir, filename string) (*segmentReader, error) {
	// if the segment was cleanly closed, it'll be named firstIndex-lastIndex
	// if it wasn't it'll be called firstIndex and we'll have to find the last index ourselves
	parts := strings.SplitN(filename, "-", 2)
	fIdx, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return nil, err
	}
	firstIndex := Index(fIdx)
	lastIndex := Index(0)
	if len(parts) > 1 {
		lIdx, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return nil, err
		}
		lastIndex = Index(lIdx)
	}
	f, err := os.Open(path.Join(dir, filename))
	if err != nil {
		return nil, err
	}
	idx := Index(0)
	err = binary.Read(f, binary.LittleEndian, &idx)
	if err != nil {
		return nil, err
	}
	if idx != firstIndex {
		return nil, errors.New(fmt.Sprintf("Segment %v expected to having starting index %d but was %d", filename, firstIndex, idx))
	}
	rdr := &segmentReader{
		filename:   path.Join(dir, filename),
		firstIndex: firstIndex,
		lastIndex:  lastIndex,
		f:          f,
	}
	if lastIndex == 0 {
		rdr.index()
		rdr.lastIndex = rdr.firstIndex + Index(len(rdr.offsets)) - 1
	}
	return rdr, nil
}

func newSegment(dir string, config *Config, firstIndex Index) (*segmentReaderWriter, error) {
	fn := fmt.Sprintf("%020d", firstIndex)
	f, err := os.Create(path.Join(dir, fn))
	if err != nil {
		return nil, err
	}
	err = binary.Write(f, binary.LittleEndian, firstIndex)
	if err != nil {
		return nil, err
	}
	return &segmentReaderWriter{
		config: *config,
		reader: segmentReader{
			filename:   path.Join(dir, fn),
			firstIndex: firstIndex,
			lastIndex:  0,
			f:          f,
		},
		nextIndex: firstIndex,
		fileSize:  8,
	}, nil
}

func (s *segmentReader) close() error {
	err := s.f.Close()
	s.f = nil
	return err
}

func (s *segmentReader) read(idx Index) ([]byte, error) {
	if s.offsets == nil {
		if err := s.index(); err != nil {
			return nil, err
		}
	}
	offset := s.offsets[idx-s.firstIndex]
	if _, err := s.f.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	len := uint32(0)
	if err := binary.Read(s.f, binary.LittleEndian, &len); err != nil {
		return nil, err
	}
	data := make([]byte, len)
	if _, err := io.ReadFull(s.f, data); err != nil {
		return nil, err
	}
	hv := uint64(0)
	if err := binary.Read(s.f, binary.LittleEndian, &hv); err != nil {
		return nil, err
	}
	h := fnv.New64()
	h.Write(data)
	if hv != h.Sum64() {
		return nil, fmt.Errorf("Entry at index %d with offset %d has invalid hash of %x, expecting %x", idx, offset, h.Sum64(), h)
	}
	return data, nil
}

func (s *segmentReader) index() error {
	offset := int64(8)
	var err error
	offsets := make([]int64, 0, 32)
	for {
		if offset, err = s.f.Seek(offset, io.SeekStart); err != nil {
			if err == io.EOF {
				s.offsets = offsets
				return nil
			}
			return err
		}
		len := uint32(0)
		err = binary.Read(s.f, binary.LittleEndian, &len)
		if err == io.EOF {
			s.offsets = offsets
			return nil
		}
		offsets = append(offsets, offset)
		offset += 4 + int64(len) + 8 // len, data, hash
	}
}

func (s *segmentReaderWriter) full() bool {
	// returns true if there shouldn't be any more data appended to this segment
	if s.config.MaxSegmentItems > 0 {
		if s.nextIndex-s.reader.firstIndex > Index(s.config.MaxSegmentFileSize) {
			return true
		}
	}
	if s.config.MaxSegmentFileSize > 0 {
		if s.fileSize > s.config.MaxSegmentFileSize {
			return true
		}
	}
	return false
}

func (s *segmentReaderWriter) append(d []byte) (Index, error) {
	// 4 bytes for len, then data, then hash
	offset, err := s.reader.f.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	if len(d) > math.MaxUint32 {
		return 0, errors.New("Entry is larger than the maximum supported size")
	}
	l := uint32(len(d))
	err = binary.Write(s.reader.f, binary.LittleEndian, l)
	if err != nil {
		return 0, err
	}
	_, err = s.reader.f.Write(d)
	if err != nil {
		return 0, err
	}
	h := fnv.New64()
	h.Write(d)
	hv := h.Sum64()
	err = binary.Write(s.reader.f, binary.LittleEndian, hv)
	if err != nil {
		return 0, err
	}
	idx := s.nextIndex
	s.nextIndex++
	s.reader.offsets = append(s.reader.offsets, offset)
	s.reader.lastIndex = idx
	s.fileSize += (4 + int64(len(d)) + 8)
	return idx, nil
}
