package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"lsmdb/internal/bloom"
	"lsmdb/internal/memtable"
)

// StreamWriter constructs one SSTable from a strictly sorted record stream.
// It retains only Bloom-filter and sparse-index metadata in memory.
type StreamWriter struct {
	path         string
	f            *os.File
	expected     int64
	written      int64
	dataOffset   int64
	indexEntries []indexEntry
	bloom        *bloom.BloomFilter
	minKey       string
	maxKey       string
	lastKey      string
	closed       bool
}

// NewStreamWriter creates a streaming SSTable writer for exactly expected records.
func NewStreamWriter(path string, expected int64) (*StreamWriter, error) {
	if expected <= 0 {
		return nil, errors.New("streaming sstable requires a positive record count")
	}
	if expected > int64(^uint(0)>>1) {
		return nil, errors.New("streaming sstable record count exceeds platform limit")
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create sstable: %w", err)
	}
	return &StreamWriter{path: path, f: f, expected: expected, bloom: bloom.New(int(expected), 0.01)}, nil
}

// Add appends one record. Keys must be non-empty and strictly increasing.
func (w *StreamWriter) Add(record memtable.Record) error {
	if w.closed {
		return errors.New("streaming sstable writer is closed")
	}
	if record.Key == "" || (w.written > 0 && record.Key <= w.lastKey) {
		return errors.New("streaming sstable keys must be non-empty and strictly sorted")
	}
	if w.written >= w.expected {
		return fmt.Errorf("streaming sstable received more than %d records", w.expected)
	}
	key := []byte(record.Key)
	if uint64(len(key)) > uint64(^uint32(0)) || uint64(len(record.Value)) > uint64(^uint32(0)) {
		return errors.New("sstable record field exceeds uint32 length")
	}
	if w.written%indexStep == 0 {
		w.indexEntries = append(w.indexEntries, indexEntry{key: record.Key, offset: w.dataOffset})
	}
	buf := make([]byte, recordHeaderSize+len(key)+len(record.Value))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(key)))
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(record.Value)))
	binary.BigEndian.PutUint64(buf[8:16], record.SeqNum)
	buf[16] = uint8(record.Type)
	copy(buf[17:], key)
	copy(buf[17+len(key):], record.Value)
	if _, err := w.f.Write(buf); err != nil {
		return fmt.Errorf("write record: %w", err)
	}
	if w.written == 0 {
		w.minKey = record.Key
	}
	w.maxKey = record.Key
	w.lastKey = record.Key
	w.bloom.Add(key)
	w.dataOffset += int64(len(buf))
	w.written++
	return nil
}

// Finish writes the metadata/footer, syncs the table, and closes it.
func (w *StreamWriter) Finish() error {
	if w.closed {
		return errors.New("streaming sstable writer is closed")
	}
	if w.written != w.expected {
		return fmt.Errorf("streaming sstable received %d records, want %d", w.written, w.expected)
	}
	metaOffset := w.dataOffset
	metaSize, err := writeMetaBlock(w.f, w.minKey, w.maxKey)
	if err != nil {
		return err
	}
	bloomOffset := metaOffset + metaSize
	bloomData := w.bloom.Encode()
	if _, err := w.f.Write(bloomData); err != nil {
		return fmt.Errorf("write bloom: %w", err)
	}
	indexOffset := bloomOffset + int64(len(bloomData))
	var indexLen int64
	for _, entry := range w.indexEntries {
		key := []byte(entry.key)
		buf := make([]byte, 4+len(key)+8)
		binary.BigEndian.PutUint32(buf[0:4], uint32(len(key)))
		copy(buf[4:], key)
		binary.BigEndian.PutUint64(buf[4+len(key):], uint64(entry.offset))
		if _, err := w.f.Write(buf); err != nil {
			return fmt.Errorf("write index entry: %w", err)
		}
		indexLen += int64(len(buf))
	}
	footer := make([]byte, footerSize)
	binary.BigEndian.PutUint64(footer[0:8], uint64(metaOffset))
	binary.BigEndian.PutUint64(footer[8:16], uint64(bloomOffset))
	binary.BigEndian.PutUint64(footer[16:24], uint64(len(bloomData)))
	binary.BigEndian.PutUint64(footer[24:32], uint64(indexOffset))
	binary.BigEndian.PutUint64(footer[32:40], uint64(indexLen))
	binary.BigEndian.PutUint64(footer[40:48], uint64(w.written))
	if _, err := w.f.Write(footer); err != nil {
		return fmt.Errorf("write footer: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	w.closed = true
	return w.f.Close()
}

// Abort closes the writer and removes its unpublished file.
func (w *StreamWriter) Abort() {
	if !w.closed {
		w.closed = true
		_ = w.f.Close()
	}
	_ = os.Remove(w.path)
}
