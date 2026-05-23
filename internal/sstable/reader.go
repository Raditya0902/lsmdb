package sstable

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"sync/atomic"

	"lsmdb/internal/bloom"
	"lsmdb/internal/memtable"
)

// Reader opens an SSTable file for point lookups and full scans.
// The footer, bloom filter, and sparse index are loaded into memory on Open;
// data records are read on demand via ReadAt.
type Reader struct {
	f           *os.File
	bf          *bloom.BloomFilter
	index       []indexEntry
	minKey      string
	maxKey      string
	recordCount int64
	metaOffset  int64 // byte offset where the data section ends
	bloomSkips  int64 // incremented atomically each time bloom filter rejects a key
}

// Open loads an SSTable's footer, bloom filter, metadata, and sparse index into memory.
// The caller must call Close when done.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open sstable: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat sstable: %w", err)
	}
	if info.Size() < footerSize {
		f.Close()
		return nil, fmt.Errorf("sstable %q is too small to be valid", path)
	}

	// Decode footer (last 48 bytes).
	footerBuf := make([]byte, footerSize)
	if _, err := f.ReadAt(footerBuf, info.Size()-footerSize); err != nil {
		f.Close()
		return nil, fmt.Errorf("read footer: %w", err)
	}
	ft := sstFooter{
		metaOffset:  int64(binary.BigEndian.Uint64(footerBuf[0:])),
		bloomOffset: int64(binary.BigEndian.Uint64(footerBuf[8:])),
		bloomLen:    int64(binary.BigEndian.Uint64(footerBuf[16:])),
		indexOffset: int64(binary.BigEndian.Uint64(footerBuf[24:])),
		indexLen:    int64(binary.BigEndian.Uint64(footerBuf[32:])),
		recordCount: int64(binary.BigEndian.Uint64(footerBuf[40:])),
	}

	// Decode metadata block (minKey, maxKey).
	metaBuf := make([]byte, ft.bloomOffset-ft.metaOffset)
	if _, err := f.ReadAt(metaBuf, ft.metaOffset); err != nil {
		f.Close()
		return nil, fmt.Errorf("read metadata: %w", err)
	}
	minKeyLen := int(binary.BigEndian.Uint32(metaBuf[0:]))
	minKey := string(metaBuf[4 : 4+minKeyLen])
	maxKeyLen := int(binary.BigEndian.Uint32(metaBuf[4+minKeyLen:]))
	maxKey := string(metaBuf[4+minKeyLen+4 : 4+minKeyLen+4+maxKeyLen])

	// Decode bloom filter.
	bloomBuf := make([]byte, ft.bloomLen)
	if _, err := f.ReadAt(bloomBuf, ft.bloomOffset); err != nil {
		f.Close()
		return nil, fmt.Errorf("read bloom: %w", err)
	}
	bf, err := bloom.Decode(bloomBuf)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("decode bloom: %w", err)
	}

	// Decode sparse index.
	var index []indexEntry
	if ft.indexLen > 0 {
		indexBuf := make([]byte, ft.indexLen)
		if _, err := f.ReadAt(indexBuf, ft.indexOffset); err != nil {
			f.Close()
			return nil, fmt.Errorf("read index: %w", err)
		}
		index, err = decodeIndex(indexBuf)
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("decode index: %w", err)
		}
	}

	return &Reader{
		f:           f,
		bf:          bf,
		index:       index,
		minKey:      minKey,
		maxKey:      maxKey,
		recordCount: ft.recordCount,
		metaOffset:  ft.metaOffset,
	}, nil
}

// Get looks up key in the SSTable.
// The bloom filter is consulted first: if it returns false the file is skipped entirely.
// Returns the record and true if found; the record may be a tombstone (Type == RecordTypeDelete).
// Callers must check the returned record's Type before treating it as a live value.
func (r *Reader) Get(key string) (memtable.Record, bool, error) {
	// Bloom check: definite miss → skip the file entirely.
	if !r.bf.MayContain([]byte(key)) {
		atomic.AddInt64(&r.bloomSkips, 1)
		return memtable.Record{}, false, nil
	}

	// Range check: key outside [minKey, maxKey] cannot be in this SSTable.
	if key < r.minKey || key > r.maxKey {
		return memtable.Record{}, false, nil
	}

	// Binary-search index for the largest entry with entry.key <= key.
	startOffset := int64(0)
	if len(r.index) > 0 {
		i := sort.Search(len(r.index), func(i int) bool {
			return r.index[i].key > key
		}) - 1
		if i >= 0 {
			startOffset = r.index[i].offset
		}
	}

	// Scan forward from startOffset through the data section.
	pos := startOffset
	var best memtable.Record
	found := false

	for pos < r.metaOffset {
		rec, size, err := r.readRecordAt(pos)
		if err != nil {
			return memtable.Record{}, false, err
		}
		pos += size

		if rec.Key == key {
			if !found || rec.SeqNum > best.SeqNum {
				best = rec
				found = true
			}
		} else if rec.Key > key {
			break // sorted order — no further match possible
		}
	}

	return best, found, nil
}

// LoadAll returns every record in the data section, in sorted key order.
// Used for compaction and testing.
func (r *Reader) LoadAll() ([]memtable.Record, error) {
	records := make([]memtable.Record, 0, r.recordCount)
	pos := int64(0)

	for pos < r.metaOffset {
		rec, size, err := r.readRecordAt(pos)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
		pos += size
	}

	return records, nil
}

// BloomSkips returns the number of times this reader's bloom filter returned false,
// causing the SSTable to be skipped without a disk scan.
func (r *Reader) BloomSkips() int64 {
	return atomic.LoadInt64(&r.bloomSkips)
}

// Path returns the file-system path this Reader was opened from.
func (r *Reader) Path() string {
	return r.f.Name()
}

// RecordCount returns the number of data records in this SSTable.
func (r *Reader) RecordCount() int64 {
	return r.recordCount
}

// Close releases the file descriptor held by the Reader.
func (r *Reader) Close() error {
	return r.f.Close()
}

// readRecordAt decodes one data record from the given byte offset.
// Returns the record, the total number of bytes consumed, and any error.
func (r *Reader) readRecordAt(offset int64) (memtable.Record, int64, error) {
	hdr := make([]byte, recordHeaderSize)
	if _, err := r.f.ReadAt(hdr, offset); err != nil {
		return memtable.Record{}, 0, fmt.Errorf("read record header at %d: %w", offset, err)
	}

	keyLen := int(binary.BigEndian.Uint32(hdr[0:]))
	valLen := int(binary.BigEndian.Uint32(hdr[4:]))
	seqNum := binary.BigEndian.Uint64(hdr[8:])
	recType := memtable.RecordType(hdr[16])

	body := make([]byte, keyLen+valLen)
	if len(body) > 0 {
		if _, err := r.f.ReadAt(body, offset+recordHeaderSize); err != nil {
			return memtable.Record{}, 0, fmt.Errorf("read record body at %d: %w", offset, err)
		}
	}

	rec := memtable.Record{
		Key:    string(body[:keyLen]),
		Value:  body[keyLen : keyLen+valLen],
		SeqNum: seqNum,
		Type:   recType,
	}
	return rec, int64(recordHeaderSize + keyLen + valLen), nil
}

// decodeIndex parses raw index bytes into a slice of indexEntry.
func decodeIndex(buf []byte) ([]indexEntry, error) {
	var entries []indexEntry
	pos := 0
	for pos < len(buf) {
		if pos+4 > len(buf) {
			return nil, fmt.Errorf("truncated index entry at pos %d", pos)
		}
		keyLen := int(binary.BigEndian.Uint32(buf[pos:]))
		pos += 4
		if pos+keyLen+8 > len(buf) {
			return nil, fmt.Errorf("truncated index key at pos %d", pos)
		}
		key := string(buf[pos : pos+keyLen])
		pos += keyLen
		offset := int64(binary.BigEndian.Uint64(buf[pos:]))
		pos += 8
		entries = append(entries, indexEntry{key: key, offset: offset})
	}
	return entries, nil
}
