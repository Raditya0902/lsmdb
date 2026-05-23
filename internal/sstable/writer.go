package sstable

import (
	"encoding/binary"
	"fmt"
	"os"

	"lsmdb/internal/bloom"
	"lsmdb/internal/memtable"
)

// Write serialises records (sorted ascending by key) to a new SSTable at path.
// The file is created (or truncated), written in full, and fsynced before returning.
// records must be sorted ascending by key — callers are responsible for ordering.
// Write is a no-op and returns nil when records is empty.
func Write(path string, records []memtable.Record) error {
	if len(records) == 0 {
		return nil
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create sstable: %w", err)
	}
	defer f.Close()

	var (
		dataOffset   int64
		indexEntries []indexEntry
		minKey       = records[0].Key
		maxKey       = records[len(records)-1].Key
	)

	// Build the bloom filter up front — it needs the complete key set.
	bf := bloom.New(len(records), 0.01)
	for _, r := range records {
		bf.Add([]byte(r.Key))
	}

	// Write data records and build the sparse index.
	for i, r := range records {
		if i%indexStep == 0 {
			indexEntries = append(indexEntries, indexEntry{key: r.Key, offset: dataOffset})
		}

		key := []byte(r.Key)
		recSize := recordHeaderSize + len(key) + len(r.Value)
		buf := make([]byte, recSize)

		binary.BigEndian.PutUint32(buf[0:], uint32(len(key)))
		binary.BigEndian.PutUint32(buf[4:], uint32(len(r.Value)))
		binary.BigEndian.PutUint64(buf[8:], r.SeqNum)
		buf[16] = uint8(r.Type)
		copy(buf[17:], key)
		copy(buf[17+len(key):], r.Value)

		if _, err := f.Write(buf); err != nil {
			return fmt.Errorf("write record: %w", err)
		}
		dataOffset += int64(recSize)
	}

	metaOffset := dataOffset

	// Write metadata block: minKey then maxKey.
	metaSize, err := writeMetaBlock(f, minKey, maxKey)
	if err != nil {
		return err
	}
	bloomOffset := metaOffset + metaSize

	// Write bloom filter section.
	bloomData := bf.Encode()
	if _, err := f.Write(bloomData); err != nil {
		return fmt.Errorf("write bloom: %w", err)
	}
	bloomLen := int64(len(bloomData))
	indexOffset := bloomOffset + bloomLen

	// Write sparse index entries.
	indexLen := int64(0)
	for _, e := range indexEntries {
		key := []byte(e.key)
		buf := make([]byte, 4+len(key)+8)
		binary.BigEndian.PutUint32(buf[0:], uint32(len(key)))
		copy(buf[4:], key)
		binary.BigEndian.PutUint64(buf[4+len(key):], uint64(e.offset))
		if _, err := f.Write(buf); err != nil {
			return fmt.Errorf("write index entry: %w", err)
		}
		indexLen += int64(len(buf))
	}

	// Write footer: 6 × 8-byte fields.
	footer := make([]byte, footerSize)
	binary.BigEndian.PutUint64(footer[0:], uint64(metaOffset))
	binary.BigEndian.PutUint64(footer[8:], uint64(bloomOffset))
	binary.BigEndian.PutUint64(footer[16:], uint64(bloomLen))
	binary.BigEndian.PutUint64(footer[24:], uint64(indexOffset))
	binary.BigEndian.PutUint64(footer[32:], uint64(indexLen))
	binary.BigEndian.PutUint64(footer[40:], uint64(len(records)))

	if _, err := f.Write(footer); err != nil {
		return fmt.Errorf("write footer: %w", err)
	}

	return f.Sync()
}

// writeMetaBlock encodes minKey and maxKey into the file and returns the number of bytes written.
func writeMetaBlock(f *os.File, minKey, maxKey string) (int64, error) {
	mk := []byte(minKey)
	mx := []byte(maxKey)
	buf := make([]byte, 4+len(mk)+4+len(mx))
	binary.BigEndian.PutUint32(buf[0:], uint32(len(mk)))
	copy(buf[4:], mk)
	binary.BigEndian.PutUint32(buf[4+len(mk):], uint32(len(mx)))
	copy(buf[4+len(mk)+4:], mx)
	if _, err := f.Write(buf); err != nil {
		return 0, fmt.Errorf("write metadata: %w", err)
	}
	return int64(len(buf)), nil
}
