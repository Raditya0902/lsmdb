// Package wal implements a binary write-ahead log for crash recovery.
//
// Record format on disk:
//
//	| type(1) | seq(8) | keyLen(4) | valLen(4) | key | value | crc32(4) |
//
// type: 1=PUT, 2=DELETE. CRC32 covers all preceding bytes in the record.
package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// Record type constants. Separate from memtable.RecordType to avoid package coupling.
const (
	TypePut    = uint8(1)
	TypeDelete = uint8(2)
)

// Guard against OOM when replaying a WAL with corrupted length fields.
const (
	maxKeyLen = 16 << 10 // 16 KB
	maxValLen = 64 << 20 // 64 MB
)

// Record is one WAL entry.
type Record struct {
	Type   uint8
	SeqNum uint64
	Key    string
	Value  []byte
}

// WAL is an append-only binary log backed by a single file.
type WAL struct {
	f *os.File
}

// Open opens or creates the WAL file at path.
// The caller must call ReadAll before any Append to position the write cursor.
func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	return &WAL{f: f}, nil
}

// Append encodes r and writes it atomically as a single Write call.
func (w *WAL) Append(r Record) error {
	key := []byte(r.Key)
	total := 1 + 8 + 4 + 4 + len(key) + len(r.Value) + 4
	buf := make([]byte, total)

	i := 0
	buf[i] = r.Type
	i++
	binary.BigEndian.PutUint64(buf[i:], r.SeqNum)
	i += 8
	binary.BigEndian.PutUint32(buf[i:], uint32(len(key)))
	i += 4
	binary.BigEndian.PutUint32(buf[i:], uint32(len(r.Value)))
	i += 4
	copy(buf[i:], key)
	i += len(key)
	copy(buf[i:], r.Value)
	i += len(r.Value)
	binary.BigEndian.PutUint32(buf[i:], crc32.ChecksumIEEE(buf[:i]))

	_, err := w.f.Write(buf)
	return err
}

// ReadAll replays every valid record from the beginning of the WAL.
// Truncated trailing records are dropped silently.
// Records with a bad CRC are skipped; reading continues at the next record boundary.
//
// After ReadAll the file is truncated to the end of the last valid record and
// the write cursor is positioned at EOF, ready for Append.
func (w *WAL) ReadAll() ([]Record, error) {
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("wal seek: %w", err)
	}

	var (
		records  []Record
		pos      int64
		validEnd int64
	)

	for {
		var hdr [17]byte
		n, err := io.ReadFull(w.f, hdr[:])
		pos += int64(n)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("wal read header: %w", err)
		}

		typ := hdr[0]
		seq := binary.BigEndian.Uint64(hdr[1:9])
		keyLen := binary.BigEndian.Uint32(hdr[9:13])
		valLen := binary.BigEndian.Uint32(hdr[13:17])

		if keyLen > maxKeyLen || valLen > maxValLen {
			// corrupt length fields — can't locate the next record boundary
			break
		}

		body := make([]byte, int(keyLen)+int(valLen)+4)
		n, err = io.ReadFull(w.f, body)
		pos += int64(n)
		if err == io.ErrUnexpectedEOF {
			break // truncated record at end of file
		}
		if err != nil {
			return nil, fmt.Errorf("wal read body: %w", err)
		}

		storedCRC := binary.BigEndian.Uint32(body[keyLen+valLen:])
		crcInput := make([]byte, 17+int(keyLen)+int(valLen))
		copy(crcInput, hdr[:])
		copy(crcInput[17:], body[:keyLen+valLen])
		if crc32.ChecksumIEEE(crcInput) != storedCRC {
			// bad CRC: skip this record; pos has already advanced past it
			continue
		}
		if typ != TypePut && typ != TypeDelete {
			continue // unknown type — skip
		}

		records = append(records, Record{
			Type:   typ,
			SeqNum: seq,
			Key:    string(body[:keyLen]),
			Value:  body[keyLen : keyLen+valLen],
		})
		validEnd = pos
	}

	// Drop any trailing partial or corrupt records so future Appends start cleanly.
	if err := w.f.Truncate(validEnd); err != nil {
		return nil, fmt.Errorf("wal truncate: %w", err)
	}
	if _, err := w.f.Seek(0, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("wal seek end: %w", err)
	}

	return records, nil
}

// Reset truncates the WAL to zero bytes and positions the write cursor at the start.
// Called after a successful memtable flush — the flushed data is now in an SSTable
// and no longer needs WAL-based recovery.
func (w *WAL) Reset() error {
	if err := w.f.Truncate(0); err != nil {
		return fmt.Errorf("wal reset truncate: %w", err)
	}
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("wal reset seek: %w", err)
	}
	return nil
}

// Close syncs the file to disk and closes it.
func (w *WAL) Close() error {
	if err := w.f.Sync(); err != nil {
		return err
	}
	return w.f.Close()
}
