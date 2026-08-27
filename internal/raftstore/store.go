// Package raftstore persists Raft hard state and log entries.
package raftstore

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"lsmdb/internal/raft"
)

const (
	hardStateFile = "HARDSTATE"
	logFile       = "raft.log"
	entryHeader   = 8 + 8 + 4
	maxEntryData  = 4 << 20
)

// Store is a synchronized disk-backed Raft stable store.
type Store struct {
	mu      sync.Mutex
	dir     string
	logFile *os.File
	entries []raft.Entry
	offsets []int64
	hard    raft.HardState
}

// Open creates or recovers a stable store. A partial or corrupt trailing log
// record is truncated to the last verified CRC boundary.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create raft store: %w", err)
	}
	hard, err := loadHardState(dir)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, logFile), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open raft log: %w", err)
	}
	entries, offsets, validEnd, err := readLog(f)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if err := f.Truncate(validEnd); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("truncate raft log: %w", err)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("seek raft log: %w", err)
	}
	return &Store{dir: dir, logFile: f, entries: entries, offsets: offsets, hard: hard}, nil
}

// Load returns defensive copies of the durable state.
func (s *Store) Load() (raft.HardState, []raft.Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := make([]raft.Entry, len(s.entries))
	for i, entry := range s.entries {
		entries[i] = cloneEntry(entry)
	}
	return s.hard, entries
}

// Persist durably applies the persistence effects in update. Hard state and the
// modified log reach disk before this method returns.
func (s *Store) Persist(update raft.Update) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if update.HardState != nil {
		if err := storeHardState(s.dir, *update.HardState); err != nil {
			return err
		}
		s.hard = *update.HardState
	}
	if update.TruncateFrom != 0 {
		if err := s.truncate(update.TruncateFrom); err != nil {
			return err
		}
	}
	if len(update.Entries) > 0 {
		if err := s.append(update.Entries); err != nil {
			return err
		}
	}
	return nil
}

// Close syncs and closes the log.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.logFile.Sync(); err != nil {
		return err
	}
	return s.logFile.Close()
}

func (s *Store) truncate(from uint64) error {
	if from == 0 || from > uint64(len(s.entries))+1 {
		return fmt.Errorf("invalid raft truncation index %d", from)
	}
	if from == uint64(len(s.entries))+1 {
		return nil
	}
	offset := s.offsets[from-1]
	if err := s.logFile.Truncate(offset); err != nil {
		return fmt.Errorf("truncate raft log at %d: %w", from, err)
	}
	if _, err := s.logFile.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("seek truncated raft log: %w", err)
	}
	s.entries = s.entries[:from-1]
	s.offsets = s.offsets[:from-1]
	return s.logFile.Sync()
}

func (s *Store) append(entries []raft.Entry) error {
	for _, entry := range entries {
		want := uint64(len(s.entries) + 1)
		if entry.Index != want || entry.Term == 0 {
			return fmt.Errorf("append raft entry index %d term %d, want index %d", entry.Index, entry.Term, want)
		}
		if len(entry.Data) > maxEntryData {
			return fmt.Errorf("raft entry %d exceeds %d bytes", entry.Index, maxEntryData)
		}
		offset, err := s.logFile.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("locate raft log append: %w", err)
		}
		buf := encodeEntry(entry)
		if _, err := s.logFile.Write(buf); err != nil {
			return fmt.Errorf("append raft entry %d: %w", entry.Index, err)
		}
		s.offsets = append(s.offsets, offset)
		s.entries = append(s.entries, cloneEntry(entry))
	}
	if err := s.logFile.Sync(); err != nil {
		return fmt.Errorf("sync raft log: %w", err)
	}
	return nil
}

func readLog(f *os.File) ([]raft.Entry, []int64, int64, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, nil, 0, fmt.Errorf("seek raft log start: %w", err)
	}
	var entries []raft.Entry
	var offsets []int64
	var pos int64
	for {
		start := pos
		var header [entryHeader]byte
		n, err := io.ReadFull(f, header[:])
		pos += int64(n)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return entries, offsets, start, nil
		}
		if err != nil {
			return nil, nil, 0, fmt.Errorf("read raft log header: %w", err)
		}
		index := binary.BigEndian.Uint64(header[0:8])
		term := binary.BigEndian.Uint64(header[8:16])
		length := binary.BigEndian.Uint32(header[16:20])
		if length > maxEntryData || index != uint64(len(entries)+1) || term == 0 {
			return entries, offsets, start, nil
		}
		body := make([]byte, int(length)+4)
		n, err = io.ReadFull(f, body)
		pos += int64(n)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return entries, offsets, start, nil
		}
		if err != nil {
			return nil, nil, 0, fmt.Errorf("read raft log body: %w", err)
		}
		stored := binary.BigEndian.Uint32(body[length:])
		crcInput := append(append([]byte(nil), header[:]...), body[:length]...)
		if crc32.ChecksumIEEE(crcInput) != stored {
			return entries, offsets, start, nil
		}
		offsets = append(offsets, start)
		entries = append(entries, raft.Entry{Index: index, Term: term, Data: append([]byte(nil), body[:length]...)})
	}
}

func encodeEntry(entry raft.Entry) []byte {
	buf := make([]byte, entryHeader+len(entry.Data)+4)
	binary.BigEndian.PutUint64(buf[0:8], entry.Index)
	binary.BigEndian.PutUint64(buf[8:16], entry.Term)
	binary.BigEndian.PutUint32(buf[16:20], uint32(len(entry.Data)))
	copy(buf[20:], entry.Data)
	binary.BigEndian.PutUint32(buf[20+len(entry.Data):], crc32.ChecksumIEEE(buf[:20+len(entry.Data)]))
	return buf
}

func loadHardState(dir string) (raft.HardState, error) {
	data, err := os.ReadFile(filepath.Join(dir, hardStateFile))
	if errors.Is(err, os.ErrNotExist) {
		return raft.HardState{}, nil
	}
	if err != nil {
		return raft.HardState{}, fmt.Errorf("read raft hard state: %w", err)
	}
	var hard raft.HardState
	if err := json.Unmarshal(data, &hard); err != nil {
		return raft.HardState{}, fmt.Errorf("decode raft hard state: %w", err)
	}
	return hard, nil
}

func storeHardState(dir string, hard raft.HardState) error {
	data, err := json.Marshal(hard)
	if err != nil {
		return fmt.Errorf("encode raft hard state: %w", err)
	}
	tmp := filepath.Join(dir, ".HARDSTATE.tmp")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create hard state temp: %w", err)
	}
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(tmp)
		}
	}()
	if _, err := f.Write(append(data, '\n')); err != nil {
		_ = f.Close()
		return fmt.Errorf("write hard state: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync hard state: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close hard state: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, hardStateFile)); err != nil {
		return fmt.Errorf("publish hard state: %w", err)
	}
	ok = true
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open hard state dir: %w", err)
	}
	defer d.Close() //nolint:errcheck
	if err := d.Sync(); err != nil {
		return fmt.Errorf("sync hard state dir: %w", err)
	}
	return nil
}

func cloneEntry(entry raft.Entry) raft.Entry {
	entry.Data = append([]byte(nil), entry.Data...)
	return entry
}
