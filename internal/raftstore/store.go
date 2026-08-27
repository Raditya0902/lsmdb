// Package raftstore persists Raft hard state and log entries.
package raftstore

import (
	"bytes"
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
	hardStateFile   = "HARDSTATE"
	snapshotFile    = "SNAPSHOT"
	logFile         = "raft.log"
	entryHeader     = 8 + 8 + 4
	maxEntryData    = 4 << 20
	maxSnapshotData = uint64(64 << 30)
	snapshotHeader  = 8 + 8 + 8
)

// Store is a synchronized disk-backed Raft stable store.
type Store struct {
	mu               sync.Mutex
	dir              string
	logFile          *os.File
	entries          []raft.Entry
	offsets          []int64
	hard             raft.HardState
	snapshot         raft.Snapshot
	snapshotSize     uint64
	snapshotChecksum uint32
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
	snapshot, snapshotSize, snapshotChecksum, err := inspectSnapshot(dir)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, logFile), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open raft log: %w", err)
	}
	entries, offsets, validEnd, err := readLog(f, snapshot.Index)
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
	return &Store{dir: dir, logFile: f, entries: entries, offsets: offsets, hard: hard, snapshot: snapshot, snapshotSize: snapshotSize, snapshotChecksum: snapshotChecksum}, nil
}

// SnapshotMetadata returns the durable snapshot without materializing its data.
func (s *Store) SnapshotMetadata() raft.Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneSnapshot(s.snapshot)
}

// LoadSnapshot returns a byte-backed copy for compatibility and small tests.
func (s *Store) LoadSnapshot() raft.Snapshot {
	metadata := s.SnapshotMetadata()
	if metadata.Index == 0 {
		return metadata
	}
	reader, _, _, err := s.OpenSnapshot(metadata.Index)
	if err != nil {
		return raft.Snapshot{}
	}
	defer reader.Close()
	metadata.Data, err = io.ReadAll(reader)
	if err != nil {
		return raft.Snapshot{}
	}
	return metadata
}

type sectionReadCloser struct {
	*io.SectionReader
	closer io.Closer
}

func (r *sectionReadCloser) Close() error { return r.closer.Close() }

type boundedSnapshotWriter struct {
	writer  io.Writer
	written uint64
}

func (w *boundedSnapshotWriter) Write(data []byte) (int, error) {
	if uint64(len(data)) > maxSnapshotData-w.written {
		return 0, fmt.Errorf("snapshot exceeds %d bytes", maxSnapshotData)
	}
	n, err := w.writer.Write(data)
	w.written += uint64(n)
	return n, err
}

// OpenSnapshot opens the durable state-machine bytes for one snapshot.
func (s *Store) OpenSnapshot(index uint64) (io.ReadCloser, uint64, uint32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index == 0 || index != s.snapshot.Index {
		return nil, 0, 0, fmt.Errorf("raft snapshot %d is not durable", index)
	}
	f, err := os.Open(filepath.Join(s.dir, snapshotFile))
	if err != nil {
		return nil, 0, 0, fmt.Errorf("open raft snapshot: %w", err)
	}
	return &sectionReadCloser{SectionReader: io.NewSectionReader(f, snapshotHeader, int64(s.snapshotSize)), closer: f}, s.snapshotSize, s.snapshotChecksum, nil
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
	return s.persist(update, nil)
}

// PersistSnapshot streams snapshot data while durably applying its Raft update.
func (s *Store) PersistSnapshot(update raft.Update, writeData func(io.Writer) error) error {
	if update.Snapshot == nil || writeData == nil {
		return errors.New("streamed snapshot persistence requires metadata and a writer")
	}
	return s.persist(update, writeData)
}

func (s *Store) persist(update raft.Update, writeData func(io.Writer) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if update.HardState != nil {
		if err := storeHardState(s.dir, *update.HardState); err != nil {
			return err
		}
		s.hard = *update.HardState
	}
	if update.Snapshot != nil {
		if writeData == nil {
			data := append([]byte(nil), update.Snapshot.Data...)
			writeData = func(writer io.Writer) error {
				_, err := io.Copy(writer, bytes.NewReader(data))
				return err
			}
		}
		if err := s.installSnapshot(*update.Snapshot, writeData); err != nil {
			return err
		}
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
	first := s.snapshot.Index + 1
	last := s.snapshot.Index + uint64(len(s.entries))
	if from < first || from > last+1 {
		return fmt.Errorf("invalid raft truncation index %d", from)
	}
	if from == last+1 {
		return nil
	}
	offsetIndex := from - first
	offset := s.offsets[offsetIndex]
	if err := s.logFile.Truncate(offset); err != nil {
		return fmt.Errorf("truncate raft log at %d: %w", from, err)
	}
	if _, err := s.logFile.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("seek truncated raft log: %w", err)
	}
	s.entries = s.entries[:offsetIndex]
	s.offsets = s.offsets[:offsetIndex]
	return s.logFile.Sync()
}

func (s *Store) append(entries []raft.Entry) error {
	for _, entry := range entries {
		want := s.snapshot.Index + uint64(len(s.entries)) + 1
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

func readLog(f *os.File, snapshotIndex uint64) ([]raft.Entry, []int64, int64, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, nil, 0, fmt.Errorf("seek raft log start: %w", err)
	}
	var entries []raft.Entry
	var offsets []int64
	var pos int64
	var previous uint64
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
		if length > maxEntryData || term == 0 || (previous != 0 && index != previous+1) {
			return entries, offsets, start, nil
		}
		if previous == 0 && index != 1 && index != snapshotIndex+1 {
			return entries, offsets, start, nil
		}
		previous = index
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
		if index > snapshotIndex {
			offsets = append(offsets, start)
			entries = append(entries, raft.Entry{Index: index, Term: term, Data: append([]byte(nil), body[:length]...)})
		}
	}
}

func (s *Store) installSnapshot(snapshot raft.Snapshot, writeData func(io.Writer) error) error {
	if snapshot.Index == 0 || snapshot.Term == 0 {
		return fmt.Errorf("invalid raft snapshot %d/%d (%d bytes)", snapshot.Index, snapshot.Term, len(snapshot.Data))
	}
	if snapshot.Index <= s.snapshot.Index {
		return nil
	}
	keepSuffix := false
	if snapshot.Index > s.snapshot.Index && snapshot.Index <= s.snapshot.Index+uint64(len(s.entries)) {
		keepSuffix = s.entries[snapshot.Index-s.snapshot.Index-1].Term == snapshot.Term
	}
	var suffix []raft.Entry
	if keepSuffix {
		start := snapshot.Index - s.snapshot.Index
		for _, entry := range s.entries[start:] {
			suffix = append(suffix, cloneEntry(entry))
		}
	}
	size, checksum, err := storeSnapshotStream(s.dir, snapshot, writeData)
	if err != nil {
		return err
	}
	if err := s.rewriteLog(suffix); err != nil {
		return err
	}
	snapshot.Data = nil
	s.snapshot = cloneSnapshot(snapshot)
	s.snapshotSize = size
	s.snapshotChecksum = checksum
	return nil
}

func (s *Store) rewriteLog(entries []raft.Entry) error {
	tmp := filepath.Join(s.dir, ".raft.log.tmp")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create compacted raft log: %w", err)
	}
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(tmp)
		}
	}()
	for _, entry := range entries {
		if _, err := f.Write(encodeEntry(entry)); err != nil {
			_ = f.Close()
			return fmt.Errorf("rewrite raft entry %d: %w", entry.Index, err)
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync compacted raft log: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close compacted raft log: %w", err)
	}
	if err := s.logFile.Close(); err != nil {
		return fmt.Errorf("close old raft log: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, logFile)); err != nil {
		return fmt.Errorf("publish compacted raft log: %w", err)
	}
	ok = true
	if err := syncDir(s.dir); err != nil {
		return err
	}
	newFile, err := os.OpenFile(filepath.Join(s.dir, logFile), os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("reopen compacted raft log: %w", err)
	}
	s.logFile = newFile
	s.entries = nil
	s.offsets = nil
	var offset int64
	for _, entry := range entries {
		s.offsets = append(s.offsets, offset)
		s.entries = append(s.entries, cloneEntry(entry))
		offset += int64(len(encodeEntry(entry)))
	}
	_, err = s.logFile.Seek(0, io.SeekEnd)
	return err
}

func inspectSnapshot(dir string) (raft.Snapshot, uint64, uint32, error) {
	f, err := os.Open(filepath.Join(dir, snapshotFile))
	if errors.Is(err, os.ErrNotExist) {
		return raft.Snapshot{}, 0, 0, nil
	}
	if err != nil {
		return raft.Snapshot{}, 0, 0, fmt.Errorf("open raft snapshot: %w", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return raft.Snapshot{}, 0, 0, fmt.Errorf("stat raft snapshot: %w", err)
	}
	if info.Size() < snapshotHeader+4 {
		return raft.Snapshot{}, 0, 0, errors.New("raft snapshot is truncated")
	}
	var header [snapshotHeader]byte
	if _, err := io.ReadFull(f, header[:]); err != nil {
		return raft.Snapshot{}, 0, 0, errors.New("raft snapshot is truncated")
	}
	index := binary.BigEndian.Uint64(header[0:8])
	term := binary.BigEndian.Uint64(header[8:16])
	length := binary.BigEndian.Uint64(header[16:24])
	if index == 0 || term == 0 || length > maxSnapshotData || length > uint64(info.Size()-snapshotHeader-4) || length > uint64(^uint64(0)-snapshotHeader-8) {
		return raft.Snapshot{}, 0, 0, errors.New("raft snapshot header is invalid")
	}
	baseEnd := int64(snapshotHeader) + int64(length)
	var membership raft.Membership
	if info.Size() != baseEnd+4 {
		if info.Size() < baseEnd+8 {
			return raft.Snapshot{}, 0, 0, errors.New("raft snapshot membership is truncated")
		}
		var lengthBytes [4]byte
		if _, err := f.ReadAt(lengthBytes[:], baseEnd); err != nil {
			return raft.Snapshot{}, 0, 0, errors.New("raft snapshot membership is truncated")
		}
		membershipLen := uint64(binary.BigEndian.Uint32(lengthBytes[:]))
		if membershipLen > 1<<20 || uint64(info.Size()) != uint64(baseEnd)+4+membershipLen+4 {
			return raft.Snapshot{}, 0, 0, errors.New("raft snapshot membership length is invalid")
		}
		membershipData := make([]byte, membershipLen)
		if _, err := f.ReadAt(membershipData, baseEnd+4); err != nil {
			return raft.Snapshot{}, 0, 0, errors.New("raft snapshot membership is truncated")
		}
		if err := json.Unmarshal(membershipData, &membership); err != nil {
			return raft.Snapshot{}, 0, 0, fmt.Errorf("decode snapshot membership: %w", err)
		}
	}
	var storedChecksum [4]byte
	if _, err := f.ReadAt(storedChecksum[:], info.Size()-4); err != nil {
		return raft.Snapshot{}, 0, 0, errors.New("raft snapshot checksum is truncated")
	}
	wholeHash := crc32.NewIEEE()
	if _, err := io.CopyN(wholeHash, io.NewSectionReader(f, 0, info.Size()-4), info.Size()-4); err != nil {
		return raft.Snapshot{}, 0, 0, fmt.Errorf("checksum raft snapshot: %w", err)
	}
	if wholeHash.Sum32() != binary.BigEndian.Uint32(storedChecksum[:]) {
		return raft.Snapshot{}, 0, 0, errors.New("raft snapshot checksum mismatch")
	}
	dataHash := crc32.NewIEEE()
	if _, err := io.CopyN(dataHash, io.NewSectionReader(f, snapshotHeader, int64(length)), int64(length)); err != nil {
		return raft.Snapshot{}, 0, 0, fmt.Errorf("checksum raft snapshot data: %w", err)
	}
	return raft.Snapshot{Index: index, Term: term, Membership: membership}, length, dataHash.Sum32(), nil
}

func storeSnapshot(dir string, snapshot raft.Snapshot) error {
	_, _, err := storeSnapshotStream(dir, snapshot, func(writer io.Writer) error {
		_, err := io.Copy(writer, bytes.NewReader(snapshot.Data))
		return err
	})
	return err
}

func storeSnapshotStream(dir string, snapshot raft.Snapshot, writeData func(io.Writer) error) (uint64, uint32, error) {
	membership, err := json.Marshal(snapshot.Membership)
	if err != nil {
		return 0, 0, fmt.Errorf("encode snapshot membership: %w", err)
	}
	tmp := filepath.Join(dir, ".SNAPSHOT.tmp")
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return 0, 0, fmt.Errorf("create snapshot temp: %w", err)
	}
	ok := false
	defer func() {
		if !ok {
			_ = f.Close()
			_ = os.Remove(tmp)
		}
	}()
	var header [snapshotHeader]byte
	binary.BigEndian.PutUint64(header[0:8], snapshot.Index)
	binary.BigEndian.PutUint64(header[8:16], snapshot.Term)
	if _, err := f.Write(header[:]); err != nil {
		return 0, 0, fmt.Errorf("write snapshot header: %w", err)
	}
	dataHash := crc32.NewIEEE()
	start, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, 0, fmt.Errorf("locate snapshot data: %w", err)
	}
	if err := writeData(&boundedSnapshotWriter{writer: io.MultiWriter(f, dataHash)}); err != nil {
		return 0, 0, fmt.Errorf("write snapshot data: %w", err)
	}
	end, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, 0, fmt.Errorf("measure snapshot data: %w", err)
	}
	dataSize := uint64(end - start)
	binary.BigEndian.PutUint64(header[16:24], dataSize)
	if _, err := f.WriteAt(header[:], 0); err != nil {
		return 0, 0, fmt.Errorf("finalize snapshot header: %w", err)
	}
	if _, err := f.Seek(end, io.SeekStart); err != nil {
		return 0, 0, fmt.Errorf("seek snapshot metadata: %w", err)
	}
	var membershipLength [4]byte
	binary.BigEndian.PutUint32(membershipLength[:], uint32(len(membership)))
	if _, err := f.Write(membershipLength[:]); err != nil {
		return 0, 0, fmt.Errorf("write snapshot membership length: %w", err)
	}
	if _, err := f.Write(membership); err != nil {
		return 0, 0, fmt.Errorf("write snapshot membership: %w", err)
	}
	checksumEnd, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, 0, fmt.Errorf("measure snapshot: %w", err)
	}
	wholeHash := crc32.NewIEEE()
	if _, err := io.CopyN(wholeHash, io.NewSectionReader(f, 0, checksumEnd), checksumEnd); err != nil {
		return 0, 0, fmt.Errorf("checksum snapshot: %w", err)
	}
	if _, err := f.Seek(checksumEnd, io.SeekStart); err != nil {
		return 0, 0, fmt.Errorf("seek snapshot checksum: %w", err)
	}
	var checksum [4]byte
	binary.BigEndian.PutUint32(checksum[:], wholeHash.Sum32())
	if _, err := f.Write(checksum[:]); err != nil {
		return 0, 0, fmt.Errorf("write snapshot checksum: %w", err)
	}
	if err := f.Sync(); err != nil {
		return 0, 0, fmt.Errorf("sync snapshot: %w", err)
	}
	if err := f.Close(); err != nil {
		return 0, 0, fmt.Errorf("close snapshot: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, snapshotFile)); err != nil {
		return 0, 0, fmt.Errorf("publish snapshot: %w", err)
	}
	ok = true
	if err := syncDir(dir); err != nil {
		return 0, 0, err
	}
	return dataSize, dataHash.Sum32(), nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func cloneSnapshot(snapshot raft.Snapshot) raft.Snapshot {
	snapshot.Data = append([]byte(nil), snapshot.Data...)
	return snapshot
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
