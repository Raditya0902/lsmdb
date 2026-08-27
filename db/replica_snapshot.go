package db

import (
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"lsmdb/internal/manifest"
	"lsmdb/internal/memtable"
	"lsmdb/internal/sstable"
)

var replicaSnapshotMagic = [8]byte{'L', 'S', 'M', 'S', 'N', 'A', 'P', 1}

const maxReplicaSnapshotField = 64 << 20

type recordSource interface {
	Next() (memtable.Record, bool, error)
}

type sliceRecordSource struct {
	records []memtable.Record
	index   int
}

func (s *sliceRecordSource) Next() (memtable.Record, bool, error) {
	if s.index >= len(s.records) {
		return memtable.Record{}, false, nil
	}
	record := s.records[s.index]
	s.index++
	return record, true, nil
}

type iteratorRecordSource struct{ iterator *sstable.Iterator }

func (s *iteratorRecordSource) Next() (memtable.Record, bool, error) {
	return s.iterator.Next()
}

type snapshotHeapEntry struct {
	record memtable.Record
	source int
}

type snapshotHeap []snapshotHeapEntry

func (h snapshotHeap) Len() int { return len(h) }
func (h snapshotHeap) Less(i, j int) bool {
	if h[i].record.Key != h[j].record.Key {
		return h[i].record.Key < h[j].record.Key
	}
	return h[i].record.SeqNum > h[j].record.SeqNum
}
func (h snapshotHeap) Swap(i, j int)   { h[i], h[j] = h[j], h[i] }
func (h *snapshotHeap) Push(value any) { *h = append(*h, value.(snapshotHeapEntry)) }
func (h *snapshotHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}

// WriteReplicaSnapshot writes a consistent logical replica image without
// retaining all live values in memory.
func (db *DB) WriteReplicaSnapshot(writer io.Writer) (uint64, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.opts.DurabilityMode != DurabilityReplica {
		return 0, errors.New("replica state export requires replica durability mode")
	}
	if db.appliedIndex == 0 {
		return 0, errors.New("cannot snapshot an unapplied replica")
	}
	var count uint64
	if err := db.visitReplicaStateLocked(func(memtable.Record) error {
		count++
		return nil
	}); err != nil {
		return 0, err
	}
	if _, err := writer.Write(replicaSnapshotMagic[:]); err != nil {
		return 0, err
	}
	if err := binary.Write(writer, binary.BigEndian, count); err != nil {
		return 0, err
	}
	err := db.visitReplicaStateLocked(func(record memtable.Record) error {
		key := []byte(record.Key)
		if uint64(len(key)) > uint64(^uint32(0)) || uint64(len(record.Value)) > uint64(^uint32(0)) {
			return errors.New("snapshot record is too large")
		}
		if err := binary.Write(writer, binary.BigEndian, uint32(len(key))); err != nil {
			return err
		}
		if err := binary.Write(writer, binary.BigEndian, uint32(len(record.Value))); err != nil {
			return err
		}
		if _, err := writer.Write(key); err != nil {
			return err
		}
		_, err := writer.Write(record.Value)
		return err
	})
	return db.appliedIndex, err
}

// ReplaceReplicaSnapshot validates and atomically publishes a streamed logical image.
func (db *DB) ReplaceReplicaSnapshot(index uint64, size uint64, reader io.Reader) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.opts.DurabilityMode != DurabilityReplica {
		return errors.New("replica state replacement requires replica durability mode")
	}
	if index == 0 {
		return errors.New("snapshot index must be positive")
	}
	if index < db.appliedIndex {
		return fmt.Errorf("snapshot index %d is older than applied index %d", index, db.appliedIndex)
	}
	if size < uint64(len(replicaSnapshotMagic))+8 {
		return errors.New("state snapshot is truncated")
	}
	var magic [8]byte
	if _, err := io.ReadFull(reader, magic[:]); err != nil || magic != replicaSnapshotMagic {
		return errors.New("invalid state snapshot magic")
	}
	var count uint64
	if err := binary.Read(reader, binary.BigEndian, &count); err != nil {
		return fmt.Errorf("decode snapshot count: %w", err)
	}
	if count > (size-uint64(len(replicaSnapshotMagic))-8)/8 || count > uint64(^uint(0)>>1) || count > uint64(^uint64(0)>>1) {
		return errors.New("invalid state snapshot record count")
	}

	nextSST := db.nextSST + 1
	var newPath string
	var stream *sstable.StreamWriter
	if count > 0 {
		newPath = filepath.Join(db.path, fmt.Sprintf("%06d.sst", nextSST))
		var err error
		stream, err = sstable.NewStreamWriter(newPath, int64(count))
		if err != nil {
			return fmt.Errorf("create snapshot sstable: %w", err)
		}
		defer func() {
			if stream != nil {
				stream.Abort()
			}
		}()
	}
	var previous string
	for i := uint64(0); i < count; i++ {
		var keyLen, valueLen uint32
		if err := binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
			return fmt.Errorf("decode snapshot key length: %w", err)
		}
		if err := binary.Read(reader, binary.BigEndian, &valueLen); err != nil {
			return fmt.Errorf("decode snapshot value length: %w", err)
		}
		if keyLen == 0 || keyLen > maxReplicaSnapshotField || valueLen > maxReplicaSnapshotField {
			return errors.New("invalid state snapshot record size")
		}
		key := make([]byte, keyLen)
		value := make([]byte, valueLen)
		if _, err := io.ReadFull(reader, key); err != nil {
			return errors.New("truncated state snapshot record")
		}
		if _, err := io.ReadFull(reader, value); err != nil {
			return errors.New("truncated state snapshot record")
		}
		if string(key) <= previous {
			return errors.New("snapshot keys must be non-empty and strictly sorted")
		}
		previous = string(key)
		if err := stream.Add(memtable.Record{Key: previous, Value: value, SeqNum: index, Type: memtable.RecordTypePut}); err != nil {
			return fmt.Errorf("write snapshot sstable: %w", err)
		}
	}
	var trailing [1]byte
	if n, err := io.ReadFull(reader, trailing[:]); n != 0 || !errors.Is(err, io.EOF) {
		return errors.New("state snapshot has trailing data")
	}
	if stream != nil {
		if err := stream.Finish(); err != nil {
			return fmt.Errorf("finish snapshot sstable: %w", err)
		}
		stream = nil
	}

	var newReaders []*sstable.Reader
	var newNames []string
	if newPath != "" {
		newReader, err := sstable.Open(newPath)
		if err != nil {
			_ = os.Remove(newPath)
			return fmt.Errorf("open snapshot sstable: %w", err)
		}
		newReaders = []*sstable.Reader{newReader}
		newNames = []string{filepath.Base(newPath)}
	}
	state := db.manifest
	state.SSTables = newNames
	state.NextSST = nextSST
	state.AppliedIndex = index
	if err := manifest.Store(db.path, state); err != nil {
		for _, newReader := range newReaders {
			_ = newReader.Close()
		}
		if newPath != "" {
			_ = os.Remove(newPath)
		}
		return fmt.Errorf("publish snapshot manifest: %w", err)
	}
	oldReaders := db.readers
	db.readers = newReaders
	db.mem = memtable.NewWithSeq(index + 1)
	db.nextSST = nextSST
	db.manifest = state
	db.appliedIndex = index
	db.durableIndex = index
	for _, oldReader := range oldReaders {
		oldPath := oldReader.Path()
		_ = oldReader.Close()
		if oldPath != newPath {
			_ = os.Remove(oldPath)
		}
	}
	return nil
}

func (db *DB) visitReplicaStateLocked(visit func(memtable.Record) error) error {
	sources := make([]recordSource, 0, len(db.readers)+1)
	sources = append(sources, &sliceRecordSource{records: db.mem.SortedEntries()})
	for _, reader := range db.readers {
		sources = append(sources, &iteratorRecordSource{iterator: reader.Iterator()})
	}
	queue := &snapshotHeap{}
	heap.Init(queue)
	for source, records := range sources {
		record, ok, err := records.Next()
		if err != nil {
			return err
		}
		if ok {
			heap.Push(queue, snapshotHeapEntry{record: record, source: source})
		}
	}
	advance := func(entry snapshotHeapEntry) error {
		record, ok, err := sources[entry.source].Next()
		if err != nil {
			return err
		}
		if ok {
			heap.Push(queue, snapshotHeapEntry{record: record, source: entry.source})
		}
		return nil
	}
	for queue.Len() > 0 {
		winner := heap.Pop(queue).(snapshotHeapEntry)
		if err := advance(winner); err != nil {
			return err
		}
		for queue.Len() > 0 && (*queue)[0].record.Key == winner.record.Key {
			duplicate := heap.Pop(queue).(snapshotHeapEntry)
			if err := advance(duplicate); err != nil {
				return err
			}
		}
		if winner.record.Type == memtable.RecordTypePut {
			if err := visit(winner.record); err != nil {
				return err
			}
		}
	}
	return nil
}
