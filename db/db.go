// Package db exposes the public API for the LSM-tree key-value store.
package db

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"lsmdb/internal/compact"
	"lsmdb/internal/manifest"
	"lsmdb/internal/memtable"
	"lsmdb/internal/sstable"
	"lsmdb/internal/wal"
)

// KVPair is a live key-value pair returned by Scan.
type KVPair struct {
	Key   string
	Value []byte
}

// MutationType identifies an operation in an externally ordered batch.
type MutationType uint8

const (
	MutationPut MutationType = iota
	MutationDelete
)

// Mutation is one key change applied at an externally supplied sequence index.
type Mutation struct {
	Type  MutationType
	Key   string
	Value []byte
}

// DB is the handle to an open database instance.
type DB struct {
	mu           sync.RWMutex
	mem          *memtable.MemTable
	log          *wal.WAL          // nil for in-memory and replica modes
	path         string            // empty for in-memory DB
	readers      []*sstable.Reader // SSTable readers, newest first
	nextSST      uint64            // sequence number for the next SSTable file
	opts         *Options
	manifest     manifest.State
	appliedIndex uint64
	durableIndex uint64
	closed       bool
}

// Open creates or opens a database at path.
// If path is empty the database is in-memory only — no WAL, no persistence.
// When path is non-empty the WAL is replayed and existing SSTables are opened
// before Open returns, restoring the full persisted state.
func Open(path string, opts *Options) (*DB, error) {
	if opts == nil {
		opts = DefaultOptions()
	}
	mem := memtable.New()
	if path == "" {
		if opts.DurabilityMode == DurabilityReplica {
			return nil, fmt.Errorf("replica durability requires a persistent path")
		}
		return &DB{mem: mem, opts: opts}, nil
	}

	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, fmt.Errorf("create db dir: %w", err)
	}

	readers, manifestState, err := openSSTables(path)
	if err != nil {
		return nil, err
	}
	if opts.DurabilityMode == DurabilityReplica {
		return &DB{
			mem: mem, path: path, readers: readers, nextSST: manifestState.NextSST,
			opts: opts, manifest: manifestState, appliedIndex: manifestState.AppliedIndex,
			durableIndex: manifestState.AppliedIndex,
		}, nil
	}

	w, err := wal.Open(filepath.Join(path, "wal"))
	if err != nil {
		for _, r := range readers {
			r.Close() //nolint:errcheck
		}
		return nil, fmt.Errorf("open wal: %w", err)
	}

	records, err := w.ReadAll()
	if err != nil {
		w.Close() //nolint:errcheck
		for _, r := range readers {
			r.Close() //nolint:errcheck
		}
		return nil, fmt.Errorf("replay wal: %w", err)
	}

	for _, r := range records {
		// WAL types (1=PUT, 2=DELETE) map to memtable RecordType (0=PUT, 1=DELETE).
		mem.SetRaw(memtable.Record{
			Key:    r.Key,
			Value:  r.Value,
			SeqNum: r.SeqNum,
			Type:   memtable.RecordType(r.Type - 1),
		})
	}

	return &DB{
		mem:      mem,
		log:      w,
		path:     path,
		readers:  readers,
		nextSST:  manifestState.NextSST,
		opts:     opts,
		manifest: manifestState,
	}, nil
}

// Get returns the value stored under key.
// Returns (nil, false) when the key does not exist or has been deleted.
// The memtable is checked first; if not found, SSTables are searched newest-first.
func (db *DB) Get(key string) ([]byte, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// A memtable tombstone beats any older SSTable value.
	if r, ok := db.mem.GetRecord(key); ok {
		if r.Type == memtable.RecordTypeDelete {
			return nil, false
		}
		return r.Value, true
	}

	for _, reader := range db.readers {
		r, found, err := reader.Get(key)
		if err != nil || !found {
			continue
		}
		if r.Type == memtable.RecordTypeDelete {
			return nil, false
		}
		return r.Value, true
	}

	return nil, false
}

// Set stores value under key, replacing any previous value.
// The WAL record is written before the memtable is updated.
// Triggers a flush (and possibly compaction) if the memtable reaches the threshold.
func (db *DB) Set(key string, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.opts.DurabilityMode == DurabilityReplica {
		return errors.New("Set is unavailable in replica mode; use ApplyBatch")
	}

	seq := db.mem.AllocSeq()
	if db.log != nil {
		if err := db.log.Append(wal.Record{Type: wal.TypePut, SeqNum: seq, Key: key, Value: value}); err != nil {
			return fmt.Errorf("wal append: %w", err)
		}
	}
	db.mem.SetRaw(memtable.Record{Key: key, Value: value, SeqNum: seq, Type: memtable.RecordTypePut})

	return db.maybeFlush()
}

// Delete removes key from the database by writing a tombstone.
// The WAL record is written before the memtable is updated.
// Triggers a flush (and possibly compaction) if the memtable reaches the threshold.
func (db *DB) Delete(key string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.opts.DurabilityMode == DurabilityReplica {
		return errors.New("Delete is unavailable in replica mode; use ApplyBatch")
	}

	seq := db.mem.AllocSeq()
	if db.log != nil {
		if err := db.log.Append(wal.Record{Type: wal.TypeDelete, SeqNum: seq, Key: key}); err != nil {
			return fmt.Errorf("wal append: %w", err)
		}
	}
	db.mem.SetRaw(memtable.Record{Key: key, SeqNum: seq, Type: memtable.RecordTypeDelete})

	return db.maybeFlush()
}

// ApplyBatch atomically applies mutations at a committed external log index.
// Reapplying an already applied index is a no-op; gaps are rejected.
func (db *DB) ApplyBatch(index uint64, mutations []Mutation) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.opts.DurabilityMode != DurabilityReplica {
		return errors.New("ApplyBatch requires replica durability mode")
	}
	if index == 0 {
		return errors.New("apply index must be greater than zero")
	}
	if index <= db.appliedIndex {
		return nil
	}
	if index != db.appliedIndex+1 {
		return fmt.Errorf("apply index gap: got %d, want %d", index, db.appliedIndex+1)
	}
	for _, mutation := range mutations {
		if mutation.Key == "" {
			return errors.New("mutation key must not be empty")
		}
		if mutation.Type != MutationPut && mutation.Type != MutationDelete {
			return fmt.Errorf("unknown mutation type %d", mutation.Type)
		}
	}
	for _, mutation := range mutations {
		recordType := memtable.RecordTypePut
		if mutation.Type == MutationDelete {
			recordType = memtable.RecordTypeDelete
		}
		db.mem.SetRaw(memtable.Record{
			Key: mutation.Key, Value: append([]byte(nil), mutation.Value...),
			SeqNum: index, Type: recordType,
		})
	}
	db.appliedIndex = index
	return db.maybeFlush()
}

// AppliedIndex is the highest external index applied to the in-memory state.
func (db *DB) AppliedIndex() uint64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.appliedIndex
}

// DurableIndex is the highest external index published through the manifest.
func (db *DB) DurableIndex() uint64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.durableIndex
}

// Scan returns all live key-value pairs where from <= key <= to, sorted ascending.
// Tombstoned keys are excluded. Results merge the MemTable with all SSTables;
// the highest sequence number wins per key.
func (db *DB) Scan(from, to string) ([]KVPair, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	memRecs := db.mem.ScanRange(from, to)
	all := make([]memtable.Record, 0, len(memRecs))
	all = append(all, memRecs...)

	for _, r := range db.readers {
		recs, err := r.Scan(from, to)
		if err != nil {
			return nil, fmt.Errorf("scan sstable: %w", err)
		}
		all = append(all, recs...)
	}

	byKey := make(map[string]memtable.Record, len(all))
	for _, rec := range all {
		if existing, ok := byKey[rec.Key]; !ok || rec.SeqNum > existing.SeqNum {
			byKey[rec.Key] = rec
		}
	}

	pairs := make([]KVPair, 0, len(byKey))
	for key, rec := range byKey {
		if rec.Type == memtable.RecordTypePut {
			pairs = append(pairs, KVPair{Key: key, Value: rec.Value})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].Key < pairs[j].Key
	})
	return pairs, nil
}

// SSTableCount returns the number of SSTable files currently open.
// Intended for testing and diagnostics.
func (db *DB) SSTableCount() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.readers)
}

// BloomSkips returns the total number of times any SSTable's bloom filter
// returned false during a Get, saving a disk scan.
// Intended for testing and diagnostics.
func (db *DB) BloomSkips() int64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var total int64
	for _, r := range db.readers {
		total += r.BloomSkips()
	}
	return total
}

// ForceFlush flushes the current memtable to an SSTable regardless of the flush
// threshold. A no-op when the memtable is empty or the DB is in-memory mode.
// Intended for testing.
func (db *DB) ForceFlush() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.flush()
}

// ForceCompact compacts all current SSTables into one regardless of the
// compaction threshold. A no-op when there are no SSTables.
// Intended for testing.
func (db *DB) ForceCompact() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.compact()
}

// Close syncs the WAL to disk and releases all file descriptors.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	if db.opts.DurabilityMode == DurabilityReplica {
		if err := db.flush(); err != nil {
			return err
		}
	}
	db.closed = true

	var firstErr error
	for _, r := range db.readers {
		if err := r.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if db.log != nil {
		if err := db.log.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// ── internal write path ────────────────────────────────────────────────────

// maybeFlush flushes the memtable if the flush threshold is reached.
// Must be called with db.mu held.
func (db *DB) maybeFlush() error {
	if db.path == "" {
		return nil
	}
	if db.mem.Size() < db.opts.flushThreshold() {
		return nil
	}
	return db.flush()
}

// flush writes the current memtable to a new SSTable, resets the memtable,
// rotates the WAL, then triggers compaction if the threshold is met.
// Must be called with db.mu held.
func (db *DB) flush() error {
	records := db.mem.SortedEntries()
	if len(records) == 0 {
		if db.opts.DurabilityMode == DurabilityReplica && db.appliedIndex > db.durableIndex {
			state := db.manifest
			state.AppliedIndex = db.appliedIndex
			if err := manifest.Store(db.path, state); err != nil {
				return fmt.Errorf("publish applied index: %w", err)
			}
			db.manifest = state
			db.durableIndex = db.appliedIndex
		}
		return nil
	}
	if db.path == "" {
		return nil
	}

	db.nextSST++
	sstName := fmt.Sprintf("%06d.sst", db.nextSST)
	sstPath := filepath.Join(db.path, sstName)

	if err := sstable.Write(sstPath, records); err != nil {
		db.nextSST--
		return fmt.Errorf("flush sstable: %w", err)
	}

	r, err := sstable.Open(sstPath)
	if err != nil {
		db.nextSST--
		_ = os.Remove(sstPath)
		return fmt.Errorf("open flushed sstable: %w", err)
	}

	state := db.manifest
	state.SSTables = append(append([]string(nil), state.SSTables...), sstName)
	state.NextSST = db.nextSST
	if db.opts.DurabilityMode == DurabilityReplica {
		state.AppliedIndex = db.appliedIndex
	}
	if err := manifest.Store(db.path, state); err != nil {
		_ = r.Close()
		_ = os.Remove(sstPath)
		db.nextSST--
		return fmt.Errorf("publish flush manifest: %w", err)
	}

	db.readers = append([]*sstable.Reader{r}, db.readers...)
	db.mem = memtable.NewWithSeq(db.mem.NextSeq())
	db.manifest = state
	if db.opts.DurabilityMode == DurabilityReplica {
		db.durableIndex = db.appliedIndex
	}

	if db.log != nil {
		if err := db.log.Reset(); err != nil {
			return fmt.Errorf("rotate wal: %w", err)
		}
	}

	return db.maybeCompact()
}

// maybeCompact runs compaction if the SSTable count meets the threshold.
// Must be called with db.mu held.
func (db *DB) maybeCompact() error {
	if len(db.readers) < db.opts.compactionThreshold() {
		return nil
	}
	return db.compact()
}

// compact merges all current SSTables into a single new one.
// Because we always compact the full set, this is always a bottom-level
// compaction and tombstones are dropped.
// Must be called with db.mu held.
func (db *DB) compact() error {
	if len(db.readers) == 0 {
		return nil
	}

	db.nextSST++
	outPath := filepath.Join(db.path, fmt.Sprintf("%06d.sst", db.nextSST))

	// Collect stats and old file paths before we close the readers.
	filesIn := len(db.readers)
	recordsIn := int64(0)
	for _, r := range db.readers {
		recordsIn += r.RecordCount()
	}
	oldPaths := make([]string, filesIn)
	for i, r := range db.readers {
		oldPaths[i] = r.Path()
	}

	recordsOut, err := compact.Compact(db.readers, outPath, true)
	if err != nil {
		db.nextSST--
		return fmt.Errorf("compact: %w", err)
	}

	// Open the new reader only if the output file was actually created.
	var newReaders []*sstable.Reader
	var newNames []string
	if recordsOut > 0 {
		nr, err := sstable.Open(outPath)
		if err != nil {
			return fmt.Errorf("open compacted sstable: %w", err)
		}
		newReaders = []*sstable.Reader{nr}
		newNames = []string{filepath.Base(outPath)}
	}

	state := db.manifest
	state.SSTables = newNames
	state.NextSST = db.nextSST
	if err := manifest.Store(db.path, state); err != nil {
		for _, r := range newReaders {
			_ = r.Close()
		}
		if recordsOut > 0 {
			_ = os.Remove(outPath)
		}
		db.nextSST--
		return fmt.Errorf("publish compaction manifest: %w", err)
	}

	// Close old readers then delete their files.
	for _, r := range db.readers {
		r.Close() //nolint:errcheck
	}
	for _, p := range oldPaths {
		os.Remove(p) //nolint:errcheck
	}

	db.readers = newReaders
	db.manifest = state

	log.Printf("[compact] %d files, %d records in → %d records out (%s)",
		filesIn, recordsIn, recordsOut, filepath.Base(outPath))

	return nil
}

// ── startup helpers ────────────────────────────────────────────────────────

// openSSTables scans path for *.sst files, opens them newest-first, and returns
// the reader slice plus the sequence number of the newest file found.
func openSSTables(path string) ([]*sstable.Reader, manifest.State, error) {
	state, ok, err := manifest.Load(path)
	if err != nil {
		return nil, manifest.State{}, err
	}
	if !ok {
		entries, err := os.ReadDir(path)
		if err != nil {
			return nil, manifest.State{}, fmt.Errorf("read db dir: %w", err)
		}
		var names []string
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".sst") {
				names = append(names, e.Name())
			}
		}
		sort.Strings(names)
		state = manifest.New()
		state.SSTables = names
		if len(names) > 0 {
			_, _ = fmt.Sscanf(names[len(names)-1], "%d.sst", &state.NextSST)
		}
		if err := manifest.Store(path, state); err != nil {
			return nil, manifest.State{}, fmt.Errorf("bootstrap manifest: %w", err)
		}
	}
	names := state.SSTables

	readers := make([]*sstable.Reader, 0, len(names))
	for i := len(names) - 1; i >= 0; i-- {
		r, err := sstable.Open(filepath.Join(path, names[i]))
		if err != nil {
			for _, opened := range readers {
				opened.Close() //nolint:errcheck
			}
			return nil, manifest.State{}, fmt.Errorf("open sstable %s: %w", names[i], err)
		}
		readers = append(readers, r)
	}

	return readers, state, nil
}
