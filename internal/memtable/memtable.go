// Package memtable provides an in-memory key-value store with sequence numbers and tombstone support.
package memtable

import (
	"sort"
	"sync"
)

// RecordType distinguishes a live value from a deletion marker.
type RecordType uint8

const (
	RecordTypePut    RecordType = 0
	RecordTypeDelete RecordType = 1
)

// Record is a single versioned entry in the memtable.
type Record struct {
	Key    string
	Value  []byte
	SeqNum uint64
	Type   RecordType
}

// MemTable holds the latest record per key in memory.
// Concurrent access is safe via an internal RWMutex.
type MemTable struct {
	entries map[string]Record
	nextSeq uint64
	mu      sync.RWMutex
}

// New returns an empty MemTable with sequence numbers starting at 1.
func New() *MemTable {
	return &MemTable{
		entries: make(map[string]Record),
		nextSeq: 1,
	}
}

// NewWithSeq returns an empty MemTable whose sequence counter starts at startSeq.
// Used when resetting the memtable after an SSTable flush so sequence numbers
// remain monotonically increasing across flushes.
func NewWithSeq(startSeq uint64) *MemTable {
	return &MemTable{
		entries: make(map[string]Record),
		nextSeq: startSeq,
	}
}

// Set inserts or overwrites key with the given value.
func (m *MemTable) Set(key string, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries[key] = Record{
		Key:    key,
		Value:  value,
		SeqNum: m.nextSeq,
		Type:   RecordTypePut,
	}
	m.nextSeq++
}

// Get returns the value for key and true if the key exists and is not deleted.
// Returns nil, false for missing keys or tombstones.
func (m *MemTable) Get(key string) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.entries[key]
	if !ok || r.Type == RecordTypeDelete {
		return nil, false
	}
	return r.Value, true
}

// Delete writes a tombstone for key, marking it as deleted.
func (m *MemTable) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries[key] = Record{
		Key:    key,
		SeqNum: m.nextSeq,
		Type:   RecordTypeDelete,
	}
	m.nextSeq++
}

// AllocSeq atomically reserves the next sequence number and advances the counter.
// Used by the WAL write path so the seqnum is stamped before the WAL record is written.
func (m *MemTable) AllocSeq() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	seq := m.nextSeq
	m.nextSeq++
	return seq
}

// SetRaw writes r directly into the table using its pre-assigned SeqNum.
// Newer SeqNum always wins. Updates nextSeq if r.SeqNum is ahead of it.
// Used during WAL replay and by the WAL-backed write path in db.go.
func (m *MemTable) SetRaw(r Record) {
	m.mu.Lock()
	defer m.mu.Unlock()
	existing, ok := m.entries[r.Key]
	if !ok || r.SeqNum > existing.SeqNum {
		m.entries[r.Key] = r
	}
	if r.SeqNum >= m.nextSeq {
		m.nextSeq = r.SeqNum + 1
	}
}

// Size returns the number of entries in the memtable, including tombstones.
func (m *MemTable) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.entries)
}

// NextSeq returns the next sequence number that will be assigned.
// Used when resetting the memtable after a flush to preserve the counter.
func (m *MemTable) NextSeq() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.nextSeq
}

// GetRecord returns the raw Record for key, including tombstones.
// Returns (Record{}, false) when the key is not present at all.
// Unlike Get, a tombstone record is returned with ok=true so callers can
// distinguish "not in memtable" from "deleted in memtable".
func (m *MemTable) GetRecord(key string) (Record, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.entries[key]
	return r, ok
}

// ScanRange returns all records where from <= key <= to, sorted ascending by key.
// Includes tombstones — callers must filter RecordTypeDelete if needed.
func (m *MemTable) ScanRange(from, to string) []Record {
	all := m.SortedEntries()
	start := sort.Search(len(all), func(i int) bool { return all[i].Key >= from })
	var out []Record
	for _, r := range all[start:] {
		if r.Key > to {
			break
		}
		out = append(out, r)
	}
	return out
}

// SortedEntries returns all records sorted ascending by key.
// Used by the SSTable writer to flush a sorted run to disk.
func (m *MemTable) SortedEntries() []Record {
	m.mu.RLock()
	defer m.mu.RUnlock()
	records := make([]Record, 0, len(m.entries))
	for _, r := range m.entries {
		records = append(records, r)
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].Key < records[j].Key
	})
	return records
}
