package tests

import (
	"fmt"
	"testing"

	"lsmdb/db"
)

// compactOpts returns Options that flush every 2 writes and compact every 2 SSTables,
// making it easy to trigger both operations with a small number of writes.
func compactOpts() *db.Options {
	return &db.Options{FlushThreshold: 2, CompactionThreshold: 2}
}

// writeFlushPair writes two key-value pairs so the memtable hits the flush threshold.
func writeFlushPair(t *testing.T, d *db.DB, key, val, padKey, padVal string) {
	t.Helper()
	if err := d.Set(key, []byte(val)); err != nil {
		t.Fatalf("Set %s: %v", key, err)
	}
	if err := d.Set(padKey, []byte(padVal)); err != nil {
		t.Fatalf("Set %s: %v", padKey, err)
	}
}

func TestCompactionOldValueRemoved(t *testing.T) {
	dir := t.TempDir()
	// CompactionThreshold=100 so we control compaction manually.
	opts := &db.Options{FlushThreshold: 2, CompactionThreshold: 100}

	d, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Flush 1: key=v1 lands in SSTable #1.
	writeFlushPair(t, d, "key", "v1", "pad1", "x")

	// Flush 2: key=v2 lands in SSTable #2.
	writeFlushPair(t, d, "key", "v2", "pad2", "x")

	if d.SSTableCount() != 2 {
		t.Fatalf("expected 2 SSTables before compaction, got %d", d.SSTableCount())
	}

	if err := d.ForceCompact(); err != nil {
		t.Fatalf("ForceCompact: %v", err)
	}

	if d.SSTableCount() != 1 {
		t.Errorf("expected 1 SSTable after compaction, got %d", d.SSTableCount())
	}

	// Only the latest value must survive.
	if v, ok := d.Get("key"); !ok || string(v) != "v2" {
		t.Errorf("key: got (%q, %v), want (\"v2\", true)", v, ok)
	}

	d.Close()
}

func TestCompactionNewestValueSurvivesMultipleFlushes(t *testing.T) {
	dir := t.TempDir()
	opts := &db.Options{FlushThreshold: 2, CompactionThreshold: 100}

	d, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Write key=v1..v4 across 4 flushes.
	for i := 1; i <= 4; i++ {
		writeFlushPair(t, d, "key", fmt.Sprintf("v%d", i), fmt.Sprintf("pad%d", i), "x")
	}

	if err := d.ForceCompact(); err != nil {
		t.Fatalf("ForceCompact: %v", err)
	}

	if v, ok := d.Get("key"); !ok || string(v) != "v4" {
		t.Errorf("key: got (%q, %v), want (\"v4\", true)", v, ok)
	}
	if d.SSTableCount() != 1 {
		t.Errorf("expected 1 SSTable, got %d", d.SSTableCount())
	}

	d.Close()
}

func TestCompactionTombstoneHidesOldValue(t *testing.T) {
	dir := t.TempDir()
	opts := &db.Options{FlushThreshold: 2, CompactionThreshold: 100}

	d, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Flush 1: write key=v1.
	writeFlushPair(t, d, "key", "v1", "pad1", "x")

	// Flush 2: delete key.
	if err := d.Delete("key"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := d.Set("pad2", []byte("x")); err != nil {
		t.Fatalf("Set pad2: %v", err)
	}

	if err := d.ForceCompact(); err != nil {
		t.Fatalf("ForceCompact: %v", err)
	}

	// After bottom-level compaction the tombstone is dropped — key is gone.
	if _, ok := d.Get("key"); ok {
		t.Error("key should not exist after compaction dropped its tombstone")
	}

	d.Close()
}

func TestCompactionDeletedKeyDoesNotReappear(t *testing.T) {
	dir := t.TempDir()
	// Use automatic compaction (threshold=2) for this test.
	opts := compactOpts()

	d, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	// Flush 1: write key=v1 → Flush 2: delete key → compaction fires.
	writeFlushPair(t, d, "key", "v1", "pad1", "x")
	if err := d.Delete("key"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := d.Set("pad2", []byte("x")); err != nil {
		t.Fatalf("Set pad2: %v", err)
	}
	// At this point 2 SSTables exist → auto-compaction fires → 1 SSTable (key gone).

	// Write unrelated data, triggering more flushes and a second compaction.
	writeFlushPair(t, d, "other1", "a", "other2", "b")
	// 2 SSTables → another auto-compaction.

	// The deleted key must never reappear.
	if _, ok := d.Get("key"); ok {
		t.Error("deleted key reappeared after second compaction")
	}

	// Unrelated data must still be accessible.
	if v, ok := d.Get("other1"); !ok || string(v) != "a" {
		t.Errorf("other1: got (%q, %v), want (\"a\", true)", v, ok)
	}
}

func TestCompactionReducesSSTableCount(t *testing.T) {
	dir := t.TempDir()
	// Threshold=4: compaction fires when 4 SSTables accumulate.
	opts := &db.Options{FlushThreshold: 2, CompactionThreshold: 4}

	d, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	// 4 flushes → triggers automatic compaction.
	for i := 0; i < 4; i++ {
		writeFlushPair(t, d, fmt.Sprintf("k%d", i), "v", fmt.Sprintf("p%d", i), "x")
	}

	// After the 4th flush maybeCompact fires and reduces to 1.
	if d.SSTableCount() != 1 {
		t.Errorf("expected 1 SSTable after auto-compaction, got %d", d.SSTableCount())
	}

	// All keys must still be accessible.
	for i := 0; i < 4; i++ {
		k := fmt.Sprintf("k%d", i)
		if _, ok := d.Get(k); !ok {
			t.Errorf("key %q missing after compaction", k)
		}
	}
}

func TestCompactionPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	opts := &db.Options{FlushThreshold: 2, CompactionThreshold: 100}

	d, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	writeFlushPair(t, d, "key", "v1", "pad1", "x")
	writeFlushPair(t, d, "key", "v2", "pad2", "x")

	if err := d.ForceCompact(); err != nil {
		t.Fatalf("ForceCompact: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify the compacted state is restored.
	d2, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer d2.Close()

	if v, ok := d2.Get("key"); !ok || string(v) != "v2" {
		t.Errorf("after reopen: key got (%q, %v), want (\"v2\", true)", v, ok)
	}
	if d2.SSTableCount() != 1 {
		t.Errorf("expected 1 SSTable on reopen, got %d", d2.SSTableCount())
	}
}
