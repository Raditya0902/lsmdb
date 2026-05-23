package tests

import (
	"fmt"
	"testing"

	"lsmdb/db"
)

// smallFlushOpts returns Options with a low threshold to trigger flushes in tests.
func smallFlushOpts() *db.Options {
	return &db.Options{FlushThreshold: 5}
}

func TestPersistenceFlushAndReopen(t *testing.T) {
	dir := t.TempDir()

	d1, err := db.Open(dir, smallFlushOpts())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Write 12 keys — with threshold=5 this triggers at least 2 flushes.
	for i := 0; i < 12; i++ {
		key := fmt.Sprintf("key%02d", i)
		val := fmt.Sprintf("val%02d", i)
		if err := d1.Set(key, []byte(val)); err != nil {
			t.Fatalf("Set %s: %v", key, err)
		}
	}

	// Overwrite key00 — the old version is already in an SSTable.
	if err := d1.Set("key00", []byte("updated")); err != nil {
		t.Fatalf("overwrite key00: %v", err)
	}

	// Delete key05 — its original value is in an SSTable.
	if err := d1.Delete("key05"); err != nil {
		t.Fatalf("delete key05: %v", err)
	}

	if err := d1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	d2, err := db.Open(dir, smallFlushOpts())
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer d2.Close()

	// Overwritten key returns the latest value.
	if v, ok := d2.Get("key00"); !ok || string(v) != "updated" {
		t.Errorf("key00: got (%q, %v), want (\"updated\", true)", v, ok)
	}

	// Deleted key returns not-found.
	if _, ok := d2.Get("key05"); ok {
		t.Error("key05 should be deleted")
	}

	// All other keys return their original values.
	for i := 1; i < 12; i++ {
		if i == 5 {
			continue
		}
		key := fmt.Sprintf("key%02d", i)
		want := fmt.Sprintf("val%02d", i)
		if v, ok := d2.Get(key); !ok || string(v) != want {
			t.Errorf("%s: got (%q, %v), want (%q, true)", key, v, ok, want)
		}
	}
}

func TestPersistenceDeleteFlushed(t *testing.T) {
	dir := t.TempDir()

	d1, err := db.Open(dir, smallFlushOpts())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Write enough to flush, then write a tombstone for a key that is in the SSTable.
	keys := []struct {
		key string
		val string
	}{
		{"a", "1"}, {"b", "2"}, {"c", "3"}, {"d", "4"}, {"e", "5"}, // flush here
		{"f", "6"},
	}
	for _, kv := range keys {
		if err := d1.Set(kv.key, []byte(kv.val)); err != nil {
			t.Fatalf("Set %s: %v", kv.key, err)
		}
	}
	// Delete a key that landed in the first SSTable.
	if err := d1.Delete("b"); err != nil {
		t.Fatalf("Delete b: %v", err)
	}

	if err := d1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	d2, err := db.Open(dir, smallFlushOpts())
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer d2.Close()

	if _, ok := d2.Get("b"); ok {
		t.Error("b should be deleted after reopen")
	}
	if v, ok := d2.Get("a"); !ok || string(v) != "1" {
		t.Errorf("a: got (%q, %v), want (\"1\", true)", v, ok)
	}
	if v, ok := d2.Get("f"); !ok || string(v) != "6" {
		t.Errorf("f: got (%q, %v), want (\"6\", true)", v, ok)
	}
}

func TestPersistenceOverwriteAcrossFlush(t *testing.T) {
	dir := t.TempDir()

	d1, err := db.Open(dir, smallFlushOpts())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Flush "x" with value "old", then overwrite it in the next memtable batch.
	for i := 0; i < 4; i++ {
		d1.Set(fmt.Sprintf("filler%d", i), []byte("v")) //nolint:errcheck
	}
	d1.Set("x", []byte("old")) //nolint:errcheck  — this triggers flush (5 entries)
	d1.Set("x", []byte("new")) //nolint:errcheck  — goes into fresh memtable

	if err := d1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	d2, err := db.Open(dir, smallFlushOpts())
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer d2.Close()

	// The in-memory (WAL-replayed) record must win over the SSTable record.
	if v, ok := d2.Get("x"); !ok || string(v) != "new" {
		t.Errorf("x: got (%q, %v), want (\"new\", true)", v, ok)
	}
}
