package tests

import (
	"os"
	"path/filepath"
	"testing"

	"lsmdb/db"
)

func TestRecoveryBasic(t *testing.T) {
	dir := t.TempDir()

	d1, err := db.Open(dir, db.DefaultOptions())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := d1.Set("a", []byte("1")); err != nil {
		t.Fatalf("Set a: %v", err)
	}
	if err := d1.Set("b", []byte("2")); err != nil {
		t.Fatalf("Set b: %v", err)
	}
	if err := d1.Delete("b"); err != nil {
		t.Fatalf("Delete b: %v", err)
	}
	if err := d1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	d2, err := db.Open(dir, db.DefaultOptions())
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer d2.Close()

	if v, ok := d2.Get("a"); !ok || string(v) != "1" {
		t.Errorf("Get(a) = (%q, %v), want (\"1\", true)", v, ok)
	}
	if _, ok := d2.Get("b"); ok {
		t.Error("Get(b) should be (nil, false) — key was deleted before close")
	}
}

func TestRecoveryCrash(t *testing.T) {
	dir := t.TempDir()

	d1, err := db.Open(dir, db.DefaultOptions())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := d1.Set("x", []byte("foo")); err != nil {
		t.Fatalf("Set x: %v", err)
	}
	if err := d1.Set("y", []byte("bar")); err != nil {
		t.Fatalf("Set y: %v", err)
	}
	// Simulate crash: omit Close. Each Set already issued a write() syscall,
	// so the WAL records are in the kernel page cache and survive the "crash".

	d2, err := db.Open(dir, db.DefaultOptions())
	if err != nil {
		t.Fatalf("Reopen after crash: %v", err)
	}
	defer d2.Close()

	if v, ok := d2.Get("x"); !ok || string(v) != "foo" {
		t.Errorf("Get(x) = (%q, %v), want (\"foo\", true)", v, ok)
	}
	if v, ok := d2.Get("y"); !ok || string(v) != "bar" {
		t.Errorf("Get(y) = (%q, %v), want (\"bar\", true)", v, ok)
	}
}

func TestRecoveryCorruptedWAL(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	d1, err := db.Open(dir, db.DefaultOptions())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := d1.Set("k1", []byte("v1")); err != nil {
		t.Fatalf("Set k1: %v", err)
	}
	if err := d1.Set("k2", []byte("v2")); err != nil {
		t.Fatalf("Set k2: %v", err)
	}
	if err := d1.Set("k3", []byte("v3")); err != nil {
		t.Fatalf("Set k3: %v", err)
	}
	if err := d1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Truncate the last 5 bytes to corrupt the final record's CRC / body.
	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("Stat WAL: %v", err)
	}
	if info.Size() < 5 {
		t.Fatalf("WAL too small to truncate meaningfully (%d bytes)", info.Size())
	}
	if err := os.Truncate(walPath, info.Size()-5); err != nil {
		t.Fatalf("Truncate WAL: %v", err)
	}

	// Reopen must succeed (no crash) and recover the uncorrupted records.
	d2, err := db.Open(dir, db.DefaultOptions())
	if err != nil {
		t.Fatalf("Open with corrupted WAL: %v", err)
	}
	defer d2.Close()

	if _, ok := d2.Get("k1"); !ok {
		t.Error("k1 should survive WAL truncation")
	}
	if _, ok := d2.Get("k2"); !ok {
		t.Error("k2 should survive WAL truncation")
	}
	// k3 may be lost — its record was the one truncated. No assertion either way.
}
