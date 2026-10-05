package tests

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"lsmdb/db"
	"lsmdb/internal/wal"
)

// sequenceOpts disables automatic flushes and compactions so tests control both.
func sequenceOpts() *db.Options {
	return &db.Options{FlushThreshold: 1000, CompactionThreshold: 100}
}

// writeFlushedOldValue stores k=old behind five filler writes (k=old gets seq 6),
// flushes it to an SSTable, and closes the database.
func writeFlushedOldValue(t *testing.T, dir string) {
	t.Helper()
	d, err := db.Open(dir, sequenceOpts())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := d.Set("filler", []byte{byte(i)}); err != nil {
			t.Fatalf("Set filler: %v", err)
		}
	}
	if err := d.Set("k", []byte("old")); err != nil {
		t.Fatalf("Set k: %v", err)
	}
	if err := d.ForceFlush(); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func readManifest(t *testing.T, dir string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "MANIFEST"))
	if err != nil {
		t.Fatalf("read MANIFEST: %v", err)
	}
	fields := map[string]any{}
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatalf("decode MANIFEST: %v", err)
	}
	return fields
}

// rewriteManifestAsVersionOne turns the published manifest into the format
// written before LastSeq existed.
func rewriteManifestAsVersionOne(t *testing.T, dir string) {
	t.Helper()
	fields := readManifest(t, dir)
	fields["version"] = 1
	delete(fields, "last_seq")
	data, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "MANIFEST"), data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func assertValue(t *testing.T, d *db.DB, stage, key, want string) {
	t.Helper()
	if got, ok := d.Get(key); !ok || string(got) != want {
		t.Fatalf("%s: Get(%q) = (%q, %v), want %q", stage, key, got, ok, want)
	}
	pairs, err := d.Scan(key, key)
	if err != nil {
		t.Fatalf("%s: Scan(%q): %v", stage, key, err)
	}
	if len(pairs) != 1 || string(pairs[0].Value) != want {
		t.Fatalf("%s: Scan(%q) = %+v, want %q", stage, key, pairs, want)
	}
}

func flushAndCompact(t *testing.T, d *db.DB) {
	t.Helper()
	if err := d.ForceFlush(); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}
	if err := d.ForceCompact(); err != nil {
		t.Fatalf("ForceCompact: %v", err)
	}
}

func TestSequenceSurvivesReopenAfterFlush(t *testing.T) {
	dir := t.TempDir()
	writeFlushedOldValue(t, dir)

	d, err := db.Open(dir, sequenceOpts())
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer d.Close()
	if err := d.Set("k", []byte("new")); err != nil {
		t.Fatalf("Set k=new: %v", err)
	}
	assertValue(t, d, "after reopen", "k", "new")
	flushAndCompact(t, d)
	assertValue(t, d, "after flush and compact", "k", "new")
}

func TestResurrectedWALAfterFlushIsIgnored(t *testing.T) {
	dir := t.TempDir()
	d, err := db.Open(dir, sequenceOpts())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := d.Set("k", []byte("old")); err != nil {
		t.Fatal(err)
	}
	// Keep the pre-flush WAL bytes to simulate a truncate that never became durable.
	walPath := filepath.Join(dir, "wal")
	preFlushWAL, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.ForceFlush(); err != nil {
		t.Fatal(err)
	}
	if err := d.Set("k", []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := d.ForceFlush(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(walPath, preFlushWAL, 0o644); err != nil {
		t.Fatal(err)
	}

	d, err = db.Open(dir, sequenceOpts())
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer d.Close()
	assertValue(t, d, "after reopen with resurrected WAL", "k", "new")
}

func TestLegacyManifestDerivesLastSeq(t *testing.T) {
	dir := t.TempDir()
	writeFlushedOldValue(t, dir)
	rewriteManifestAsVersionOne(t, dir)

	d, err := db.Open(dir, sequenceOpts())
	if err != nil {
		t.Fatalf("Open legacy: %v", err)
	}
	defer d.Close()
	fields := readManifest(t, dir)
	if fields["version"] != float64(2) || fields["last_seq"] != float64(6) {
		t.Fatalf("upgraded manifest version=%v last_seq=%v, want 2 and 6", fields["version"], fields["last_seq"])
	}
	if err := d.Set("k", []byte("new")); err != nil {
		t.Fatal(err)
	}
	assertValue(t, d, "after legacy open", "k", "new")
	flushAndCompact(t, d)
	assertValue(t, d, "after flush and compact", "k", "new")
}

func TestLegacyWALBelowLastSeqIsRenumbered(t *testing.T) {
	dir := t.TempDir()
	writeFlushedOldValue(t, dir)
	rewriteManifestAsVersionOne(t, dir)
	// A database already hit by sequence reuse: acknowledged WAL writes carry
	// sequence numbers below the SSTable that holds k=old at seq 6.
	log, err := wal.Open(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.ReadAll(); err != nil {
		t.Fatal(err)
	}
	for i, record := range []wal.Record{
		{Type: wal.TypePut, SeqNum: 1, Key: "k", Value: []byte("new")},
		{Type: wal.TypePut, SeqNum: 2, Key: "fresh", Value: []byte("kept")},
	} {
		if err := log.Append(record); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	d, err := db.Open(dir, sequenceOpts())
	if err != nil {
		t.Fatalf("Open legacy: %v", err)
	}
	defer d.Close()
	assertValue(t, d, "after legacy open", "k", "new")
	assertValue(t, d, "after legacy open", "fresh", "kept")
	flushAndCompact(t, d)
	assertValue(t, d, "after flush and compact", "k", "new")
	assertValue(t, d, "after flush and compact", "fresh", "kept")
}
