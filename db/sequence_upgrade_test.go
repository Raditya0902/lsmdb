package db

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"lsmdb/internal/manifest"
	"lsmdb/internal/wal"
)

var errSimulatedCrash = errors.New("simulated crash")

// legacyDatabaseWithReusedSequences builds a version-1 database whose WAL holds
// acknowledged writes numbered below the SSTable that stores k=old at seq 6.
func legacyDatabaseWithReusedSequences(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	d, err := Open(dir, &Options{FlushThreshold: 1000, CompactionThreshold: 100})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := d.Set("filler", []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Set("k", []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := d.ForceFlush(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	state, _, err := manifest.Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := json.Marshal(map[string]any{"version": 1, "sstables": state.SSTables, "next_sst": state.NextSST})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "MANIFEST"), legacy, 0o644); err != nil {
		t.Fatal(err)
	}
	log, err := wal.Open(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.ReadAll(); err != nil {
		t.Fatal(err)
	}
	for _, record := range []wal.Record{
		{Type: wal.TypePut, SeqNum: 1, Key: "k", Value: []byte("new")},
		{Type: wal.TypePut, SeqNum: 2, Key: "fresh", Value: []byte("kept")},
	} {
		if err := log.Append(record); err != nil {
			t.Fatal(err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

func crashAt(t *testing.T, target string) {
	t.Helper()
	crashPoint = func(step string) error {
		if step == target {
			return errSimulatedCrash
		}
		return nil
	}
	t.Cleanup(func() { crashPoint = nil })
}

func assertUpgradedValues(t *testing.T, d *DB) {
	t.Helper()
	for key, want := range map[string]string{"k": "new", "fresh": "kept"} {
		if got, ok := d.Get(key); !ok || string(got) != want {
			t.Fatalf("Get(%q) = (%q, %v), want %q", key, got, ok, want)
		}
		pairs, err := d.Scan(key, key)
		if err != nil || len(pairs) != 1 || string(pairs[0].Value) != want {
			t.Fatalf("Scan(%q) = (%+v, %v), want %q", key, pairs, err, want)
		}
	}
}

func TestLegacyUpgradeCrashBeforePublishReruns(t *testing.T) {
	dir := legacyDatabaseWithReusedSequences(t)
	crashAt(t, stepFlushBeforeManifest)
	if _, err := Open(dir, &Options{FlushThreshold: 1000, CompactionThreshold: 100}); !errors.Is(err, errSimulatedCrash) {
		t.Fatalf("Open with crash before publish = %v", err)
	}
	if state, _, err := manifest.Load(dir); err != nil || !state.Legacy() {
		t.Fatalf("manifest after crash before publish = (%#v, %v), want legacy", state, err)
	}
	crashPoint = nil

	d, err := Open(dir, &Options{FlushThreshold: 1000, CompactionThreshold: 100})
	if err != nil {
		t.Fatalf("rerun upgrade: %v", err)
	}
	defer d.Close()
	assertUpgradedValues(t, d)
	if err := d.ForceCompact(); err != nil {
		t.Fatal(err)
	}
	assertUpgradedValues(t, d)
}

func TestLegacyUpgradeCrashAfterPublishSkipsLeftoverWAL(t *testing.T) {
	dir := legacyDatabaseWithReusedSequences(t)
	crashAt(t, stepFlushAfterManifest)
	if _, err := Open(dir, &Options{FlushThreshold: 1000, CompactionThreshold: 100}); !errors.Is(err, errSimulatedCrash) {
		t.Fatalf("Open with crash after publish = %v", err)
	}
	state, _, err := manifest.Load(dir)
	if err != nil || state.Legacy() || state.LastSeq != 8 {
		t.Fatalf("manifest after crash after publish = (%#v, %v), want version 2 with last_seq 8", state, err)
	}
	crashPoint = nil

	d, err := Open(dir, &Options{FlushThreshold: 1000, CompactionThreshold: 100})
	if err != nil {
		t.Fatalf("reopen after publish: %v", err)
	}
	defer d.Close()
	assertUpgradedValues(t, d)
	// The leftover WAL still holds seqs 1 and 2; the version-2 rule must skip them,
	// leaving nothing for a flush to write.
	tables := d.SSTableCount()
	if err := d.ForceFlush(); err != nil {
		t.Fatal(err)
	}
	if got := d.SSTableCount(); got != tables {
		t.Fatalf("leftover WAL records were replayed: SSTable count %d -> %d", tables, got)
	}
	if err := d.ForceCompact(); err != nil {
		t.Fatal(err)
	}
	assertUpgradedValues(t, d)
}
