package manifest

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestStoreLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	want := State{
		Version:      version,
		SSTables:     []string{"000001.sst", "000003.sst"},
		NextSST:      3,
		AppliedIndex: 42,
		LastSeq:      17,
	}
	if err := Store(dir, want); err != nil {
		t.Fatalf("Store: %v", err)
	}
	got, ok, err := Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !ok {
		t.Fatal("Load reported no manifest")
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Load = %#v, want %#v", got, want)
	}
}

func TestLoadIgnoresUnpublishedTemporaryManifest(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ".MANIFEST.tmp"), []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, ok, err := Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if ok {
		t.Fatal("temporary manifest must not be treated as published")
	}
}

func TestStateRejectsUnsafeSSTableName(t *testing.T) {
	state := New()
	state.SSTables = []string{"../outside.sst"}
	if err := state.Validate(); err == nil {
		t.Fatal("Validate accepted path traversal")
	}
}

func TestLoadAcceptsVersionOneManifest(t *testing.T) {
	dir := t.TempDir()
	legacy := []byte(`{"version": 1, "sstables": ["000002.sst"], "next_sst": 2}`)
	if err := os.WriteFile(filepath.Join(dir, fileName), legacy, 0o644); err != nil {
		t.Fatal(err)
	}
	got, ok, err := Load(dir)
	if err != nil || !ok {
		t.Fatalf("Load = (%#v, %v, %v)", got, ok, err)
	}
	if !got.Legacy() || got.LastSeq != 0 || got.NextSST != 2 {
		t.Fatalf("legacy manifest loaded as %#v", got)
	}
}

func TestStoreWritesCurrentVersion(t *testing.T) {
	dir := t.TempDir()
	if err := Store(dir, State{Version: 1, SSTables: []string{"000001.sst"}, NextSST: 1, LastSeq: 9}); err != nil {
		t.Fatalf("Store: %v", err)
	}
	got, _, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got.Legacy() || got.Version != version || got.LastSeq != 9 {
		t.Fatalf("stored manifest = %#v", got)
	}
}

func TestLoadRejectsUnknownVersion(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, fileName), []byte(`{"version": 3}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Load(dir); err == nil {
		t.Fatal("Load accepted an unknown manifest version")
	}
}
