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
