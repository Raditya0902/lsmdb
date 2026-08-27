package raftstore

import (
	"os"
	"path/filepath"
	"testing"

	"lsmdb/internal/raft"
)

func TestStorePersistsHardStateAndLog(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	hard := raft.HardState{Term: 3, VotedFor: 2}
	if err := store.Persist(raft.Update{
		HardState: &hard,
		Entries: []raft.Entry{
			{Index: 1, Term: 2, Data: []byte("one")},
			{Index: 2, Term: 3, Data: []byte("two")},
		},
	}); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	gotHard, entries := reopened.Load()
	if gotHard != hard {
		t.Fatalf("hard state = %+v, want %+v", gotHard, hard)
	}
	if len(entries) != 2 || string(entries[1].Data) != "two" {
		t.Fatalf("entries = %#v", entries)
	}
}

func TestStoreTruncatesAndReplacesSuffix(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Persist(raft.Update{Entries: []raft.Entry{
		{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(raft.Update{
		TruncateFrom: 2,
		Entries:      []raft.Entry{{Index: 2, Term: 2}, {Index: 3, Term: 2}},
	}); err != nil {
		t.Fatal(err)
	}
	_, entries := store.Load()
	if len(entries) != 3 || entries[1].Term != 2 || entries[2].Term != 2 {
		t.Fatalf("entries after replacement = %#v", entries)
	}
}

func TestStoreDropsTruncatedTailOnRecovery(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(raft.Update{Entries: []raft.Entry{
		{Index: 1, Term: 1, Data: []byte("safe")},
		{Index: 2, Term: 1, Data: []byte("truncate-me")},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, logFile)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, info.Size()-3); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	_, entries := reopened.Load()
	if len(entries) != 1 || string(entries[0].Data) != "safe" {
		t.Fatalf("recovered entries = %#v", entries)
	}
	if err := reopened.Persist(raft.Update{Entries: []raft.Entry{{Index: 2, Term: 2, Data: []byte("replacement")}}}); err != nil {
		t.Fatalf("append after recovery: %v", err)
	}
}
