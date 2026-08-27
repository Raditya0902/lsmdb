package raftstore

import (
	"bytes"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"

	"lsmdb/internal/raft"
)

func TestStoreStreamsSnapshotDataWithoutKeepingItInMetadata(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	block := bytes.Repeat([]byte("streamed-snapshot"), 64*1024)
	wantSize := uint64(len(block) * 3)
	snapshot := raft.Snapshot{Index: 1, Term: 1, Membership: raft.Membership{Voters: []uint64{1}, Index: 1}}
	if err := store.PersistSnapshot(raft.Update{Snapshot: &snapshot}, func(writer io.Writer) error {
		for i := 0; i < 3; i++ {
			if _, err := writer.Write(block); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if metadata := store.SnapshotMetadata(); metadata.Index != 1 || len(metadata.Data) != 0 {
		t.Fatalf("snapshot metadata = %#v", metadata)
	}
	reader, size, checksum, err := store.OpenSnapshot(1)
	if err != nil {
		t.Fatal(err)
	}
	hash := crc32.NewIEEE()
	read, err := io.Copy(hash, reader)
	if closeErr := reader.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	if uint64(read) != wantSize || size != wantSize || checksum != hash.Sum32() {
		t.Fatalf("stream = read %d size %d checksum %08x/%08x", read, size, checksum, hash.Sum32())
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if metadata := reopened.SnapshotMetadata(); metadata.Index != 1 || len(metadata.Data) != 0 {
		t.Fatalf("reopened metadata = %#v", metadata)
	}
}

func TestFailedStreamedSnapshotDoesNotPublishOrCompactLog(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	entries := []raft.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}
	if err := store.Persist(raft.Update{Entries: entries}); err != nil {
		t.Fatal(err)
	}
	snapshot := raft.Snapshot{Index: 1, Term: 1}
	wantErr := errors.New("injected snapshot failure")
	err = store.PersistSnapshot(raft.Update{Snapshot: &snapshot}, func(writer io.Writer) error {
		_, _ = writer.Write([]byte("partial"))
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("PersistSnapshot error = %v, want %v", err, wantErr)
	}
	if metadata := store.SnapshotMetadata(); metadata.Index != 0 {
		t.Fatalf("failed snapshot published metadata %#v", metadata)
	}
	_, retained := store.Load()
	if len(retained) != 2 || retained[0].Index != 1 || retained[1].Index != 2 {
		t.Fatalf("failed snapshot compacted log: %#v", retained)
	}
}

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

func TestStorePersistsSnapshotAndCompactsPrefix(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(raft.Update{Entries: []raft.Entry{{Index: 1, Term: 1, Data: []byte("one")}, {Index: 2, Term: 1, Data: []byte("two")}, {Index: 3, Term: 2, Data: []byte("three")}}}); err != nil {
		t.Fatal(err)
	}
	snapshot := raft.Snapshot{Index: 2, Term: 1, Data: []byte("state"), Membership: raft.Membership{Voters: []uint64{1, 2, 3}, Index: 2}}
	if err := store.Persist(raft.Update{Snapshot: &snapshot}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got := reopened.LoadSnapshot()
	if got.Index != 2 || got.Term != 1 || string(got.Data) != "state" {
		t.Fatalf("snapshot = %#v", got)
	}
	if len(got.Membership.Voters) != 3 || got.Membership.Index != 2 {
		t.Fatalf("snapshot membership = %+v", got.Membership)
	}
	_, entries := reopened.Load()
	if len(entries) != 1 || entries[0].Index != 3 {
		t.Fatalf("retained entries = %#v", entries)
	}
	if err := reopened.Persist(raft.Update{Entries: []raft.Entry{{Index: 4, Term: 2}}}); err != nil {
		t.Fatalf("append after compaction: %v", err)
	}
}

func TestStoreRejectsCorruptSnapshot(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := raft.Snapshot{Index: 1, Term: 1, Data: []byte("state")}
	if err := store.Persist(raft.Update{Entries: []raft.Entry{{Index: 1, Term: 1}}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(raft.Update{Snapshot: &snapshot}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, snapshotFile)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil {
		t.Fatal("Open accepted corrupt snapshot")
	}
}

func TestStoreRecoversSnapshotPublishedBeforeLogRewrite(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(raft.Update{Entries: []raft.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 2, Data: []byte("suffix")}}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	// Model a crash after the atomic snapshot rename but before raft.log rewrite.
	if err := storeSnapshot(dir, raft.Snapshot{Index: 2, Term: 1, Data: []byte("state")}); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	_, entries := reopened.Load()
	if len(entries) != 1 || entries[0].Index != 3 || string(entries[0].Data) != "suffix" {
		t.Fatalf("recovered suffix = %#v", entries)
	}
}
