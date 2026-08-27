package tests

import (
	"testing"

	"lsmdb/db"
)

func replicaOptions() *db.Options {
	return &db.Options{
		DurabilityMode:      db.DurabilityReplica,
		FlushThreshold:      2,
		CompactionThreshold: 8,
	}
}

func TestReplicaApplyBatchAndRecoverWatermark(t *testing.T) {
	dir := t.TempDir()
	d, err := db.Open(dir, replicaOptions())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	err = d.ApplyBatch(1, []db.Mutation{
		{Type: db.MutationPut, Key: "user/a", Value: []byte("one")},
		{Type: db.MutationPut, Key: "session/client", Value: []byte("1")},
	})
	if err != nil {
		t.Fatalf("ApplyBatch(1): %v", err)
	}
	if got := d.DurableIndex(); got != 1 {
		t.Fatalf("DurableIndex = %d, want 1 after threshold flush", got)
	}
	if err := d.ApplyBatch(2, []db.Mutation{{Type: db.MutationPut, Key: "user/a", Value: []byte("two")}}); err != nil {
		t.Fatalf("ApplyBatch(2): %v", err)
	}
	if got := d.DurableIndex(); got != 1 {
		t.Fatalf("DurableIndex = %d, want 1 before close flush", got)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := db.Open(dir, replicaOptions())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if got := reopened.AppliedIndex(); got != 2 {
		t.Fatalf("AppliedIndex = %d, want 2", got)
	}
	if value, ok := reopened.Get("user/a"); !ok || string(value) != "two" {
		t.Fatalf("Get(user/a) = (%q, %v), want (two, true)", value, ok)
	}
	if err := reopened.ApplyBatch(2, []db.Mutation{{Type: db.MutationDelete, Key: "user/a"}}); err != nil {
		t.Fatalf("idempotent ApplyBatch: %v", err)
	}
	if value, ok := reopened.Get("user/a"); !ok || string(value) != "two" {
		t.Fatalf("duplicate index changed state: (%q, %v)", value, ok)
	}
}

func TestReplicaApplyRejectsGapsAndEmbeddedWrites(t *testing.T) {
	d, err := db.Open(t.TempDir(), replicaOptions())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	if err := d.ApplyBatch(2, []db.Mutation{{Type: db.MutationPut, Key: "k", Value: []byte("v")}}); err == nil {
		t.Fatal("ApplyBatch accepted an index gap")
	}
	if err := d.Set("k", []byte("v")); err == nil {
		t.Fatal("Set succeeded in replica mode")
	}
	if err := d.Delete("k"); err == nil {
		t.Fatal("Delete succeeded in replica mode")
	}
}

func TestReplicaNoopCanPersistAppliedIndex(t *testing.T) {
	dir := t.TempDir()
	d, err := db.Open(dir, replicaOptions())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := d.ApplyBatch(1, nil); err != nil {
		t.Fatalf("ApplyBatch no-op: %v", err)
	}
	if err := d.ForceFlush(); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}
	if got := d.DurableIndex(); got != 1 {
		t.Fatalf("DurableIndex = %d, want 1", got)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := db.Open(dir, replicaOptions())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if got := reopened.AppliedIndex(); got != 1 {
		t.Fatalf("AppliedIndex = %d, want 1", got)
	}
}
