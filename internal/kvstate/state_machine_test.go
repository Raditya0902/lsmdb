package kvstate

import (
	"testing"

	lsmdbv1 "lsmdb/api/lsmdb/v1"
	"lsmdb/db"
)

func command(t *testing.T, operation lsmdbv1.Command_Operation, value string, sequence uint64) []byte {
	t.Helper()
	data, err := EncodeCommand(operation, []byte("key"), []byte(value), "client-a", sequence)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestMachineDeduplicatesDelayedRetry(t *testing.T) {
	machine, err := Open(t.TempDir(), &db.Options{FlushThreshold: 2, CompactionThreshold: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer machine.Close()
	if err := machine.Apply(1, nil); err != nil {
		t.Fatal(err)
	}
	if err := machine.Apply(2, command(t, lsmdbv1.Command_OPERATION_PUT, "one", 1)); err != nil {
		t.Fatal(err)
	}
	if err := machine.Apply(3, command(t, lsmdbv1.Command_OPERATION_PUT, "two", 2)); err != nil {
		t.Fatal(err)
	}
	if err := machine.Apply(4, command(t, lsmdbv1.Command_OPERATION_PUT, "one", 1)); err != nil {
		t.Fatal(err)
	}
	value, ok, err := machine.Get([]byte("key"))
	if err != nil || !ok || string(value) != "two" {
		t.Fatalf("Get = (%q, %v, %v), want (two, true, nil)", value, ok, err)
	}
	if machine.AppliedIndex() != 4 {
		t.Fatalf("AppliedIndex = %d, want 4", machine.AppliedIndex())
	}
}

func TestMachineRecoversSessionMetadata(t *testing.T) {
	dir := t.TempDir()
	options := &db.Options{FlushThreshold: 2, CompactionThreshold: 8}
	machine, err := Open(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := machine.Apply(1, command(t, lsmdbv1.Command_OPERATION_PUT, "new", 2)); err != nil {
		t.Fatal(err)
	}
	if err := machine.Close(); err != nil {
		t.Fatal(err)
	}

	machine, err = Open(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	defer machine.Close()
	if err := machine.Apply(2, command(t, lsmdbv1.Command_OPERATION_PUT, "old", 1)); err != nil {
		t.Fatal(err)
	}
	value, ok, err := machine.Get([]byte("key"))
	if err != nil || !ok || string(value) != "new" {
		t.Fatalf("Get after restart = (%q, %v, %v)", value, ok, err)
	}
}
