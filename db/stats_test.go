package db

import (
	"fmt"
	"testing"
)

func TestStatsCountFlushesAndCompactions(t *testing.T) {
	// FlushThreshold 10 and CompactionThreshold 2: every tenth distinct key
	// flushes, and every flush that leaves two SSTables compacts them into one.
	cases := []struct {
		writes, flushes, compactions uint64
	}{
		{writes: 0, flushes: 0, compactions: 0},
		{writes: 9, flushes: 0, compactions: 0},
		{writes: 10, flushes: 1, compactions: 0},
		{writes: 20, flushes: 2, compactions: 1},
		{writes: 40, flushes: 4, compactions: 3},
	}
	modes := []struct {
		name  string
		mode  DurabilityMode
		write func(*DB, int) error
	}{
		{name: "embedded", mode: DurabilityEmbedded, write: func(database *DB, i int) error {
			return database.Set(fmt.Sprintf("key-%03d", i), []byte("v"))
		}},
		{name: "replica", mode: DurabilityReplica, write: func(database *DB, i int) error {
			return database.ApplyBatch(uint64(i+1), []Mutation{{Type: MutationPut, Key: fmt.Sprintf("key-%03d", i), Value: []byte("v")}})
		}},
	}
	for _, mode := range modes {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("%s/%d writes", mode.name, tc.writes), func(t *testing.T) {
				database, err := Open(t.TempDir(), &Options{FlushThreshold: 10, CompactionThreshold: 2, DurabilityMode: mode.mode})
				if err != nil {
					t.Fatal(err)
				}
				defer database.Close()
				for i := 0; i < int(tc.writes); i++ {
					if err := mode.write(database, i); err != nil {
						t.Fatal(err)
					}
				}
				stats := database.Stats()
				if stats.Flushes != tc.flushes || stats.Compactions != tc.compactions {
					t.Fatalf("Stats = %d flushes, %d compactions; want %d and %d", stats.Flushes, stats.Compactions, tc.flushes, tc.compactions)
				}
				if (stats.Flushes > 0) != (stats.FlushTime > 0) {
					t.Errorf("%d flushes took %v", stats.Flushes, stats.FlushTime)
				}
				if (stats.Compactions > 0) != (stats.CompactionTime > 0) {
					t.Errorf("%d compactions took %v", stats.Compactions, stats.CompactionTime)
				}
			})
		}
	}
}

func TestStatsSkipEmptyForceFlush(t *testing.T) {
	database, err := Open(t.TempDir(), &Options{FlushThreshold: 10, CompactionThreshold: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.ForceFlush(); err != nil {
		t.Fatal(err)
	}
	if stats := database.Stats(); stats != (Stats{}) {
		t.Fatalf("Stats after a ForceFlush of an empty memtable = %+v, want zero", stats)
	}
}
