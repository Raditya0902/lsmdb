package benchmarks

import (
	"math/rand"
	"testing"

	"lsmdb/db"
)

func BenchmarkLSM_ConcurrentReads(b *testing.B) {
	dir := b.TempDir()
	d, err := db.Open(dir, &db.Options{FlushThreshold: 1000, CompactionThreshold: 4})
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()

	const seed = 100
	keys := SequentialKeys(1000)
	vals := RandomValues(1000, seed, 64)
	for i, k := range keys {
		if err := d.Set(string(k), vals[i]); err != nil {
			b.Fatal(err)
		}
	}
	if err := d.ForceFlush(); err != nil {
		b.Fatal(err)
	}
	if err := d.ForceCompact(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(42))
		for pb.Next() {
			d.Get(string(keys[rng.Intn(len(keys))]))
		}
	})
}

func BenchmarkSQLite_ConcurrentReads(b *testing.B) {
	dir := b.TempDir()
	d, err := openSQLite(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer d.close() //nolint:errcheck

	const seed = 100
	keys := SequentialKeys(1000)
	vals := RandomValues(1000, seed, 64)
	if err := d.batchSet(keys, vals); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(42))
		for pb.Next() {
			d.get(keys[rng.Intn(len(keys))])
		}
	})
}
