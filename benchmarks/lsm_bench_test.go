package benchmarks

import (
	"testing"

	"lsmdb/db"
)

func BenchmarkLSM_SequentialWrite(b *testing.B) {
	dir := b.TempDir()
	d, err := db.Open(dir, &db.Options{FlushThreshold: 1000, CompactionThreshold: 4})
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()

	keys := SequentialKeys(b.N)
	vals := RandomValues(b.N, 1, 64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := d.Set(string(keys[i]), vals[i]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLSM_RandomWrite(b *testing.B) {
	dir := b.TempDir()
	d, err := db.Open(dir, &db.Options{FlushThreshold: 1000, CompactionThreshold: 4})
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()

	keys := RandomKeys(b.N, 2)
	vals := RandomValues(b.N, 2, 64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := d.Set(string(keys[i]), vals[i]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLSM_PointLookup(b *testing.B) {
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

	lookups := RandomKeys(b.N, seed+1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Get(string(lookups[i%len(lookups)]))
	}
}
