package benchmarks

import (
	"testing"
)

func BenchmarkSQLite_SequentialWrite(b *testing.B) {
	dir := b.TempDir()
	d, err := openSQLite(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer d.close() //nolint:errcheck

	keys := SequentialKeys(b.N)
	vals := RandomValues(b.N, 1, 64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := d.set(keys[i], vals[i]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLite_RandomWrite(b *testing.B) {
	dir := b.TempDir()
	d, err := openSQLite(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer d.close() //nolint:errcheck

	keys := RandomKeys(b.N, 2)
	vals := RandomValues(b.N, 2, 64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := d.set(keys[i], vals[i]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLite_PointLookup(b *testing.B) {
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

	lookups := RandomKeys(b.N, seed+1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.get(lookups[i%len(lookups)])
	}
}
