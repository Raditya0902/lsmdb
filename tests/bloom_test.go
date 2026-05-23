package tests

import (
	"fmt"
	"testing"

	"lsmdb/db"
	"lsmdb/internal/bloom"
)

// ── BloomFilter unit tests ──────────────────────────────────────────────────

func TestBloomNoFalseNegatives(t *testing.T) {
	const n = 1000
	bf := bloom.New(n, 0.01)

	inserted := make([][]byte, n)
	for i := range inserted {
		inserted[i] = []byte(fmt.Sprintf("key:%06d", i))
		bf.Add(inserted[i])
	}

	for _, k := range inserted {
		if !bf.MayContain(k) {
			t.Errorf("false negative for %q", k)
		}
	}
}

func TestBloomFalsePositiveRate(t *testing.T) {
	const (
		inserted = 100
		trials   = 10_000
		maxFPR   = 0.02
	)
	bf := bloom.New(inserted, 0.01)

	for i := 0; i < inserted; i++ {
		bf.Add([]byte(fmt.Sprintf("present:%06d", i)))
	}

	fp := 0
	for i := 0; i < trials; i++ {
		if bf.MayContain([]byte(fmt.Sprintf("absent:%06d", i))) {
			fp++
		}
	}

	rate := float64(fp) / trials
	if rate >= maxFPR {
		t.Errorf("false positive rate %.4f >= %.4f (%d/%d)", rate, maxFPR, fp, trials)
	}
}

func TestBloomEncodeDecode(t *testing.T) {
	const n = 50
	bf := bloom.New(n, 0.01)

	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("k:%04d", i))
		bf.Add(keys[i])
	}

	data := bf.Encode()
	bf2, err := bloom.Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	// All inserted keys must still be found after decode.
	for _, k := range keys {
		if !bf2.MayContain(k) {
			t.Errorf("decoded filter: false negative for %q", k)
		}
	}

	// The decoded filter must agree with the original for non-inserted keys.
	for i := n; i < n*2; i++ {
		k := []byte(fmt.Sprintf("k:%04d", i))
		if bf.MayContain(k) != bf2.MayContain(k) {
			t.Errorf("decoded filter result differs for non-inserted key %d", i)
		}
	}
}

func TestBloomDecodeRejectsShortInput(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"15 bytes", make([]byte, 15)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := bloom.Decode(tc.data); err == nil {
				t.Error("expected error, got nil")
			}
		})
	}
}

// ── Integration test: bloom filter skips SSTables ──────────────────────────

func TestBloomSSTableSkipping(t *testing.T) {
	dir := t.TempDir()
	// CompactionThreshold=100 disables auto-compaction so all 5 SSTables stay open.
	opts := &db.Options{FlushThreshold: 10, CompactionThreshold: 100}

	d, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Write 50 keys → 5 flushes → 5 SSTables (threshold=10).
	const numKeys = 50
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key:%06d", i)
		if err := d.Set(key, []byte("v")); err != nil {
			t.Fatalf("Set %s: %v", key, err)
		}
	}

	numSSTables := d.SSTableCount()
	if numSSTables < 3 {
		t.Fatalf("expected ≥3 SSTables, got %d — check FlushThreshold", numSSTables)
	}

	// All inserted keys must still be readable.
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key:%06d", i)
		if _, ok := d.Get(key); !ok {
			t.Errorf("inserted key %q not found", key)
		}
	}

	// 1000 lookups for keys that were never inserted.
	// "zzz:" prefix puts them lexicographically above every inserted key,
	// so they are definitely absent from every SSTable.
	const numMisses = 1000
	for i := 0; i < numMisses; i++ {
		if _, ok := d.Get(fmt.Sprintf("zzz:%06d", i)); ok {
			t.Fatalf("unexpected hit for zzz:%06d", i)
		}
	}

	// Each miss consults all SSTables. With a 1% FPR, ≥99% of those
	// per-(SSTable, lookup) checks should have been bloom-skipped.
	bloomSkips := d.BloomSkips()
	totalChecks := int64(numSSTables) * numMisses
	minExpected := totalChecks * 9 / 10 // 90% lower bound
	if bloomSkips < minExpected {
		t.Errorf("bloom skips %d < %d (90%% of %d total checks across %d SSTables)",
			bloomSkips, minExpected, totalChecks, numSSTables)
	}

	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
