package tests

import (
	"fmt"
	"math/rand"
	"testing"

	"lsmdb/db"
)

// TestPropertyRandomOps generates 200 random Set/Delete operations against both
// the LSM-tree and a reference map. After forcing a flush and compaction, every
// key in the reference must match the LSM result, and every key absent from the
// reference must not be found in the LSM.
func TestPropertyRandomOps(t *testing.T) {
	dir := t.TempDir()
	opts := &db.Options{
		FlushThreshold:      5,
		CompactionThreshold: 4,
	}

	d, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	ref := make(map[string]string)
	rng := rand.New(rand.NewSource(42))

	const (
		numOps  = 200
		numKeys = 10
	)

	keys := make([]string, numKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%02d", i)
	}

	for i := 0; i < numOps; i++ {
		k := keys[rng.Intn(numKeys)]
		if rng.Intn(3) != 0 { // 2/3 chance of Set
			val := fmt.Sprintf("val%04d", rng.Intn(10000))
			if err := d.Set(k, []byte(val)); err != nil {
				t.Fatalf("op %d Set(%s): %v", i, k, err)
			}
			ref[k] = val
		} else {
			if err := d.Delete(k); err != nil {
				t.Fatalf("op %d Delete(%s): %v", i, k, err)
			}
			delete(ref, k)
		}
	}

	// Flush any remaining memtable entries, then compact everything into one SSTable.
	if err := d.ForceFlush(); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}
	if err := d.ForceCompact(); err != nil {
		t.Fatalf("ForceCompact: %v", err)
	}

	// Every key in the reference must be found with the correct value.
	for k, want := range ref {
		if v, ok := d.Get(k); !ok || string(v) != want {
			t.Errorf("key %q: got (%q, %v), want (%q, true)", k, v, ok, want)
		}
	}

	// Every key absent from the reference must not be found.
	for _, k := range keys {
		if _, inRef := ref[k]; !inRef {
			if _, ok := d.Get(k); ok {
				t.Errorf("key %q should not exist (deleted or never written)", k)
			}
		}
	}
}

// TestPropertyCompactionIsIdempotent verifies that running compaction twice
// on the same data produces the same observable results.
func TestPropertyCompactionIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	opts := &db.Options{FlushThreshold: 3, CompactionThreshold: 100}

	d, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	// Build a small known dataset.
	data := map[string]string{
		"alpha": "1", "beta": "2", "gamma": "3",
		"delta": "4", "epsilon": "5",
	}
	deleted := []string{"beta", "delta"}

	for k, v := range data {
		if err := d.Set(k, []byte(v)); err != nil {
			t.Fatalf("Set %s: %v", k, err)
		}
	}
	for _, k := range deleted {
		if err := d.Delete(k); err != nil {
			t.Fatalf("Delete %s: %v", k, err)
		}
		delete(data, k)
	}

	if err := d.ForceFlush(); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}

	// Compact once.
	if err := d.ForceCompact(); err != nil {
		t.Fatalf("ForceCompact 1: %v", err)
	}
	// Compact again — should be a no-op (1 SSTable < threshold=100, but ForceCompact runs regardless).
	if err := d.ForceCompact(); err != nil {
		t.Fatalf("ForceCompact 2: %v", err)
	}

	// Results must be identical after both compactions.
	for k, want := range data {
		if v, ok := d.Get(k); !ok || string(v) != want {
			t.Errorf("%s: got (%q, %v), want (%q, true)", k, v, ok, want)
		}
	}
	for _, k := range deleted {
		if _, ok := d.Get(k); ok {
			t.Errorf("deleted key %q found after double compaction", k)
		}
	}
}

// TestPropertyRandomOpsWithReopen compares the engine with a reference map while
// interleaving writes, deletes, flushes, compactions, and reopens. Reopening after
// a flush is what exposed sequence-number reuse, so every seed exercises it.
func TestPropertyRandomOpsWithReopen(t *testing.T) {
	const (
		numOps  = 1000
		numKeys = 16
	)
	opts := &db.Options{FlushThreshold: 32, CompactionThreshold: 3}
	keys := make([]string, numKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%02d", i)
	}

	for seed := int64(1); seed <= 4; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			dir := t.TempDir()
			d, err := db.Open(dir, opts)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer func() { _ = d.Close() }()
			ref := make(map[string]string)
			rng := rand.New(rand.NewSource(seed))

			for step := 0; step < numOps; step++ {
				k := keys[rng.Intn(numKeys)]
				switch roll := rng.Intn(100); {
				case roll < 60:
					val := fmt.Sprintf("s%d-v%d", seed, step)
					if err := d.Set(k, []byte(val)); err != nil {
						t.Fatalf("step %d Set(%s): %v", step, k, err)
					}
					ref[k] = val
				case roll < 80:
					if err := d.Delete(k); err != nil {
						t.Fatalf("step %d Delete(%s): %v", step, k, err)
					}
					delete(ref, k)
				case roll < 88:
					if err := d.ForceFlush(); err != nil {
						t.Fatalf("step %d ForceFlush: %v", step, err)
					}
				case roll < 94:
					if err := d.ForceCompact(); err != nil {
						t.Fatalf("step %d ForceCompact: %v", step, err)
					}
				default:
					if err := d.Close(); err != nil {
						t.Fatalf("step %d Close: %v", step, err)
					}
					if d, err = db.Open(dir, opts); err != nil {
						t.Fatalf("step %d reopen: %v", step, err)
					}
				}
				assertMatchesModel(t, d, ref, keys, step)
			}
		})
	}
}

func assertMatchesModel(t *testing.T, d *db.DB, ref map[string]string, keys []string, step int) {
	t.Helper()
	for _, k := range keys {
		got, ok := d.Get(k)
		want, exists := ref[k]
		if ok != exists || string(got) != want {
			t.Fatalf("step %d Get(%s) = (%q, %v), want (%q, %v)", step, k, got, ok, want, exists)
		}
	}
	pairs, err := d.Scan(keys[0], keys[len(keys)-1])
	if err != nil {
		t.Fatalf("step %d Scan: %v", step, err)
	}
	if len(pairs) != len(ref) {
		t.Fatalf("step %d Scan returned %d pairs, want %d", step, len(pairs), len(ref))
	}
	for _, pair := range pairs {
		if want := ref[pair.Key]; string(pair.Value) != want {
			t.Fatalf("step %d Scan[%s] = %q, want %q", step, pair.Key, pair.Value, want)
		}
	}
}
