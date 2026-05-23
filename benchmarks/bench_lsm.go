package benchmarks

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"lsmdb/db"
)

const lsmEngine = "LSM"

// diskUsage sums the sizes of all files in dir.
func diskUsage(dir string) int64 {
	var total int64
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err == nil {
			total += info.Size()
		}
	}
	return total
}

// lsmDB wraps db.DB with a temp directory that is cleaned up on close.
type lsmDB struct {
	*db.DB
	dir string
}

func openLSM(dir string) (*lsmDB, error) {
	opts := &db.Options{
		FlushThreshold:      1000,
		CompactionThreshold: 4,
	}
	d, err := db.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	return &lsmDB{DB: d, dir: dir}, nil
}

// RunLSM executes all six standard workloads against the LSM engine.
func RunLSM(dir string) ([]WorkloadResult, error) {
	results := make([]WorkloadResult, 0, 6)

	wl, err := runLSMWorkload(dir, "A: Sequential Writes", workloadALSM)
	if err != nil {
		return nil, fmt.Errorf("workload A: %w", err)
	}
	results = append(results, wl)

	wl, err = runLSMWorkload(dir, "B: Random Writes", workloadBLSM)
	if err != nil {
		return nil, fmt.Errorf("workload B: %w", err)
	}
	results = append(results, wl)

	wl, err = runLSMWorkload(dir, "C: Read-after-Write", workloadCLSM)
	if err != nil {
		return nil, fmt.Errorf("workload C: %w", err)
	}
	results = append(results, wl)

	wl, err = runLSMWorkload(dir, "D: Point Lookups", workloadDLSM)
	if err != nil {
		return nil, fmt.Errorf("workload D: %w", err)
	}
	results = append(results, wl)

	wl, err = runLSMWorkload(dir, "E: Update-Heavy", workloadELSM)
	if err != nil {
		return nil, fmt.Errorf("workload E: %w", err)
	}
	results = append(results, wl)

	wl, err = runLSMWorkload(dir, "F: Delete-Heavy", workloadFLSM)
	if err != nil {
		return nil, fmt.Errorf("workload F: %w", err)
	}
	results = append(results, wl)

	return results, nil
}

func runLSMWorkload(baseDir, name string, fn func(*lsmDB) ([]time.Duration, error)) (WorkloadResult, error) {
	dir := filepath.Join(baseDir, "lsm")
	os.RemoveAll(dir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return WorkloadResult{}, err
	}

	d, err := openLSM(dir)
	if err != nil {
		return WorkloadResult{}, err
	}

	latencies, err := fn(d)
	if err != nil {
		d.Close() //nolint:errcheck
		return WorkloadResult{}, err
	}

	bloomSkips := d.BloomSkips()
	sstCount := d.SSTableCount()
	if err := d.Close(); err != nil {
		return WorkloadResult{}, err
	}

	disk := diskUsage(dir)
	p50, p95, p99 := latencyStats(latencies)
	n := len(latencies)

	var totalDur time.Duration
	for _, l := range latencies {
		totalDur += l
	}
	opsPerSec := float64(n) / totalDur.Seconds()

	var readAmp float64
	if sstCount > 0 {
		readAmp = float64(sstCount)
	}

	return WorkloadResult{
		Workload:   name,
		Engine:     lsmEngine,
		OpsPerSec:  opsPerSec,
		P50Ms:      p50,
		P95Ms:      p95,
		P99Ms:      p99,
		DiskBytes:  disk,
		BloomSkips: bloomSkips,
		ReadAmp:    readAmp,
	}, nil
}

// ── Workload implementations ──────────────────────────────────────────────

// A: 10,000 sequential writes.
func workloadALSM(d *lsmDB) ([]time.Duration, error) {
	keys := SequentialKeys(10_000)
	vals := RandomValues(10_000, 1, 64)
	latencies := make([]time.Duration, len(keys))
	for i, k := range keys {
		start := time.Now()
		if err := d.Set(string(k), vals[i]); err != nil {
			return nil, err
		}
		latencies[i] = time.Since(start)
	}
	return latencies, nil
}

// B: 10,000 random writes.
func workloadBLSM(d *lsmDB) ([]time.Duration, error) {
	keys := RandomKeys(10_000, 2)
	vals := RandomValues(10_000, 2, 64)
	latencies := make([]time.Duration, len(keys))
	for i, k := range keys {
		start := time.Now()
		if err := d.Set(string(k), vals[i]); err != nil {
			return nil, err
		}
		latencies[i] = time.Since(start)
	}
	return latencies, nil
}

// C: 5,000 read-after-write pairs.
func workloadCLSM(d *lsmDB) ([]time.Duration, error) {
	keys := SequentialKeys(5_000)
	vals := RandomValues(5_000, 3, 64)
	latencies := make([]time.Duration, 0, len(keys)*2)
	for i, k := range keys {
		start := time.Now()
		if err := d.Set(string(k), vals[i]); err != nil {
			return nil, err
		}
		latencies = append(latencies, time.Since(start))

		start = time.Now()
		d.Get(string(k))
		latencies = append(latencies, time.Since(start))
	}
	return latencies, nil
}

// D: 5,000 point lookups — 50% existing, 50% missing.
func workloadDLSM(d *lsmDB) ([]time.Duration, error) {
	const n = 5_000
	keys := SequentialKeys(n)
	vals := RandomValues(n, 4, 64)
	for i, k := range keys {
		if err := d.Set(string(k), vals[i]); err != nil {
			return nil, err
		}
	}
	rng := rand.New(rand.NewSource(4))
	latencies := make([]time.Duration, n)
	for i := 0; i < n; i++ {
		var key string
		if rng.Intn(2) == 0 {
			key = string(keys[rng.Intn(n)])
		} else {
			key = fmt.Sprintf("missing%09d", i)
		}
		start := time.Now()
		d.Get(key)
		latencies[i] = time.Since(start)
	}
	return latencies, nil
}

// E: 10 keys written 1,000 times each (update-heavy).
func workloadELSM(d *lsmDB) ([]time.Duration, error) {
	const (
		numKeys    = 10
		iterations = 1_000
	)
	rng := rand.New(rand.NewSource(5))
	latencies := make([]time.Duration, numKeys*iterations)
	idx := 0
	for i := 0; i < iterations; i++ {
		for j := 0; j < numKeys; j++ {
			key := fmt.Sprintf("hotkey%02d", j)
			val := fmt.Sprintf("val%08d", rng.Int63())
			start := time.Now()
			if err := d.Set(key, []byte(val)); err != nil {
				return nil, err
			}
			latencies[idx] = time.Since(start)
			idx++
		}
	}
	return latencies, nil
}

// F: write 5,000 keys, delete 2,500 random ones, read all 5,000.
func workloadFLSM(d *lsmDB) ([]time.Duration, error) {
	const n = 5_000
	keys := SequentialKeys(n)
	vals := RandomValues(n, 6, 64)
	for i, k := range keys {
		if err := d.Set(string(k), vals[i]); err != nil {
			return nil, err
		}
	}

	rng := rand.New(rand.NewSource(6))
	perm := rng.Perm(n)
	for _, idx := range perm[:n/2] {
		if err := d.Delete(string(keys[idx])); err != nil {
			return nil, err
		}
	}

	latencies := make([]time.Duration, n)
	for i, k := range keys {
		start := time.Now()
		d.Get(string(k))
		latencies[i] = time.Since(start)
	}
	return latencies, nil
}
