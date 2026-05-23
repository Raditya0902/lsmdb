// Package benchmarks implements workload harnesses comparing the LSM engine to SQLite.
package benchmarks

import (
	"fmt"
	"math/rand"
	"sort"
	"time"
)

// WorkloadResult holds timing and storage metrics for one workload run.
type WorkloadResult struct {
	Workload   string  `json:"workload"`
	Engine     string  `json:"engine"`
	OpsPerSec  float64 `json:"ops_per_sec"`
	P50Ms      float64 `json:"p50_ms"`
	P95Ms      float64 `json:"p95_ms"`
	P99Ms      float64 `json:"p99_ms"`
	DiskBytes  int64   `json:"disk_bytes"`
	BloomSkips int64   `json:"bloom_skips,omitempty"`
	ReadAmp    float64 `json:"read_amp,omitempty"`
}

// latencyStats computes P50/P95/P99 from an unsorted slice of durations.
func latencyStats(latencies []time.Duration) (p50, p95, p99 float64) {
	if len(latencies) == 0 {
		return 0, 0, 0
	}
	cp := make([]time.Duration, len(latencies))
	copy(cp, latencies)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })

	idx := func(pct float64) time.Duration {
		i := int(float64(len(cp)-1) * pct)
		return cp[i]
	}
	toMs := func(d time.Duration) float64 { return float64(d.Nanoseconds()) / 1e6 }
	return toMs(idx(0.50)), toMs(idx(0.95)), toMs(idx(0.99))
}

// SequentialKeys returns n keys with a zero-padded numeric suffix.
func SequentialKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key%09d", i))
	}
	return keys
}

// RandomKeys returns n keys drawn from a seeded PRNG.
func RandomKeys(n int, seed int64) [][]byte {
	rng := rand.New(rand.NewSource(seed))
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key%09d", rng.Intn(n*10)))
	}
	return keys
}

// RandomValues returns n fixed-size values from a seeded PRNG.
func RandomValues(n int, seed int64, size int) [][]byte {
	rng := rand.New(rand.NewSource(seed))
	vals := make([][]byte, n)
	buf := make([]byte, size)
	for i := range vals {
		rng.Read(buf)
		cp := make([]byte, size)
		copy(cp, buf)
		vals[i] = cp
	}
	return vals
}
