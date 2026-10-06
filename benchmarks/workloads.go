// Package benchmarks implements workload harnesses comparing the LSM engine to SQLite.
package benchmarks

import (
	"fmt"
	"math/rand"
	"time"

	"lsmdb/internal/benchenv"
)

// WorkloadResult holds timing and storage metrics for one workload run.
// OpsPerSec is Ops divided by the wall time of the timed section only;
// pre-population is excluded. Percentiles are nearest-rank over per-op latencies.
type WorkloadResult struct {
	Workload    string  `json:"workload"`
	Engine      string  `json:"engine"`
	Ops         int     `json:"ops"`
	WallSeconds float64 `json:"wall_seconds"`
	OpsPerSec   float64 `json:"ops_per_sec"`
	P50Ms       float64 `json:"p50_ms"`
	P95Ms       float64 `json:"p95_ms"`
	P99Ms       float64 `json:"p99_ms"`
	P999Ms      float64 `json:"p99_9_ms"`
	MaxMs       float64 `json:"max_ms"`
	DiskBytes   int64   `json:"disk_bytes"`
	BloomSkips  int64   `json:"bloom_skips,omitempty"`
	ReadAmp     float64 `json:"read_amp,omitempty"`
}

// resultFor computes throughput from the timed section's wall time and
// latency percentiles from the per-op latencies.
func resultFor(workload, engine string, latencies []time.Duration, wall time.Duration) WorkloadResult {
	sorted := append([]time.Duration(nil), latencies...)
	benchenv.SortDurations(sorted)
	at := func(q float64) float64 { return benchenv.Milliseconds(benchenv.Percentile(sorted, q)) }
	result := WorkloadResult{
		Workload: workload, Engine: engine, Ops: len(latencies), WallSeconds: wall.Seconds(),
		P50Ms: at(0.50), P95Ms: at(0.95), P99Ms: at(0.99), P999Ms: at(0.999), MaxMs: at(1.0),
	}
	if wall > 0 {
		result.OpsPerSec = float64(len(latencies)) / wall.Seconds()
	}
	return result
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
