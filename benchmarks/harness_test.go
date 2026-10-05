package benchmarks

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"
)

func TestLatencyStatsIncludesP999AndMax(t *testing.T) {
	latencies := make([]time.Duration, 1000)
	for i := range latencies {
		latencies[i] = time.Duration(i+1) * time.Millisecond
	}
	rand.New(rand.NewSource(1)).Shuffle(len(latencies), func(i, j int) {
		latencies[i], latencies[j] = latencies[j], latencies[i]
	})
	got := resultFor("W", "E", latencies, 10*time.Second)
	want := WorkloadResult{
		Workload: "W", Engine: "E", Ops: 1000, WallSeconds: 10, OpsPerSec: 100,
		P50Ms: 500, P95Ms: 950, P99Ms: 990, P999Ms: 999, MaxMs: 1000,
	}
	if got != want {
		t.Fatalf("resultFor = %+v\nwant        %+v", got, want)
	}
}

// pacedWorkload records near-zero per-op latencies but sleeps between ops, so
// throughput from summed latencies would be far higher than wall-clock throughput.
func pacedWorkload(ops int, gap time.Duration, set func(i int) error) ([]time.Duration, time.Duration, error) {
	start := time.Now()
	latencies := make([]time.Duration, ops)
	for i := range latencies {
		opStart := time.Now()
		if err := set(i); err != nil {
			return nil, 0, err
		}
		latencies[i] = time.Since(opStart)
		time.Sleep(gap)
	}
	return latencies, time.Since(start), nil
}

func TestWorkloadThroughputUsesWallClock(t *testing.T) {
	const (
		ops = 10
		gap = 5 * time.Millisecond
	)
	maxOpsPerSec := float64(ops) / (ops * gap).Seconds()
	lsm, err := runLSMWorkload(t.TempDir(), "paced", func(d *lsmDB) ([]time.Duration, time.Duration, error) {
		return pacedWorkload(ops, gap, func(i int) error { return d.Set(fmt.Sprintf("k%d", i), []byte("v")) })
	})
	if err != nil {
		t.Fatal(err)
	}
	sqlite, err := runSQLiteWorkload(t.TempDir(), "paced", func(d *sqliteDB) ([]time.Duration, time.Duration, error) {
		return pacedWorkload(ops, gap, func(i int) error { return d.set([]byte(fmt.Sprintf("k%d", i)), []byte("v")) })
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range []WorkloadResult{lsm, sqlite} {
		if result.Ops != ops {
			t.Errorf("%s: ops = %d, want %d", result.Engine, result.Ops, ops)
		}
		if result.OpsPerSec > maxOpsPerSec {
			t.Errorf("%s: %.0f ops/s exceeds the wall-clock bound %.0f; throughput ignores time between ops", result.Engine, result.OpsPerSec, maxOpsPerSec)
		}
		if wall := float64(result.Ops) / result.WallSeconds; math.Abs(wall-result.OpsPerSec) > 1e-9*wall {
			t.Errorf("%s: ops/s = %v, want ops / wall_seconds = %v", result.Engine, result.OpsPerSec, wall)
		}
	}
}
