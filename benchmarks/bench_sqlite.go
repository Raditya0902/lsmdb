package benchmarks

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const sqliteEngine = "SQLite"

// SQLite pragmas chosen for a fair comparison:
//   - journal_mode=WAL: matches LSM's write-ahead approach; allows concurrent reads
//   - synchronous=NORMAL: syncs on WAL checkpoints but not every write, comparable to
//     the LSM engine which only fsyncs on SSTable flush (not every WAL append)
//
// Both engines therefore accept a narrow data-loss window on OS crash, which keeps
// durability levels equivalent for benchmarking purposes.
const sqliteSetup = `
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
CREATE TABLE IF NOT EXISTS kv (key BLOB PRIMARY KEY, value BLOB NOT NULL);
`

const (
	sqliteUpsert = `INSERT INTO kv(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`
	sqliteGet    = `SELECT value FROM kv WHERE key=?`
	sqliteDelete = `DELETE FROM kv WHERE key=?`
)

// sqliteDB wraps *sql.DB with helper methods matching our workload interface.
type sqliteDB struct {
	db  *sql.DB
	dir string
}

func openSQLite(dir string) (*sqliteDB, error) {
	path := filepath.Join(dir, "bench.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	if _, err := db.Exec(sqliteSetup); err != nil {
		db.Close() //nolint:errcheck
		return nil, fmt.Errorf("sqlite setup: %w", err)
	}
	return &sqliteDB{db: db, dir: dir}, nil
}

func (s *sqliteDB) set(key, value []byte) error {
	_, err := s.db.Exec(sqliteUpsert, key, value)
	return err
}

func (s *sqliteDB) get(key []byte) ([]byte, bool) {
	var val []byte
	err := s.db.QueryRow(sqliteGet, key).Scan(&val)
	if err != nil {
		return nil, false
	}
	return val, true
}

func (s *sqliteDB) delete(key []byte) error {
	_, err := s.db.Exec(sqliteDelete, key)
	return err
}

// batchSet writes keys/values in batches of 100 inside a single transaction.
func (s *sqliteDB) batchSet(keys, values [][]byte) error {
	const batchSize = 100
	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		tx, err := s.db.Begin()
		if err != nil {
			return err
		}
		stmt, err := tx.Prepare(sqliteUpsert)
		if err != nil {
			tx.Rollback() //nolint:errcheck
			return err
		}
		for j := i; j < end; j++ {
			if _, err := stmt.Exec(keys[j], values[j]); err != nil {
				stmt.Close() //nolint:errcheck
				tx.Rollback() //nolint:errcheck
				return err
			}
		}
		stmt.Close() //nolint:errcheck
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func (s *sqliteDB) close() error { return s.db.Close() }

// RunSQLite executes all six standard workloads against SQLite.
func RunSQLite(dir string) ([]WorkloadResult, error) {
	results := make([]WorkloadResult, 0, 6)

	for _, w := range []struct {
		name string
		fn   func(*sqliteDB) ([]time.Duration, error)
	}{
		{"A: Sequential Writes", workloadASQLite},
		{"B: Random Writes", workloadBSQLite},
		{"C: Read-after-Write", workloadCSQLite},
		{"D: Point Lookups", workloadDSQLite},
		{"E: Update-Heavy", workloadESQLite},
		{"F: Delete-Heavy", workloadFSQLite},
	} {
		wl, err := runSQLiteWorkload(dir, w.name, w.fn)
		if err != nil {
			return nil, fmt.Errorf("workload %s: %w", w.name, err)
		}
		results = append(results, wl)
	}
	return results, nil
}

func runSQLiteWorkload(baseDir, name string, fn func(*sqliteDB) ([]time.Duration, error)) (WorkloadResult, error) {
	dir := filepath.Join(baseDir, "sqlite")
	os.RemoveAll(dir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return WorkloadResult{}, err
	}

	d, err := openSQLite(dir)
	if err != nil {
		return WorkloadResult{}, err
	}

	latencies, err := fn(d)
	if err != nil {
		d.close() //nolint:errcheck
		return WorkloadResult{}, err
	}

	if err := d.close(); err != nil {
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

	return WorkloadResult{
		Workload:  name,
		Engine:    sqliteEngine,
		OpsPerSec: opsPerSec,
		P50Ms:     p50,
		P95Ms:     p95,
		P99Ms:     p99,
		DiskBytes: disk,
	}, nil
}

// ── Workload implementations ──────────────────────────────────────────────

func workloadASQLite(d *sqliteDB) ([]time.Duration, error) {
	keys := SequentialKeys(10_000)
	vals := RandomValues(10_000, 1, 64)
	latencies := make([]time.Duration, len(keys))
	for i, k := range keys {
		start := time.Now()
		if err := d.set(k, vals[i]); err != nil {
			return nil, err
		}
		latencies[i] = time.Since(start)
	}
	return latencies, nil
}

func workloadBSQLite(d *sqliteDB) ([]time.Duration, error) {
	keys := RandomKeys(10_000, 2)
	vals := RandomValues(10_000, 2, 64)
	latencies := make([]time.Duration, len(keys))
	for i, k := range keys {
		start := time.Now()
		if err := d.set(k, vals[i]); err != nil {
			return nil, err
		}
		latencies[i] = time.Since(start)
	}
	return latencies, nil
}

func workloadCSQLite(d *sqliteDB) ([]time.Duration, error) {
	keys := SequentialKeys(5_000)
	vals := RandomValues(5_000, 3, 64)
	latencies := make([]time.Duration, 0, len(keys)*2)
	for i, k := range keys {
		start := time.Now()
		if err := d.set(k, vals[i]); err != nil {
			return nil, err
		}
		latencies = append(latencies, time.Since(start))

		start = time.Now()
		d.get(k)
		latencies = append(latencies, time.Since(start))
	}
	return latencies, nil
}

func workloadDSQLite(d *sqliteDB) ([]time.Duration, error) {
	const n = 5_000
	keys := SequentialKeys(n)
	vals := RandomValues(n, 4, 64)
	if err := d.batchSet(keys, vals); err != nil {
		return nil, err
	}
	rng := rand.New(rand.NewSource(4))
	latencies := make([]time.Duration, n)
	for i := 0; i < n; i++ {
		var key []byte
		if rng.Intn(2) == 0 {
			key = keys[rng.Intn(n)]
		} else {
			key = []byte(fmt.Sprintf("missing%09d", i))
		}
		start := time.Now()
		d.get(key)
		latencies[i] = time.Since(start)
	}
	return latencies, nil
}

func workloadESQLite(d *sqliteDB) ([]time.Duration, error) {
	const (
		numKeys    = 10
		iterations = 1_000
	)
	rng := rand.New(rand.NewSource(5))
	latencies := make([]time.Duration, numKeys*iterations)
	idx := 0
	for i := 0; i < iterations; i++ {
		for j := 0; j < numKeys; j++ {
			key := []byte(fmt.Sprintf("hotkey%02d", j))
			val := []byte(fmt.Sprintf("val%08d", rng.Int63()))
			start := time.Now()
			if err := d.set(key, val); err != nil {
				return nil, err
			}
			latencies[idx] = time.Since(start)
			idx++
		}
	}
	return latencies, nil
}

func workloadFSQLite(d *sqliteDB) ([]time.Duration, error) {
	const n = 5_000
	keys := SequentialKeys(n)
	vals := RandomValues(n, 6, 64)
	if err := d.batchSet(keys, vals); err != nil {
		return nil, err
	}

	rng := rand.New(rand.NewSource(6))
	perm := rng.Perm(n)
	for _, idx := range perm[:n/2] {
		if err := d.delete(keys[idx]); err != nil {
			return nil, err
		}
	}

	latencies := make([]time.Duration, n)
	for i, k := range keys {
		start := time.Now()
		d.get(k)
		latencies[i] = time.Since(start)
	}
	return latencies, nil
}
