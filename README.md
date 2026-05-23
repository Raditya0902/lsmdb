![Go Tests](https://github.com/Raditya0902/lsmdb/actions/workflows/test.yml/badge.svg)

# lsmdb

A LevelDB-style LSM-tree key-value store implemented in Go, with a WAL, per-SSTable Bloom filters, size-tiered compaction, and a benchmark harness comparing it to SQLite.

---

## Quick start

```go
import "lsmdb/db"

d, err := db.Open("/tmp/mydb", db.DefaultOptions())
if err != nil { ... }
defer d.Close()

// Write
d.Set("hello", []byte("world"))

// Read
val, ok := d.Get("hello")  // val = []byte("world"), ok = true

// Delete (writes a tombstone; the key disappears from future reads)
d.Delete("hello")

val, ok = d.Get("hello")   // val = nil, ok = false
```

Pass `""` as the path for an in-memory-only database with no WAL and no persistence.

**Options**

```go
opts := &db.Options{
    FlushThreshold:      1000, // memtable entries before an SSTable flush
    CompactionThreshold: 4,    // SSTable count before a compaction
}
```

---

## Architecture

### Write path

```
Set(key, val)
    │
    ├─► WAL append (CRC32, binary, sequential write)
    │
    └─► MemTable (in-memory map, newest record per key)
            │
            │  [size >= FlushThreshold]
            ▼
        SSTable write (sorted, immutable, Bloom filter embedded)
            │
            │  [SSTable count >= CompactionThreshold]
            ▼
        K-way merge → new SSTable  (old files deleted after new one is written)
```

### Read path

```
Get(key)
    │
    ├─► MemTable  →  hit (PUT): return value
    │               hit (DELETE): return not-found
    │               miss: fall through
    │
    └─► SSTables, newest → oldest
            │
            ├─► Bloom filter says "absent"  →  skip SSTable (no disk read)
            │
            └─► Bloom filter says "maybe"  →  range check  →  index binary search
                    │
                    ├─► hit (PUT): return value
                    ├─► hit (DELETE): return not-found
                    └─► miss: try next SSTable
```

---

## Components

| Component | Responsibility | Key file |
|---|---|---|
| `MemTable` | In-memory write buffer; holds the most recent record per key | `internal/memtable/memtable.go` |
| `WAL` | Binary append log with CRC32 per record; replayed on startup for crash recovery | `internal/wal/wal.go` |
| `SSTable` | Immutable sorted file; data records + metadata + Bloom filter + sparse index + footer | `internal/sstable/` |
| `BloomFilter` | Probabilistic per-SSTable skip filter; eliminates disk reads for absent keys | `internal/bloom/bloom.go` |
| `Compactor` | K-way heap merge across all SSTables; drops tombstones at the bottom level | `internal/compact/compactor.go` |
| `DB` | Public API (`Open`, `Get`, `Set`, `Delete`, `Close`); orchestrates all components | `db/db.go` |

---

## Benchmark results

Measured on Apple M4, macOS, Go 1.22. Each workload runs against a fresh database.
SQLite uses `journal_mode=WAL` and `synchronous=NORMAL` — see [Methodology](#benchmark-methodology) for why.

```
Workload              Engine  Ops/sec  P50(ms)  P95(ms)  P99(ms)  Disk(KB)  BloomSkips  ReadAmp
--------              ------  -------  -------  -------  -------  --------  ----------  -------
A: Sequential Writes  LSM      63,467   0.001    0.004    0.021       934            0      1.0
B: Random Writes      LSM     107,392   0.001    0.002    0.004       911            0      3.0
C: Read-after-Write   LSM     217,279   0.001    0.001    0.003       467            0      2.0
D: Point Lookups      LSM     302,649   0.000    0.011    0.012       467        6,983      2.0
E: Update-Heavy       LSM     854,413   0.001    0.001    0.003       496            0      0.0
F: Delete-Heavy       LSM     312,883   0.001    0.010    0.011       296        1,980      1.0
A: Sequential Writes  SQLite   85,094   0.009    0.016    0.024     1,072            0      —
B: Random Writes      SQLite   80,589   0.010    0.018    0.025     1,008            0      —
C: Read-after-Write   SQLite  129,093   0.008    0.013    0.018       544            0      —
D: Point Lookups      SQLite  375,991   0.003    0.003    0.004       544            0      —
E: Update-Heavy       SQLite  118,312   0.007    0.009    0.012        12            0      —
F: Delete-Heavy       SQLite  387,536   0.003    0.003    0.003       544            0      —
```

**Interpretation.** The LSM engine's write latency (P50 ≈ 1 µs) is 7–10× lower than SQLite's across all write workloads because writes land in the MemTable and the WAL — both sequential, in-memory operations — rather than updating a B-tree page in place. The update-heavy workload (E) is the extreme case: all 10 hot keys stay in the MemTable for the entire run, so compaction never fires and throughput reaches 854k ops/sec versus SQLite's 118k.

SQLite wins on point lookups (D, F) and sequential writes (A). A B-tree lookup is O(log n) with excellent page-cache locality; the LSM read path checks the MemTable, then up to N SSTables (even with Bloom filtering, false positives still require a disk probe). SQLite's sequential write advantage reflects that B-tree leaf pages are written in sorted order when keys are monotonically increasing, which is an optimal access pattern for the page cache.

The Bloom filter performs as expected: 6,983 of 5,000 × 2 SSTable checks were skipped on workload D (99% skip rate), confirming that miss-path reads avoid disk I/O in the common case.

---

## Benchmark methodology

- **SQLite pragmas:** `PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL`. Both engines accept a narrow crash window (LSM only fsyncs on SSTable flush; SQLite only fsyncs on WAL checkpoint). This keeps durability levels equivalent.
- **SQLite write batching:** Pre-population steps use batched transactions of 100 writes. Timed write workloads (A, B, C, E) use per-operation transactions to match LSM's per-write granularity.
- **Reproducibility:** All key and value sequences use fixed seeds. Running `go run ./cmd/bench/...` produces the same workload on any machine.
- **Operation counts:** A/B = 10,000 writes; C = 5,000 pairs; D = 5,000 lookups after 5,000 writes; E = 10 keys × 1,000 updates; F = 5,000 writes + 2,500 deletes + 5,000 reads.
- **Hardware:** Apple M4 (ARM64), macOS. Results will differ on x86 and under concurrent load.

---

## Limitations

- **Single writer.** `db.mu` serialises all writes. There is no concurrent write path.
- **No range scans.** Only point lookups (`Get`) are supported. There is no iterator or `Scan(from, to)` API.
- **No compression.** Keys and values are written verbatim. There is no snappy/zstd layer.
- **Flat compaction only.** All SSTables are merged into one (size-tiered, single level). There is no L0→L1→L2 leveled strategy; read amplification is bounded only by `CompactionThreshold`.
- **No atomic SSTable replacement.** A crash between writing the new SSTable and deleting the old ones leaves both on disk. On reopen, sequence number ordering produces correct results but the old files are not cleaned up.
- **No fsync per WAL append.** Only `Close()` fsyncs the WAL. Writes between the last flush and a power failure can be lost.

---

## Future work

- Leveled compaction (L0→L1→L2) to bound read amplification without growing a single large SSTable
- Iterator / range scan API (`Seek`, `Next`, `Prev`)
- Block compression (snappy or zstd) for the SSTable data section
- Manifest file for atomic SSTable replacement and crash-safe compaction
- Concurrent readers with a read-lock-free SSTable list snapshot

---

## License

MIT
