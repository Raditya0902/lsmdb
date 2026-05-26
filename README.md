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

Measured on GCP e2-standard-2 (2 vCPU, 8 GB RAM, x86_64), Debian Linux 6.1, Go 1.22. Each workload runs against a fresh database.
SQLite uses `journal_mode=WAL` and `synchronous=NORMAL` — see [Methodology](#benchmark-methodology) for why.

```
Workload              Engine  Ops/sec  P50(ms)  P95(ms)  P99(ms)  Disk(KB)  BloomSkips  ReadAmp
--------              ------  -------  -------  -------  -------  --------  ----------  -------
A: Sequential Writes  LSM      33,972   0.003    0.009    0.022       934            0      1.0
B: Random Writes      LSM      52,005   0.003    0.007    0.018       911            0      3.0
C: Read-after-Write   LSM     112,100   0.003    0.006    0.018       467            0      2.0
D: Point Lookups      LSM      61,446   0.001    0.052    0.077       467        6,983      2.0
E: Update-Heavy       LSM     272,412   0.003    0.005    0.012       496            0      0.0
F: Delete-Heavy       LSM      62,969   0.006    0.050    0.078       296        1,980      1.0
G(8):  Conc. Reads    LSM      38,098   0.047    0.088    0.127       934            0      1.0
G(16): Conc. Reads    LSM      39,740   0.047    0.085    0.123       934            0      1.0
G(32): Conc. Reads    LSM      39,759   0.047    0.085    0.130       934            0      1.0
H: Range Scans        LSM       2,200   0.403    0.711    1.046       934            0      1.0
A: Sequential Writes  SQLite   14,436   0.032    0.076    0.140     1,072            0       —
B: Random Writes      SQLite   13,829   0.032    0.075    0.125     1,008            0       —
C: Read-after-Write   SQLite   23,713   0.029    0.064    0.095       544            0       —
D: Point Lookups      SQLite   63,659   0.013    0.028    0.049       544            0       —
E: Update-Heavy       SQLite   24,047   0.027    0.059    0.086        12            0       —
F: Delete-Heavy       SQLite   71,074   0.013    0.023    0.043       544            0       —
G(8):  Conc. Reads    SQLite   65,003   0.025    0.053    0.091     1,072            0       —
G(16): Conc. Reads    SQLite   74,483   0.022    0.045    0.076     1,072            0       —
G(32): Conc. Reads    SQLite   75,514   0.021    0.043    0.075     1,072            0       —
H: Range Scans        SQLite    6,021   0.148    0.252    0.480     1,072            0       —
```

**Interpretation.** On x86_64 Linux, the LSM engine's write advantage is 2.4–3.8× on workloads A and B (34k–52k vs 14k ops/sec), because writes land in the MemTable and WAL — sequential, in-memory operations — rather than updating a B-tree page in place. The update-heavy workload (E) is the extreme case: all 10 hot keys remain in the MemTable for the entire run, so compaction never fires and LSM reaches 272k ops/sec — 11× faster than SQLite's 24k. Point lookups (D) reach near-parity: 61k LSM vs 63k SQLite, as Bloom filters eliminate most unnecessary SSTable reads.

Concurrent reads (G) reveal a scaling asymmetry: SQLite throughput grows from 65k to 75k ops/sec as goroutine count increases from 8 to 32, while LSM plateaus at 38k–40k. The bottleneck is the MemTable's internal RWMutex, which serialises concurrent `SortedEntries` calls even for read-only paths.

Range scans (H) favour SQLite by 2.7× (6k vs 2.2k ops/sec). The LSM path must collect candidates from all SSTables, deduplicate by sequence number, and sort the results; a B-tree leaf-page scan is a simpler, cache-friendlier operation for this access pattern.

The Bloom filter performs as expected: 6,983 SSTable checks were skipped on workload D (99% skip rate), confirming that miss-path reads avoid disk I/O in the common case.

---

## Benchmark methodology

- **SQLite pragmas:** `PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL`. Both engines accept a narrow crash window (LSM only fsyncs on SSTable flush; SQLite only fsyncs on WAL checkpoint). This keeps durability levels equivalent.
- **SQLite write batching:** Pre-population steps use batched transactions of 100 writes. Timed write workloads (A, B, C, E) use per-operation transactions to match LSM's per-write granularity.
- **Reproducibility:** All key and value sequences use fixed seeds. Running `go run ./cmd/bench/...` produces the same workload on any machine.
- **Operation counts:** A/B = 10,000 writes; C = 5,000 pairs; D = 5,000 lookups after 5,000 writes; E = 10 keys × 1,000 updates; F = 5,000 writes + 2,500 deletes + 5,000 reads.
- **Hardware:** GCP e2-standard-2 (2 vCPU, 8 GB RAM, x86_64), Debian Linux 6.1. Results will differ on different hardware and under concurrent load.

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
