![Go Tests](https://github.com/Raditya0902/lsmdb/actions/workflows/test.yml/badge.svg)

# lsmdb

A Go key-value database with two intentionally separate modes:

- An embedded LevelDB-style LSM engine with a WAL, memtable, immutable SSTables,
  Bloom filters, sparse indexes, range scans, and size-tiered compaction.
- A three-node Raft-replicated database, expandable to five nodes through joint
  consensus, with quorum writes, linearizable reads, automatic failover,
  snapshots, recovery, gRPC APIs, Prometheus metrics, and Docker Compose.

Replica mode uses the embedded LSM engine as its state machine while keeping Raft
as the only ordering and write-durability authority.

---

## Requirements

- Go 1.22 or newer
- Docker with Compose v2 for the containerized cluster and observability stack
- `protoc` plus the Go protobuf plugins only when regenerating checked-in API code

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

// Range scan (returns all live keys where "hello" <= key <= "world")
pairs, err := d.Scan("hello", "world")
for _, kv := range pairs {
    fmt.Println(kv.Key, kv.Value)
}
```

Pass `""` as the path for an in-memory-only database with no WAL and no persistence.

**Options**

```go
opts := &db.Options{
    FlushThreshold:      1000, // memtable entries before an SSTable flush
    CompactionThreshold: 4,    // SSTable count before a compaction
}
```

## Distributed Raft cluster

The repository also includes a three-node database, with an optional five-node
profile, that uses the LSM engine as a replicated state machine. Peers resolve
through static maps or a refreshable runtime directory. The cluster implements
Raft pre-vote, leader election, heartbeats, quorum replication, conflict repair,
quorum-loss stepdown, persistent hard state/log recovery, retry deduplication,
and ReadIndex-style linearizable point reads. Durable logical snapshots bound
the retained Raft log and recover followers that fall behind the compacted
prefix.

```text
                         Put / Delete / Get / Status / Members
                                         │
                                         ▼
                                retrying gRPC client
                                         │
                         ┌───────────────┼───────────────┐
                         ▼               ▼               ▼
                    follower        Raft leader      follower
                  leader hint      persist + order   leader hint
                                         │
                         replicate entry / snapshot chunks
                              ┌──────────┴──────────┐
                              ▼                     ▼
                         follower log          follower log
                              └──────────┬──────────┘
                                         ▼
                          majority durable → commit → apply
                                         │
                            Raft index = LSM sequence number
```

### Consistency and failure guarantees

- A successful write is persisted by a Raft majority, committed, and applied to
  the leader's LSM state machine before the client receives success.
- A linearizable `Get` confirms a current-term quorum and waits for the confirmed
  commit index to apply before reading local state.
- A leader without quorum cannot acknowledge writes or linearizable reads and
  steps down after the configured quorum-check interval.
- Commands apply once, in committed log order. Client ID and request sequence are
  replicated atomically with each mutation so retries do not apply an older
  request twice.
- Recovery restores the newest durable logical snapshot and replays the retained
  committed log suffix. Uncommitted entries never reach the LSM state machine.
- Pre-vote limits disruption from isolated followers, and joint consensus
  requires majorities of both the old and new voter sets during reconfiguration.

### Durability modes

| Mode | Ordering and recovery authority | Sequence number | Sync behavior |
|---|---|---|---|
| Embedded | Engine WAL, manifest, and SSTables | Allocated by the engine | The WAL syncs on `Close`; durable manifest/SSTable publication is synchronized |
| Replica | Durable Raft log and snapshot | Committed Raft log index | Term, vote, and dependent log changes sync before protocol responses; snapshots publish before log-prefix deletion |

Start the complete cluster, Prometheus, and Grafana:

```bash
docker compose up -d --build

# Run the client inside the Compose network so leader hints resolve.
docker compose exec node1 lsmdbctl \
  -addresses=node1:7001,node2:7002,node3:7003 put hello world
docker compose exec node1 lsmdbctl \
  -addresses=node1:7001,node2:7002,node3:7003 get hello
```

- Node gRPC ports: `7001`–`7003` by default; `7004`–`7005` with the `five-node` profile
- Node metrics/health ports: `9001`–`9003` by default; `9004`–`9005` with the `five-node` profile
- Prometheus: <http://localhost:9090>
- Grafana: <http://localhost:3000> (anonymous viewer enabled)

For a manual deployment, run `go run ./cmd/lsmdb-node` once per node. Important
node flags are:

| Flag | Meaning |
|---|---|
| `-id` | Positive, stable Raft node ID |
| `-listen` | gRPC listen address |
| `-metrics` | HTTP address serving `/healthz` and `/metrics` |
| `-data-dir` | Persistent Raft and LSM directory |
| `-peers` | Static `ID=host:port` mappings |
| `-voters` | Bootstrap voters for a fresh data directory; defaults to static peer IDs |
| `-peer-file` | Optional refreshable JSON peer directory |
| `-peer-refresh` | Minimum peer-directory reload interval; default one second |
| `-snapshot-threshold` | Applied entries between snapshots; default 1,000 |

All nodes in a fresh cluster must use the same bootstrap voter set. A restarted
node recovers membership from durable Raft state rather than reapplying
`-voters`. `SIGINT` or `SIGTERM` triggers graceful shutdown of the gRPC server,
Raft runtime, stable store, and LSM state machine.

Run the automated leader-failure exercise with:

```bash
./scripts/docker-smoke.sh
```

The public gRPC interface is defined in
[`api/lsmdb/v1/lsmdb.proto`](api/lsmdb/v1/lsmdb.proto):

| RPC | Semantics | Result |
|---|---|---|
| `Put` | Quorum-replicated mutation with client retry identity | Raft term and committed log index |
| `Delete` | Quorum-replicated tombstone | Raft term and committed log index |
| `Get` | Leader-served linearizable point read | Found/value and confirmed read index |
| `Status` | Local role, indexes, snapshot, peers, and voter configuration | Diagnostic state |
| `ChangeMembership` | Leader-only joint-consensus voter replacement | Final configuration log index |

Keys must contain 1 byte through 16 KiB and values may contain at most 4 MiB.
Followers return a typed leader hint; the Go client retries through leader
changes while retaining the same client ID and request sequence. The `lsmdbctl`
commands are `put`, `delete`, `get`, `status`, and `members`.

Membership changes use Raft joint consensus. Every candidate node ID must resolve
through either `-peers` or `-peer-file` before the change begins; use
`-voters=1,2,3` to distinguish the bootstrap voter set from non-voters that are
only addressable.

The optional Compose profile starts nodes 4 and 5 with all five addresses
preconfigured, but leaves the replicated voter set at nodes 1–3:

```bash
docker compose --profile five-node up -d --build

# Ask any reachable node; the client follows leader hints.
docker compose exec node1 lsmdbctl \
  -addresses=node1:7001,node2:7002,node3:7003,node4:7004,node5:7005 \
  members 1,2,3,4,5

# Verify every node reports voter_ids 1–5 and no joint_voter_ids.
for node in 1 2 3 4 5; do
  docker compose exec -T "node$node" lsmdbctl \
    -addresses="node$node:700$node" status
done
```

Only the leader accepts a change. `C_old,new` must commit under both majorities
before `C_new` is appended, and only one transition may run at a time. The
command returns only after the final configuration commits locally. To remove a
voter, submit the complete desired set, for example `members 1,2,3,4`; after all
remaining nodes report that final set, stop node 5 with
`docker compose stop node5`. Never stop a voter before removing it when doing so
would eliminate the quorum required by either side of the joint configuration.

Membership is durable in the Raft log and snapshots. Restarting containers does
not reapply `-voters`; that flag is only the bootstrap configuration for fresh
data directories. Use `docker compose down -v` only when intentionally deleting
the cluster and starting again from voters 1–3.

Peer addresses can be discovered at runtime from an operator-managed JSON file:

```json
{
  "1": "node1:7001",
  "2": "node2:7002",
  "3": "node3:7003"
}
```

Start each node with `-peer-file=/config/peers.json`; `-peer-refresh` controls the
minimum reload interval and defaults to one second. Static `-peers` entries remain
available as bootstrap fallbacks, while valid file entries take precedence. To
change an address or introduce a candidate, write a complete new file and rename
it atomically over the configured path. Add a candidate address before submitting
`members`, and remove its address only after every remaining node reports the
final voter set. Invalid or temporarily unreadable updates retain the last valid
directory. Address discovery changes routing only—it never changes Raft voters or
quorum requirements. A file-only startup must pass `-voters` explicitly and the
directory must include the local node and every bootstrap voter.

Nodes snapshot every 1,000 applied entries by default. Override this for local
testing with `lsmdb-node -snapshot-threshold=N`. `Status` and Prometheus expose
the snapshot index and retained log-entry count. Snapshot creation, durable
publication, recovery, and installation stream through bounded buffers; the
development transport accepts images up to 64 GiB.

### Observability

Each configured metrics address serves `GET /healthz` and `GET /metrics`.
Prometheus collects Raft role, term, leader ID, commit/applied/snapshot indexes,
retained log length, per-peer replication lag, elections, leadership changes,
quorum-loss stepdowns, proposal outcomes, transport failures, and gRPC request
counts/latencies. Compose provisions Prometheus and a Grafana Raft dashboard.

### Cluster benchmark

`go run ./cmd/clusterbench` starts a fresh local three-node cluster, measures
replicated writes, stops the leader, and records time until a write succeeds
through the new majority leader. `-concurrency=N` uses N independent clients;
each client preserves its own retry identity and request sequence.

The following results are medians from five fresh-cluster runs per profile on
2026-08-27. The environment was an Apple M4 with 16 GiB RAM, macOS 26.5.2,
Go 1.26.1, loopback gRPC, local temporary directories, and 1,000 × 128-byte
writes per run.

| Profile | Throughput, median (range) | P50 | P95 | P99 | Failover, median (range) |
|---|---:|---:|---:|---:|---:|
| 1 client | 89.6 ops/s (88.1–91.8) | 11.26 ms | 16.06 ms | 17.26 ms | 296.42 ms (195.93–440.14) |
| 4 clients | 28.6 ops/s (22.0–31.3) | 86.99 ms | 374.26 ms | 426.66 ms | 284.41 ms (234.76–292.81) |

All 10 runs committed every measured write and completed the post-failure write.
Throughput covers the write workload before leader termination; failover is a
separate measurement. The four-client result shows that this implementation
does not scale concurrent writes: the single Raft event loop and per-update
durable syncs have no group-commit path, so queued writes increase disk sync and
tail-latency pressure. These are local development results, not production
capacity.

Reproduce each profile with:

```bash
for run in 1 2 3 4 5; do
  go run ./cmd/clusterbench -operations=1000 -value-size=128 -concurrency=1
done

for run in 1 2 3 4 5; do
  go run ./cmd/clusterbench -operations=1000 -value-size=128 -concurrency=4
done
```

Run the commands on the target machine before using the numbers in a résumé.

---

## Architecture

### Embedded write path

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
        K-way merge → new SSTable
                │
                └─► sync file + atomically publish manifest + delete obsolete files
```

### Embedded read path

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

### Replicated write path

```text
client Put/Delete
       │
       ▼
leader validates client ID + stable request sequence
       │
       ▼
append Raft entry → sync leader log → replicate
       │                              │
       │                     followers sync log
       └──────────── majority acknowledgment ────────────┐
                                                         ▼
                                             commit in current term
                                                         │
                                                         ▼
                              apply mutation + dedup metadata atomically
                                                         │
                                                         ▼
                                      publish applied Raft index to LSM
```

### Linearizable read and recovery path

```text
Get → leader confirms current-term quorum → waits for read index to apply → LSM Get

restart → load hard state + snapshot → restore LSM snapshot
        → replay committed retained entries newer than applied watermark

lagging follower below compacted prefix
        → receive ordered 1 MiB snapshot chunks
        → verify offsets, size, metadata, and whole-image CRC
        → sync staged snapshot → install atomically → resume log replication
```

---

## Components

| Component | Responsibility | Location |
|---|---|---|
| Embedded DB | Public `Open`, `Get`, `Set`, `Delete`, `Scan`, and `Close` API | `db/` |
| Memtable and WAL | Latest in-memory records plus CRC-protected embedded recovery log | `internal/memtable/`, `internal/wal/` |
| SSTables and Bloom filters | Immutable sorted data, sparse indexes, range checks, and negative-lookup filtering | `internal/sstable/`, `internal/bloom/` |
| Manifest and compactor | Atomic live-file publication and K-way size-tiered merging | `internal/manifest/`, `internal/compact/` |
| Replica state machine | Externally indexed atomic mutation/dedup batches and logical snapshots | `internal/kvstate/`, `db/replica_*` |
| Deterministic Raft | Elections, pre-vote, replication, reads, snapshots, and joint membership | `internal/raft/` |
| Raft runtime | Single-owner event loop, persist-before-send ordering, application, and snapshot triggers | `internal/raftnode/` |
| Stable store | Synced hard state, CRC-protected log, snapshots, recovery, and prefix compaction | `internal/raftstore/` |
| Transport adapters | gRPC transport with streamed snapshots and deterministic faultable in-memory networking | `internal/raftgrpc/`, `internal/raftnet/` |
| Cluster API | Node lifecycle, KV handlers, retrying client, peer discovery, and metrics | `cluster/` |
| Commands | Node server, client, cluster benchmark, and embedded benchmark | `cmd/` |
| Operations | Compose cluster, Prometheus, Grafana, health checks, and smoke automation | `docker-compose.yml`, `deploy/`, `scripts/` |

---

## Testing and verification

Run the release verification matrix from the repository root:

```bash
gofmt -w <changed-go-files>
go test ./...
go test -race ./...
go vet ./...
docker compose config
docker compose --profile five-node config
./scripts/docker-smoke.sh
```

The tests include embedded correctness, persistence, recovery, scans, compaction,
Bloom filters, externally indexed replica application, deterministic Raft
elections and partitions, durable-log corruption recovery, snapshot streaming,
joint-consensus membership, node restart/failover, and discovered-voter
integration coverage. GitHub Actions runs the Go verification and Docker smoke
jobs on pushes and pull requests.

Generated protobuf Go files are checked in. Edit the `.proto` source rather than
generated `.pb.go` files; the exact regeneration command is documented at the
top of [`api/lsmdb/v1/lsmdb.proto`](api/lsmdb/v1/lsmdb.proto).

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

- **Embedded single writer.** `db.mu` serialises embedded writes.
- **No Raft group commit.** Concurrent client proposals share one event loop and
  each persistence update syncs independently, so concurrent write throughput is
  currently worse than the single-client path.
- **No compression.** Keys and values are written verbatim. There is no snappy/zstd layer.
- **Flat compaction only.** All SSTables are merged into one (size-tiered, single level). There is no L0→L1→L2 leveled strategy; read amplification is bounded only by `CompactionThreshold`.
- **Orphan cleanup is deferred.** Manifest publication makes flush/compaction replacement atomic, but a crash before publication can leave an ignored SSTable file that is not yet garbage-collected.
- **No fsync per WAL append.** Only `Close()` fsyncs the WAL. Writes between the last flush and a power failure can be lost.
- **Operator-managed peer discovery.** Runtime JSON directories can add or
  change addresses without restarting nodes, but there is no integrated service
  registry or authenticated address distribution.
- **Snapshot size ceiling.** Snapshot images stream through disk and ordered
  1 MiB gRPC chunks instead of a full-image allocation, but the development
  transport rejects images larger than 64 GiB.
- **Point operations only over gRPC.** Distributed scans, transactions, and follower-stale reads are not exposed.
- **Development security model.** The demo cluster uses plaintext gRPC with no authentication or rolling-upgrade protocol.
- **Five-node profile is operational practice, not production guidance.** It
  exercises joint-consensus expansion locally; automated placement, upgrades,
  disaster recovery, and production five-node operations remain out of scope.

---

## Future work

- Leveled compaction (L0→L1→L2) to bound read amplification without growing a single large SSTable
- Block compression (snappy or zstd) for the SSTable data section
- Concurrent readers with a read-lock-free SSTable list snapshot
- Group commit and proposal batching for concurrent replicated-write throughput
- Optional stale follower reads without changing the default linearizable API
- Consistent-hash sharding or multi-Raft with explicit replica placement
- TLS, authentication, integrated service discovery, and rolling upgrades
- Distributed scans and multi-key transactions

## Further documentation

- [`DESIGN.md`](DESIGN.md) — storage formats, correctness boundaries, Raft
  behavior, snapshots, membership, and measured limitations
- [`api/lsmdb/v1/lsmdb.proto`](api/lsmdb/v1/lsmdb.proto) — public and internal
  gRPC contracts plus the reproducible generation command
- [`dev/active/distributed-kv/`](dev/active/distributed-kv/) — implementation
  plan, architectural context, accepted decisions, and verification history

---

## License

MIT
