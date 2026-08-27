# lsmdb — Design Document

This document explains the design decisions behind lsmdb at a level suitable for
understanding the trade-offs, not just the implementation.

---

## Why LSM trees

A B-tree keeps data sorted on disk in a tree of fixed-size pages. Every write must
locate the correct leaf page, modify it, and propagate any splits upward. For a
random-key workload this means one or more random disk writes per user write —
write amplification from the B-tree structure itself.

An LSM tree (Log-Structured Merge-tree) inverts the trade-off: all writes land in
memory first (the MemTable) and are only serialised to disk as large, sequential,
immutable SSTable files. Random writes become sequential appends. The cost is paid
on the read path and during periodic compaction:

| Property | B-tree | LSM tree |
|---|---|---|
| Write amplification | High (random page updates) | Low (sequential appends) |
| Read amplification | Low (single tree traversal) | Higher (multiple SSTables) |
| Space amplification | Low (in-place updates) | Higher (stale versions until compaction) |
| Write latency P50 | Higher (page I/O) | Lower (memory + sequential WAL) |

lsmdb targets the LSM side of this trade-off: optimise for write throughput and
low write latency, accept higher read amplification, and use Bloom filters to
claw back the read cost on the miss path.

---

## Write path

Every `Set` and `Delete` follows this sequence under `db.mu`:

```
1. AllocSeq()        — atomically reserve the next sequence number
2. WAL.Append()      — write the record to disk (type | seq | keyLen | valLen | key | value | crc32)
3. MemTable.SetRaw() — insert the record into the in-memory map
4. maybeFlush()      — if MemTable.Size() >= FlushThreshold, flush to SSTable
5. maybeCompact()    — if len(readers) >= CompactionThreshold, compact all SSTables
```

The WAL write happens before the MemTable update. If the process crashes after step 2
but before step 3, the WAL record survives and is replayed on the next `Open`. If it
crashes during step 2 (partial write), `ReadAll` detects the truncated record via CRC
and drops it.

**Sequence numbers** are monotonically increasing across the lifetime of the database.
`NewWithSeq(old.NextSeq())` ensures the post-flush MemTable continues the same counter,
so WAL records written after a flush always carry higher sequence numbers than any
SSTable record. This makes "newer seqNum wins" globally correct.

**WAL rotation** occurs after each successful SSTable flush: `wal.Reset()` truncates
the WAL file to zero. On reopen, WAL replay only reconstructs writes since the last
flush; everything before is in SSTable files.

---

## Read path

```
Get(key):
  1. Check MemTable (GetRecord, includes tombstones)
     - If found as PUT  → return value immediately
     - If found as DELETE → return not-found immediately
     - If absent        → fall through

  2. For each SSTable reader, newest first:
     a. Bloom filter MayContain(key)
        - false → skip this SSTable (no disk I/O)
        - true  → proceed
     b. Range check: key < minKey or key > maxKey → skip
     c. Binary search the sparse index → locate 16-record window
     d. Scan up to 16 records forward
        - hit (PUT)    → return value immediately
        - hit (DELETE) → return not-found immediately
        - miss         → try next SSTable

  3. Return not-found
```

The loop returns on the first definitive answer (PUT or DELETE) from any SSTable.
It does not scan older SSTables after a hit because the readers are ordered newest-first
and sequence numbers guarantee that the first hit is the authoritative version.

---

## SSTable format

Every SSTable is an immutable file with this layout:

```
┌─────────────────────────────────────────────────┐
│  Data records                                   │
│  ┌───────────────────────────────────────────┐  │
│  │ keyLen(4) │ valLen(4) │ seqNum(8) │ type(1) │ key │ value │
│  └───────────────────────────────────────────┘  │
│  (one record per key, sorted ascending by key)  │
├─────────────────────────────────────────────────┤
│  Metadata block                                 │
│  minKeyLen(4) │ minKey │ maxKeyLen(4) │ maxKey  │
├─────────────────────────────────────────────────┤
│  Bloom filter                                   │
│  numBits(8) │ numHash(8) │ bits...              │
├─────────────────────────────────────────────────┤
│  Sparse index                                   │
│  (one entry per 16 records)                     │
│  keyLen(4) │ key │ offset(8)  ×  N entries      │
├─────────────────────────────────────────────────┤
│  Footer (48 bytes, always at end of file)       │
│  metaOffset(8) │ bloomOffset(8) │ bloomLen(8)   │
│  indexOffset(8) │ indexLen(8) │ recordCount(8)  │
└─────────────────────────────────────────────────┘
```

**Footer-first opening.** A reader locates all sections with three `ReadAt` calls:
read the 48-byte footer at `size-48`, read the metadata block, load the Bloom filter
and index. No sequential scan of the data section is needed on `Open`.

**Sparse index.** One index entry is written every 16 records. A `Get` binary-searches
the index to find the largest indexed key ≤ the target, then scans forward at most 16
records. This bounds the worst-case scan length while keeping index memory footprint low.

**Immutability.** Once written, an SSTable is never modified. Compaction writes an
entirely new file and only deletes old files after the new one is complete.

---

## Bloom filter

Each SSTable embeds one Bloom filter sized at construction time for the exact number
of records it contains, at a target false-positive rate of 1%.

**Sizing formulas:**

```
m = -n · ln(p) / (ln 2)²     (optimal bit-array size)
k = (m/n) · ln 2              (optimal number of hash functions)
```

Where `n` = expected keys, `p` = false-positive rate (0.01). `m` is rounded up to the
next multiple of 64 (the `uint64` word size). `k` is clamped to [1, 30].

**Hash derivation (Kirsch–Mitzenmacher double hashing):**

```
h1 = FNV-1a(key)
h2 = rotate_left(h1, 17) | 1    // forced odd: visits all residues mod numBits
position_i = (h1 + i·h2) % numBits    for i = 0 … k-1
```

A single FNV-1a invocation produces both hashes without a second hash function call.
The `| 1` ensures `h2` is always odd, which guarantees the sequence `(h1 + i·h2) mod m`
cycles through all residues for any `m` — preventing clustering in the bit array.

**False negative impossibility.** Every `Add(key)` sets `k` bits. `MayContain(key)`
checks the same `k` bits using the same deterministic hash. A key that was added always
has all `k` bits set, so `MayContain` always returns true. The benchmark confirms this:
1,000 inserted keys in `TestBloomNoFalseNegatives`, zero false negatives.

---

## Compaction

### Why compaction is necessary

Each MemTable flush appends one more SSTable. Without compaction:
- Read amplification grows by 1 for every flush (each miss probes one more file)
- Disk space accumulates stale versions of overwritten keys and dropped deletes
- Tombstones persist forever, shadowing values that no longer exist

### Size-tiered strategy

When `len(readers) >= CompactionThreshold` (default 4), all current SSTables are
merged into a single new file. This is called *size-tiered* compaction because all
files at the same "tier" (there is only one tier here) are merged together.

### K-way merge

```go
heap ordered by: (key ascending, seqNum descending)
```

Each SSTable contributes a sorted slice of records. A min-heap is seeded with the
first record from each source. On each pop:

1. The top entry is the record with the lexicographically smallest key (and highest
   seqNum among ties for that key) — it is the winner for that key.
2. All other heap entries with the same key are drained and discarded (lower seqNum
   = stale version).
3. The tombstone rule is applied: if `isBottomLevel=true` and the winner is a DELETE,
   drop it — no older SSTables exist that need this tombstone as a shadow.
4. Otherwise, emit the winner to the output.

### Tombstone safety

`isBottomLevel=true` means "we are compacting the entire known SSTable set." When
true, a DELETE tombstone serves no purpose: every older PUT for that key is already
present in the input set and will be discarded in step 2. Dropping the tombstone
is safe. If `isBottomLevel=false` (not used in Phase 5 but preserved for future
leveled compaction), tombstones must be kept to shadow older SSTables not included
in this merge.

### File lifecycle

```
1. Write merged SSTable to nextSST path (fully fsynced)
2. Open the replacement reader
3. Atomically publish a MANIFEST containing only the replacement
4. Replace db.readers with the new reader
5. Close and remove files from the old manifest generation
```

If the process crashes before step 3, recovery uses the old manifest and ignores the
unpublished output. If it crashes after step 3, recovery uses the replacement and
ignores any old files not yet removed. File membership never depends on a directory
scan after the initial migration of a pre-manifest database.

---

## Crash recovery

On embedded `Open(path, opts)`:

1. Load the published manifest and open its SSTables newest-first. If an older
   database has no manifest, bootstrap one from its existing files once.
2. Replay the WAL: `ReadAll` reads every record, skipping truncated trailing records
   and records with bad CRCs. After replay, the WAL is truncated to the last valid
   record boundary.
3. Apply WAL records to the MemTable via `SetRaw`, which honours sequence numbers:
   a later WAL record for the same key overwrites an earlier one.

**CRC truncation behaviour in detail.** The WAL record header is 17 bytes
(`type(1) | seq(8) | keyLen(4) | valLen(4)`). If `io.ReadFull` returns
`io.ErrUnexpectedEOF` while reading the header or body, the loop breaks (the record
boundary is lost — cannot safely skip). If a full record is read but its CRC does not
match, the record is skipped and reading continues at the next record (the boundary
is known). After the loop, `Truncate(validEnd)` removes any trailing garbage.

**Sequence number restoration.** WAL records carry their original `SeqNum`. `SetRaw`
advances `nextSeq` to `max(nextSeq, r.SeqNum+1)` when replaying. After replay, the
MemTable's counter is exactly where it was before the crash.

---

## Amplification factors

### Write amplification

For a single `Set`:

| Step | Writes |
|---|---|
| WAL append | 1 sequential write (header + key + value + CRC) |
| MemTable insert | 0 disk writes |
| SSTable flush (every `FlushThreshold` writes) | 1 full scan of MemTable + 1 sequential write of SSTable |
| Compaction (every `CompactionThreshold` flushes) | Read all current SSTables + write new SSTable |

In the worst case, a single key participates in every compaction until it is eventually
at the bottom level. Under size-tiered single-level compaction, each key is rewritten
O(1/CompactionThreshold) times on average across its lifetime.

### Read amplification

Worst case for a miss:

```
1 MemTable probe  +  N SSTable probes  (one per SSTable, after Bloom misses)
```

With `CompactionThreshold=4`, at most 3 SSTables accumulate before the next
compaction, bounding worst-case read amplification to 4 (MemTable + 3 SSTables).
The Bloom filter reduces the effective number of disk-touching SSTable probes
to near zero for true negatives (99% skip rate observed in benchmarks).

### Space amplification

Until compaction fires, up to `CompactionThreshold - 1` versions of a key can exist
across different SSTables (one per flush). After compaction, only the newest version
survives (or no version, if it was deleted). Space amplification is bounded by
`CompactionThreshold - 1` stale versions at any point in time.

---

## Correctness invariants

These rules are enforced throughout and must not be violated by any future change:

1. **Newer sequence number always wins.**
   SeqNums are assigned under `db.mu` before the WAL write, ensuring global
   monotonicity. `SetRaw` and the K-way merge both use SeqNum to resolve conflicts.

2. **DELETE tombstone beats any older PUT for the same key.**
   A tombstone has a higher SeqNum than the PUT it shadows (it was written later).
   The "newer SeqNum wins" rule makes tombstone priority automatic — no special case needed.

3. **Bloom filters must never produce false negatives.**
   `MayContain` returns false only when at least one of the `k` bit positions is 0.
   Since `Add` sets all `k` positions for every key, a key that was added can never
   trigger a false negative. This is a mathematical guarantee, not a probabilistic one.

4. **SSTables are immutable and manifest publication is atomic.**
   Flush and compaction create and sync new files, atomically publish the complete
   live file set through `MANIFEST`, then remove obsolete files. Recovery observes
   either the old or new generation, never an inferred mixture.

5. **WAL must be replayed before any reads on startup.**
   `Open` replays the WAL before returning the `*DB`. There is no code path that
   allows a read before replay completes. Violating this would surface stale data
   for keys written after the last SSTable flush.

---

## Replicated state machine

Replica mode uses the Raft log rather than the embedded WAL as its recovery
authority. Every committed Raft index is also the LSM sequence number. The LSM
manifest records the highest index represented by its published SSTable set, so
restart replays only committed log entries beyond that watermark.

The consensus module is deterministic and performs no networking or file I/O. A
single runtime event loop feeds it ticks and messages, persists emitted hard-state
and log effects, sends resulting messages, and applies committed entries in order.
The production adapters are gRPC and a disk-backed stable store; tests also use a
faultable in-memory transport.

### Snapshots and Raft log compaction

After a configurable number of applied entries, the state-machine adapter
serializes the live user keys and client-session records at the applied index.
The Raft core binds that image to the term at the same committed index. The stable
store publishes a CRC-protected `SNAPSHOT` file with file and directory sync,
then atomically rewrites `raft.log` with only entries after the snapshot index.
Publishing in this order makes an interrupted compaction retain harmless prefix
bytes instead of losing required state.

The consensus log uses the snapshot index/term as its virtual first entry. If a
follower rejects replication below that boundary, the leader sends the snapshot;
the follower persists it, atomically replaces its LSM generation, acknowledges
installation, and resumes normal append replication at the next index. Startup
restores a durable snapshot newer than the LSM manifest watermark before opening
the consensus core.

The gRPC adapter divides each logical snapshot message into ordered 1 MiB chunks.
Every chunk repeats immutable transfer metadata, and the receiver verifies exact
offsets, declared length, and a whole-image CRC before delivering the snapshot to
the runtime. Interrupted or inconsistent streams therefore cannot publish partial
state. Only one snapshot stream per peer may be in flight.

### Election and partition behavior

- Randomized election timeouts begin with pre-vote, which does not increment term.
- A candidate persists its new term and self-vote before requesting votes.
- A leader appends a no-op in its term and commits only current-term entries by a
  majority `matchIndex`; earlier entries become committed with that prefix.
- Followers reject mismatched prefixes with a conflict hint. Leaders move
  `nextIndex` backward and replace only uncommitted conflicting suffixes.
- Successful append responses track recent quorum activity. A leader that cannot
  confirm a majority within the configured interval steps down.

### Write and read guarantees

A write response is sent only after its entry is persisted by a majority,
committed, and applied to the leader's LSM state machine. Each command contains a
client ID and monotonically increasing request sequence. Session metadata is
applied in the same externally indexed batch as the user mutation, so an older
retry becomes a no-op even after restart or failover.

A `Get` is served only by a leader that has committed an entry in its current
term. It sends a contextual heartbeat, waits for a current-term quorum response,
captures the commit index, waits until that index is applied locally, and then
reads the LSM state. A partitioned leader therefore cannot serve a successful
linearizable read.

## Known limitations

- **Single writer.** `db.mu` is a single mutex covering the entire write path. There
  is no lock-free or MVCC read path.
- **Flat SSTable list.** All SSTables are at one logical level. Leveled compaction
  (L0→L1→L2) would bound both read amplification and space amplification more tightly
  for large datasets.
- **No fsync per WAL append.** `Close()` fsyncs the WAL file. Between the last flush
  and a power failure, recent writes can be lost.
- **No distributed range scans.** Embedded range scans exist, but the network
  interface intentionally exposes only point operations in the MVP.
- **No compression.** All bytes are stored verbatim.
- **In-memory snapshot images.** Transfer is chunked, but snapshot creation and
  receive-side reassembly are capped at 256 MiB rather than streamed through a
  temporary file.
- **Static membership and plaintext transport.** Dynamic membership, TLS, authentication,
  and rolling upgrades are deferred.
