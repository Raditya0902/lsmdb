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
2. WAL.Append()      — one write of the record (type | seq | keyLen | valLen | key | value | crc32); no fsync
3. MemTable.SetRaw() — insert the record into the in-memory map
4. maybeFlush()      — if MemTable.Size() >= FlushThreshold, flush to SSTable
5. maybeCompact()    — if len(readers) >= CompactionThreshold, compact all SSTables
```

The WAL write happens before the MemTable update. `Append` hands the record to the
operating system in one `write` call and does not fsync; only `Close` syncs the WAL. A
record therefore survives a process crash after step 2, and is replayed on the next
`Open`, but not necessarily a power failure. If the process crashes during step 2
(partial write), `ReadAll` detects the truncated record via CRC and drops it.

**Sequence numbers** are monotonically increasing across the lifetime of the database,
including restarts. Within a process, `NewWithSeq(old.NextSeq())` makes the post-flush
MemTable continue the same counter. Across restarts, the manifest carries the counter.

A flush empties the WAL, so after a restart the WAL alone cannot say which sequence
numbers SSTables already use. Before manifest version 2, a reopened database restarted
at 1. A newer write then lost to older SSTable data in `Scan` and compaction, which both
resolve by sequence number.

Every embedded flush therefore publishes `LastSeq`, the highest sequence number in the
published SSTables, and `Open` resumes at `max(LastSeq, highest WAL sequence) + 1`.
WAL records written after a flush always carry higher sequence numbers than any SSTable
record, which makes "newer seqNum wins" globally correct.

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
has all `k` bits set, so `MayContain` always returns true. `TestBloomNoFalseNegatives`
checks this for 1,000 inserted keys.

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
3. Atomically publish a MANIFEST containing only the replacement (LastSeq unchanged)
4. Replace db.readers with the new reader
5. Close and remove files from the old manifest generation
```

Compaction carries `LastSeq` forward instead of recomputing it. A bottom-level merge
can drop the highest-sequence record (a tombstone), and the counter must never move
backwards.

If the process crashes before step 3, recovery uses the old manifest and ignores the
unpublished output. If it crashes after step 3, recovery uses the replacement and
ignores any old files not yet removed. File membership never depends on a directory
scan after the initial migration of a pre-manifest database.

---

## Crash recovery

On embedded `Open(path, opts)`:

1. Load the published manifest and open its SSTables newest-first. If an older
   database has no manifest, build its file set from the existing files.
2. Determine `LastSeq`: read it from a version-2 manifest, or derive it once from the
   highest sequence in the live SSTables for a legacy (version-1 or pre-manifest)
   database.
3. Replay the WAL: `ReadAll` reads every record, skipping truncated trailing records
   and records with bad CRCs. After replay, the WAL is truncated to the last valid
   record boundary. Records at or below `LastSeq` are already in SSTables and are
   skipped. They reappear only when the WAL truncate after a flush, which is not
   synced, was lost.
4. Apply WAL records to the MemTable via `SetRaw`, which honours sequence numbers:
   a later WAL record for the same key overwrites an earlier one.
5. For a legacy database, publish a version-2 manifest before `Open` returns (see
   below).

**CRC truncation behaviour in detail.** The WAL record header is 17 bytes
(`type(1) | seq(8) | keyLen(4) | valLen(4)`). If `io.ReadFull` returns
`io.ErrUnexpectedEOF` while reading the header or body, the loop breaks (the record
boundary is lost — cannot safely skip). If a full record is read but its CRC does not
match, the record is skipped and reading continues at the next record (the boundary
is known). After the loop, `Truncate(validEnd)` removes any trailing garbage.

**Sequence number restoration.** The MemTable counter starts at `LastSeq + 1`.
WAL records carry their original `SeqNum`, and `SetRaw` advances `nextSeq` to
`max(nextSeq, r.SeqNum+1)` when replaying. After replay, the counter exceeds every
sequence number stored in SSTables or the WAL.

**Legacy manifest upgrade.** Manifest version 2 adds `last_seq`. `Load` accepts both
versions and `Store` always writes version 2. A binary older than this format rejects
version 2 with `manifest version 2 is unsupported`, so there is no downgrade.

A legacy database whose WAL holds a record at or below the derived `LastSeq` was
written while sequence numbers restarted on reopen. Its WAL records were acknowledged,
so they are kept. They are renumbered above both the SSTable and WAL ranges in WAL
order (the write order), then flushed. That flush publishes version 2 and only then
resets the WAL:

- A crash before that publish leaves the legacy manifest, and the upgrade reruns.
- A crash after it leaves a WAL whose old numbers are all at or below the new
  `LastSeq`, so they are skipped.

A legacy database without that signature publishes version 2 with the derived
`LastSeq` directly.

---

## Amplification factors

### Write amplification

For a single `Set`:

| Step | Writes |
|---|---|
| WAL append | 1 sequential write (header + key + value + CRC) |
| MemTable insert | 0 disk writes |
| SSTable flush (every `FlushThreshold` writes) | 1 full scan of MemTable + 1 sequential write of SSTable |
| Compaction (after `CompactionThreshold` flushes, then every `CompactionThreshold - 1`) | Read all current SSTables + write new SSTable |

Every compaction merges the full SSTable set, so it loads and rewrites every live
record, not just the newest flushes. A compaction runs every `CompactionThreshold - 1`
flushes, and a key written early is rewritten by each one for the rest of the
database's life. For a workload of N distinct keys, total compaction work is about
N² / (2 · FlushThreshold · (CompactionThreshold - 1)) records, so write amplification
grows linearly with the live data size.

### Read amplification

Worst case for a miss:

```
1 MemTable probe  +  N SSTable probes  (one per SSTable, after Bloom misses)
```

With `CompactionThreshold=4`, at most 3 SSTables accumulate before the next
compaction, bounding worst-case read amplification to 4 (MemTable + 3 SSTables).
The Bloom filter skips an SSTable without reading its data when the key is absent
and the filter answers false. Filters are sized for a 1% false-positive rate, so by
design about 99 of 100 absent-key probes skip; no benchmark records the actual rate.

### Space amplification

Until compaction fires, up to `CompactionThreshold - 1` versions of a key can exist
across different SSTables (one per flush). After compaction, only the newest version
survives (or no version, if it was deleted). Space amplification is bounded by
`CompactionThreshold - 1` stale versions at any point in time.

---

## Correctness invariants

These rules are enforced throughout and must not be violated by any future change:

1. **Newer sequence number always wins.**
   SeqNums are assigned under `db.mu` before the WAL write, and a reopened database
   resumes above the manifest's `LastSeq`, ensuring global monotonicity across
   restarts. `SetRaw`, `Scan`, and the K-way merge all use SeqNum to resolve conflicts.

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
faultable in-memory transport. The runtime persists each update before it sends that
update's messages or applies its committed entries, so a leader syncs a new entry to
its own log before replicating it.

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
offsets, declared length, and a whole-image CRC while writing a staging file.
Only after the complete stream validates does the runtime deliver its metadata
to Raft, atomically publish the durable image, and stream it into a replacement
SSTable generation. Interrupted or inconsistent streams therefore cannot publish
partial state. The deterministic Raft core carries snapshot index, term, and
membership rather than production image bytes; the stable-store and transport
adapters stream those bytes through bounded buffers. Only one snapshot stream
per peer may be in flight.

### Election and partition behavior

- Randomized election timeouts begin with pre-vote, which does not increment term.
  A node grants a pre-vote only if it has not heard a leader within its own
  randomized timeout and the candidate's log is at least as up to date. A pre-vote
  request never changes the receiver's term, vote, leader or election timer.
- A granted pre-vote echoes the proposed term; a rejection carries the responder's
  current term. A pre-candidate that receives a rejection with a higher term becomes a
  follower at that term, so two nodes whose terms and logs disagree cannot reject each
  other forever.
- A candidate persists its new term and self-vote before requesting votes.
- A vote request is subject to a separate lease, never longer than the pre-vote
  check's window (see Vote lease). Outside it, a vote request with a higher term
  makes the receiver adopt that term; inside it, the request is ignored.
- A leader appends a no-op in its term and commits only current-term entries by a
  majority `matchIndex`; earlier entries become committed with that prefix.
- Followers reject mismatched prefixes with a conflict hint. Leaders move
  `nextIndex` backward, but never below `matchIndex + 1`: within a term everything up
  to `matchIndex` already matches, so a rejection below it is stale. Followers
  truncate only uncommitted conflicting suffixes, and reject an append that conflicts
  at or below their commit index.
- A follower commits at most `min(LeaderCommit, lastNew)`, where `lastNew` is the
  last index the append proved. A capped append often carries a `LeaderCommit` beyond
  its last entry, and the follower's entries past `lastNew` may be a stale suffix.
- Every append or snapshot response at the leader's term marks its sender as recently
  active, rejections included. Every `CheckQuorumTicks` ticks the leader checks that
  the recently active voters, with itself if it is a voter, form a quorum, and steps
  down otherwise. A leader that loses its majority therefore steps down within
  `2 × CheckQuorumTicks` ticks of its last contact.

### Vote lease

A node is in its vote lease while it is leader, or while it knows a leader and fewer
than `ElectionTickMin` ticks have passed since its election timer was last reset,
which every append or snapshot from that leader does. An append dropped as
malformed does not reset the timer, so it does not extend the lease. A vote request
with a higher term that reaches a node in lease is ignored before the term is
adopted: the term, vote, leader, election timer and log stay unchanged, and nothing is
persisted or sent. A reply would carry the voter's lower term, and the candidate's
stale-term answer would end the lease anyway.

The lease uses the minimum election timeout, not the node's randomized draw: the
randomized timeout only spreads campaigns, and the minimum is the only length every
node guarantees. Pre-vote keeps its own check, described above. Same-term vote
requests are handled normally. `raft.Status.VotesIgnoredInLease` counts ignored
requests; neither the gRPC `Status` RPC nor Prometheus exposes it.

### Replication flow control

An append carries entries from the follower's `nextIndex` while their accounted size
stays within 1 MiB, and always at least one entry; each entry counts as its data plus
32 bytes of framing. `raft.MaxEntryBytes`, 4 MiB + 32 KiB, is the single entry-size
limit: `Propose` and `ProposeBatch` reject larger data with `ErrEntryTooLarge`, the
stable store refuses to write it, and a follower treats an append carrying it as
malformed. The largest entry therefore still fits one message under the gRPC limit.

At most one append carrying entries is in flight per follower. It stays in flight
until an ack brings that follower's `matchIndex` to its last index, a rejection
arrives, or 5 ticks pass; an expired append is re-sent on the tick it expires. While
one is in flight, heartbeats, commit advances and read probes to that follower carry
no entries, only `LeaderCommit` and any read context, and new proposals send it
nothing. Entries proposed meanwhile travel together in the next append.

A success ack triggers a follow-up append only when it strictly advances the
follower's `matchIndex` and nothing is left in flight. A duplicate or stale ack proves
nothing new and sends nothing, and because every follow-up needs a strict increase in
`matchIndex`, append/ack chains always end. A rejection ends the in-flight append and
sends the probe at once.

On the follower, an accepted append keeps the old log length, and a copy of the tail
only when a conflict truncates it. Membership is rebuilt only when the append
truncated the log or carried a configuration entry; if the rebuild fails, the log is
restored and the append rejected.

`Step` checks each append before anything else. Entry `i` must have index
`LogIndex + i + 1`, a nonzero term that never decreases from `LogTerm` and never
exceeds the message term, and at most `MaxEntryBytes` of data. An append that fails
is dropped before any state changes, not even the term or the election timer, and
gets no reply, so a leader that sends only such messages is replaced. The drop is
logged, counted in `raft.Status.MalformedAppendsDropped` and exported as the
`lsmdb_raft_malformed_appends_dropped_total` metric.

### Batching and coalescing

The runtime's event loop handles one event at a time. When it takes a proposal, it
drains queued events without blocking and batches the proposals among them, up to 256
proposals or 1 MiB accounted as in the append cap. The batch is appended by one
`ProposeBatch` and persisted with one log sync. Other events drained meanwhile are
handled afterwards, in arrival order. There is no linger: a proposal never waits for
others to arrive. Each proposal completes at its own index once its entry commits and
applies, and its result carries the entry's term as well as its index.

A follower does the same with appends. Queued appends from the same sender, within the
same caps, are stepped and persisted with one sync, and their responses are sent
after it. An update that changes hard state, truncates the log, installs a snapshot or
changes role ends the merge: the merged prefix is persisted first, then that update
alone.

Reads queued together share one probe, described below. The README's
[Results](README.md#results) section reports what these mechanisms measured, and on
which platform.

### Joint-consensus membership

Membership commands are internal Raft log entries and never reach the KV state
machine. A leader first appends `C_old,new`. While that entry is active, election,
commit, ReadIndex, and quorum-loss decisions independently require majorities of
both voter sets. After it commits, the core appends `C_new`; that entry commits
under the new voter set. A leader removed by `C_new` continues replicating the
final entry but steps down as soon as it commits.

The active configuration is reconstructed from the immutable bootstrap voters,
the latest snapshot membership metadata, and the retained log suffix. This makes
uncommitted configuration entries survive restart and ensures conflict truncation
also rolls membership back. Consensus replicates IDs only. A peer-directory
interface resolves those IDs independently through static mappings or a
refreshable operator-managed JSON file. The gRPC adapter re-resolves before use
and rotates a cached connection if the address changes, without changing any
membership state.

### Write and read guarantees

A write response is sent only after its entry is persisted by a majority,
committed, and applied to the leader's LSM state machine. It carries the entry's log
index and term. Each command contains a client ID and monotonically increasing
request sequence. Session metadata is applied in the same externally indexed batch as
the user mutation, so an older retry becomes a no-op even after restart or failover.

A `Get` is served only by a leader that has committed an entry in its current
term. The leader sends every other voter a read probe: an append carrying a fresh
context, built like any other append, so it carries no entries while an append to that
voter is in flight. Reads drained from the queue together share one probe; a read that
arrives after the probe was sent gets its own. Once a current-term quorum has answered
the context and the leader has applied its commit index, each read on the probe
completes with that index, and the handler reads the LSM state. A partitioned leader
therefore cannot serve a successful linearizable read.

A read can complete before proposals queued ahead of it. Each such proposal is still
unacknowledged, so that write is concurrent with the read and may be ordered after it.
This ordering is argued, not tested: no linearizability checker exists.

Read completion on the probe path depends on the leader applying committed entries
in the same update, before acks are counted. Moving apply off the event loop requires
revisiting `acknowledgeRead` (`internal/raftnode/runtime.go:740-742`), which returns
without completing a read while the applied index is behind the commit index, and the
single-voter path (`runtime.go:395-396`), which completes at the commit index without
checking it.

## Known limitations

- **Single writer.** `db.mu` is a single mutex covering the entire write path. There
  is no lock-free or MVCC read path.
- **The Raft log format is not versioned.** Recovery treats a record longer than the
  binary's entry limit as a torn tail and truncates `raft.log` there, discarding every
  later entry, committed or not. A binary with a lower limit, such as one from before
  the 4 MiB + 32 KiB limit, must not open a log written by this one. Decision D020
  describes a versioned log with a guard against older binaries; that guard is not
  built.
- **Flat SSTable list.** All SSTables are at one logical level. Leveled compaction
  (L0→L1→L2) would bound both read amplification and space amplification more tightly
  for large datasets.
- **No fsync per WAL append.** `Close()` fsyncs the WAL file. Between the last flush
  and a power failure, recent writes can be lost.
- **Databases written before manifest version 2 may contain out-of-order sequence
  numbers.** Before version 2, a reopened database restarted its sequence counter at
  1, so its SSTables may hold a newer value with a lower sequence number than an older
  one. The upgrade cannot detect or repair such records after the fact.

  The legacy upgrade also cannot distinguish a WAL from a sequence-reuse write from a
  WAL resurrected by a lost post-flush truncate. If a legacy database has both, the
  resurrected records are renumbered as new writes.
- **The leader persists before it replicates.** It syncs each batch to its own log
  before sending it; asynchronous leader persistence is not implemented. At most one
  append carrying entries is in flight per follower; a wider window is not
  implemented. Committed entries are applied one index at a time in the event loop,
  and snapshots, flushes and compactions run there too.
- **No linearizability checker.** Read and write ordering is covered by tests only.
- **Unexplained tail latency.** The latency above p99 at 16 clients is unexplained;
  see the caveats in the README's [Results](README.md#results).
- **No distributed range scans.** Embedded range scans exist, but the network
  interface intentionally exposes only point operations in the MVP.
- **No compression.** All bytes are stored verbatim.
- **Snapshot size ceiling.** Creation, durable recovery, and transfer are
  disk-streamed, but the development transport rejects images larger than
  64 GiB.
- **Operator-managed discovery and plaintext transport.** A refreshable JSON
  directory supports runtime address changes, but an integrated registry, TLS,
  authentication, and rolling upgrades are deferred.
