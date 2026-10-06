# Distributed KV Cluster: Decisions

## Accepted Decisions

### D001 — Implement Raft in this repository

The portfolio goal is to understand and demonstrate elections, replication,
recovery, and partitions. Do not replace the core with etcd/raft. External
libraries may be used for transport, metrics, or protobuf support.

### D002 — Preserve the embedded interface

Existing callers and tests remain valid. Replica-specific behavior is additive
and cannot silently change embedded durability or ordering.

### D003 — Static three-node MVP

Every node starts with the same complete peer map. Membership changes and a
five-node deployment are deferred.

### D004 — Raft log is the replica WAL

Replica mode does not synchronously double-write every command to both an engine
WAL and a Raft WAL. The Raft index orders LSM records, and an atomic manifest
records the durably materialized prefix.

### D005 — ReadIndex-style linearizable reads

Leaders confirm authority with a quorum in the current term, capture the commit
index, wait for local application, and only then read the LSM state machine.

### D006 — Pre-vote and quorum checking are MVP behavior

Pre-vote limits term disruption. A leader that cannot confirm majority contact
within an election timeout steps down and rejects strong operations.

### D007 — Deduplicate client writes

The Go client assigns a stable client ID and monotonic request sequence. Retries
reuse them. The replicated state machine records the latest sequence/result with
the mutation so a delayed retry cannot overwrite newer state.

### D008 — Followers return leader hints

Followers do not proxy client calls. They return a typed `NotLeader` result with
the last known leader address; the client retries within its context deadline.

### D009 — Network MVP is point-operation only

Expose `Put`, `Delete`, `Get`, and `Status`. Keep embedded `Scan`; defer the
distributed scan consistency and pagination interface.

### D010 — Observability ships with the MVP

Expose Prometheus metrics and include provisioned Prometheus/Grafana resources in
Docker Compose. Performance statements must come from reproducible benchmarks.

### D011 — Logical LSM snapshots define the Raft compaction boundary

Snapshots serialize the live user and client-session namespaces at one committed
Raft index. The stable store atomically publishes the CRC-protected snapshot
before rewriting the log suffix. A lagging follower installs the snapshot into a
new SSTable generation through the manifest, then resumes AppendEntries at the
next index. This keeps snapshot format independent of host paths and obsolete
SSTable generations while preserving retry deduplication across recovery.

### D012 — Snapshot chunking belongs in the gRPC adapter

The deterministic core and runtime continue exchanging one logical snapshot
message. The production gRPC adapter streams that image in ordered chunks with a
declared total length and whole-image CRC. The receiver validates and reassembles
the complete bounded image before calling the runtime, so network framing does
not enlarge the consensus interface or affect in-memory fault tests.

### D013 — Membership changes use two replicated configurations

The leader appends `C_old,new`; elections, replication commit, ReadIndex, and
quorum-loss checks require majorities of both voter sets. Only after that entry
commits may it append `C_new`, which commits under the new voter set. One change
may be active at a time. Candidate node addresses must already exist in every
node's transport peer map; address discovery and peer-map mutation remain a
separate operational concern. A removed leader steps down after `C_new` commits.

### D014 — Snapshot bytes stream through adapters, not consensus

The deterministic Raft module continues to own snapshot index, term, membership,
and compaction decisions, but production snapshot bytes remain in the durable
store. The runtime streams state-machine export into atomic snapshot publication,
opens the durable image for outbound transport, and streams accepted inbound
images into state-machine replacement. The gRPC adapter stages and validates a
complete stream before delivering its metadata to Raft. Byte-backed snapshots
remain supported for deterministic core tests, but production paths must not
materialize the complete image in memory. Snapshot streams have a 64 GiB safety
limit; larger deployments require an explicitly revised operational limit.

### D015 — Peer addresses resolve outside replicated membership

Raft continues replicating voter IDs only. The cluster module resolves each ID
through a small peer-directory interface before transport use, membership
validation, status reporting, or leader hints. Static maps remain the default
adapter. An optional atomically replaced JSON directory is refreshed at runtime;
it overrides static entries and retains the last valid snapshot across transient
read or parse failures. The gRPC transport closes and recreates a cached
connection when an ID resolves to a different address. Address changes therefore
cannot add voters, change quorum arithmetic, or become committed state.

### D016 — Embedded manifest records the last published sequence number

Context: embedded `Open` rebuilt the sequence counter only from WAL records. A
flush empties the WAL, so after flush → close → reopen, new writes received
sequence numbers below data already in SSTables. `Scan` then returned the older
value, and compaction permanently resurrected it. This violated DESIGN invariant 1
("newer sequence number always wins"). Reproduced in phase 11 (finding A1).

Decision: `MANIFEST` moves to version 2 and gains `last_seq`: the highest sequence
number allocated to any record in the published SSTable generation.

- Embedded flush sets it to the memtable's highest allocated sequence; compaction
  carries it forward unchanged.
- `Open` seeds the sequence counter with `max(last_seq, max WAL seq) + 1`.
- For a version-2 manifest, WAL records with `seq <= last_seq` are skipped on replay.
  They are already in SSTables and reappear only if the post-flush WAL truncate was not
  durable.
- Version-1 and pre-manifest databases are upgraded inside `Open`, before it returns:
  1. Derive `last_seq` once from the maximum sequence in the live SSTables.
  2. If a WAL record is at or below that value, the database was already affected by
     the bug. Renumber every WAL record in WAL file order (the write order), starting
     above both the SSTable maximum and the WAL maximum.
  3. Flush. The flush writes the SSTable, publishes the version-2 manifest, and only
     then resets the WAL.
  4. Without that signature, publish version 2 with the derived `last_seq` directly.
  - No version-2 manifest can coexist with a WAL containing acknowledged records it
    would skip.
  - Failpoint tests cover a crash before the publish (the upgrade reruns) and after it
    (leftover WAL records keep their old numbers, all at or below `last_seq`, and are
    skipped).

Replica mode is unaffected. It never allocates sequence numbers: Raft indexes are
supplied to `ApplyBatch`, and snapshot replacement resets the counter to
`index + 1`. Its manifest becomes version 2 on the next publish, with `last_seq`
unused.

Consequences:

- Binaries older than this change reject version-2 manifests, so there is no
  downgrade. Verified against `bd67696`: `db.Open` on a version-2 database
  returns `manifest version 2 is unsupported`.
- Opening a legacy database scans its live SSTables once.
- SSTables already written with out-of-order sequences before this fix cannot be
  repaired in general; this is documented as a known limitation.
- Known ambiguity: in a legacy database, a WAL resurrected by a lost post-flush
  truncate (which is not synced) looks exactly like sequence reuse, because its
  records sit at or below the SSTable maximum. Such records are renumbered as new
  writes and can shadow newer SSTable data. This needs both a pre-version-2 lost
  truncate and an upgrade from that state. Version-2 databases are not affected:
  they skip such records by `last_seq`.

### D017 — Pre-vote rejections carry the responder's current term

Context: pre-vote requests carry a proposed term (`term + 1`), and responses echoed
it whether granted or rejected. A node at term T+1 with a stale log and a node at
term T with a newer log therefore rejected each other indefinitely while the third
voter was down. The lower-term node never learned the higher term, because pre-vote
traffic never propagates terms. This is a reproduced liveness failure (phase 11,
finding A2).

Decision: adopt the etcd rule.

- A granted pre-vote response still echoes the proposed term.
- A rejection carries the responder's current term.
- A node receiving a rejected pre-vote response with a term above its own becomes a
  follower at that term. This persists `HardState` before any further message is
  sent, through the existing persist-before-send runtime path.
- A rejection at the receiver's own term counts as a no-vote, as before.
- Pre-vote requests still never change the receiver's term, so the D006 guarantee that
  an isolated node cannot disrupt a healthy term is unchanged.

Consequences: a node can now adopt a higher term from a pre-vote rejection without
an election. That term already belongs to a live voter, and ordinary messages from
that voter would cause the same adoption.

### D018 — A success ack triggers a follow-up append only when it advances matchIndex

Context:

- `handleAppendResponse` sent the responder another append after *every* success
  ack, whenever `nextIndex <= lastIndex` (`internal/raft/node.go:632-635` at
  `8f07b48`). That included duplicate and stale acks.
- Each resend produced another ack, so append/ack chains sustained themselves for
  as long as any entry was unacked.
- Proposals, commit advances and heartbeats each started new chains.
- The official Linux baseline (2026-10-05, e2-standard-4, pd-ssd, 30 s window)
  measured 57–242 AppendEntries per committed entry. Throughput fell from 37 ops/s at
  1 client to 18–28 ops/s at 2–16 clients, with p99 near 700 ms.
- This is analysis-report rank 1 (phase 12a).

Decision:

- **Success acks:** a success ack causes a follow-up append to its sender only if it
  strictly advanced that follower's `matchIndex`, and only while
  `nextIndex <= lastIndex`.
- **Rejections:** they still lower `nextIndex` and probe immediately.
- **Heartbeats:** unchanged. They fire every `HeartbeatTicks` and ship the full
  suffix from `nextIndex`.
- **Unchanged:** proposal and commit-advance broadcasts, message sizing, `nextIndex`
  movement, ReadIndex probes, and the follower's echo of `Context`.

Consequences:

- **Termination:** each follow-up needs a strict increase in `matchIndex`, which is
  bounded by `lastIndex`. So no ack can start an unbounded chain. Follow-ups to a
  peer during a term number at most the entries appended in that term.
- **Catch-up after a lost append or ack:** `nextIndex` never advances
  optimistically, so the next heartbeat re-ships every unacked entry.
  - One lost append is recovered within `HeartbeatTicks` ticks, plus one delivery
    and one persist.
  - If the next k heartbeats to that follower are lost as well, recovery takes at
    most `(k + 1) × HeartbeatTicks` ticks.
  - Proposals and commit advances to that peer can recover it sooner.
- **Recovery now relies on heartbeats.** It relies on heartbeats carrying entries
  and on `nextIndex` moving only on responses. A later change to either, such as
  phase 12c's entry-free heartbeats, optimistic `nextIndex` or inflight window, must
  first provide another catch-up path. That path is a probe or a resend on timeout.
- **The `ack_resend` counter changes meaning.** It no longer counts duplicate-ack
  resends; it counts rejection probes and follow-ups after advancing acks.

### D019 — AppendEntries carry at most about 1 MiB, and followers copy and rescan their log only when it changes shape

Context:

- **A3, uncapped appends.** `appendMessage` shipped every entry from `nextIndex`
  to the last index in one message (`internal/raft/node.go:686-701` at `6d6dda7`).
  - With two or more large entries pending, every append to that follower,
    heartbeats included, exceeded the gRPC receive limit of 4 MiB + 64 KiB
    (`cluster/node.go:199-200`). Elections followed.
  - The analysis report measured 24 writes × 2.5 MiB at 4 clients: 12 elections.
  - This is report rank 4, finding A3.
- **4 MiB values never committed.** The Raft store rejects entries whose `Data`
  exceeds 4 MiB (`internal/raftstore/store.go:24`). A command with a 4 MiB value
  is larger than that, and the failed persist stops the leader's runtime. So the
  4 MiB values promised in the README could not commit.
- **The follower copied its whole log on every append.** Every accepted append
  deep-copied the retained log (`node.go:487-490`) and rescanned it for membership
  (`:517`).
  - This included entry-less heartbeats, so the cost grew with run length.
  - The copy is only the undo buffer for a failed membership rebuild. Membership
    rollback on truncation comes from the rebuild itself.
  - The phase-12a sweeps showed the effect. With the log unbounded, 1-client
    throughput fell from 183 to 120 ops/s between 10 s and 60 s. With the log
    bounded, it held at 193–209.

Decision:

1. **Byte cap.** `appendMessage` takes entries from `nextIndex` while the
   accounted size stays at or under 1 MiB, and always takes at least one entry.
   - Only tests can change the cap, through the unexported
     `Config.maxAppendBytes`. Production configs cannot set it, so they always
     run with the 1 MiB default.
   - An entry counts as `len(Data) + 32`, a bound on its protobuf framing.
   - The message envelope (at most 121 bytes) is not counted.
2. **One entry-size limit.** `raft.MaxEntryBytes` is 4 MiB + 32 KiB.
   - `Propose` rejects larger data with `ErrEntryTooLarge`, which the cluster maps
     to `InvalidArgument`.
   - The store uses the same limit.
   - The largest single-entry message, `MaxEntryBytes + 32 + 121` bytes, stays
     under the gRPC limit. A test pins that relation.
3. **Heartbeats carry a capped chunk.** They are not entry-less. They still ship
   from `nextIndex`, so D018's recovery path is preserved. A caught-up follower's
   heartbeat is still entry-less.
4. **`nextIndex` floor.** A rejection never lowers `nextIndex` below
   `matchIndex + 1`. Within a term, everything at or below `matchIndex` matches the
   leader's log, so such a rejection is stale.
5. **Follower append cost.** `handleAppend` no longer copies the log up front.
   - It keeps the old length, and a shallow copy of the tail only when a conflict
     truncates.
   - It rebuilds membership only if it truncated, or if an appended entry carries
     the membership prefix.
   - A failed rebuild restores the log and rejects, as before.

Consequences:

- **Catch-up now takes one chunk per round (this updates D018's bound).**
  - Let C be the number of capped chunks a follower lacks, R the number of
    rejection rounds needed to find its match point, and k the number of heartbeat
    rounds lost to it.
  - With follow-ups lost and only heartbeats delivering, the follower catches up
    within **(R + C + k) × HeartbeatTicks** ticks, plus one delivery and one
    persist of at most one chunk per round.
  - D018's (k + 1) × HeartbeatTicks is the case R = 0, C = 1.
  - With follow-ups delivered, the C chunks chain back to back with no heartbeat
    wait.
  - The floor (decision 4) is what makes every delivered round progress. Without
    it, a stale rejection could leave the follower re-acking a chunk it already
    holds, forever.
- **Termination is unchanged.** Each follow-up still needs a strict increase in
  `matchIndex`. Follow-ups per term are bounded by chunks, not entries.
- **The phase-11 commit rule is now load-bearing.** Capped messages routinely
  carry `LeaderCommit` beyond their last entry, so a follower commits at most
  `min(LeaderCommit, lastNew)`. Phase 11 (`f2fb1e6`) implements this, and a test
  guards it.
- **ReadIndex:**
  - Probes carry a capped chunk, which bounds the follower's persist time before
    it answers.
  - Acks still count by context and term, not by entries.
  - A rejected probe still drops its context (unchanged).
- **Duplicates in flight:** while a large chunk is unacked, heartbeats re-ship it.
  This is accepted until phase 12c adds inflight tracking. Uncapped heartbeats
  re-shipped everything.
- **Entry size:** a Put whose encoded command exceeds `MaxEntryBytes` now fails
  with `InvalidArgument` instead of stopping the leader.
- **Size margins:** a test pins them on a real `RaftMessage`.
  - The largest command the API accepts (16 KiB key, 4 MiB value, 64-byte client
    ID) encodes to 4,210,776 bytes, 16,296 under `MaxEntryBytes`.
  - Alone in an append, with every id, term, index and ReadIndex context at its
    widest varint, it serializes to 4,210,887 bytes, 48,953 under the gRPC limit.
- **Raising the store limit is not downgrade-safe.**
  - Recovery treats a record whose length exceeds the limit like a torn tail.
    `readLog` stops at that record (`raftstore/store.go:276-277`), and `Open`
    truncates `raft.log` at its start (`:66`). That record and every later one
    are discarded, whether committed or not.
  - So a binary from before this change (4 MiB limit) that opens a log holding
    an entry between 4 MiB and 4 MiB + 32 KiB rewrites the log without it. It
    happens on disk, before the Raft core starts, and upgrading again does not
    restore it.
  - If the state machine's applied index is past the cut, `raft.New` refuses to
    start ("applied index … outside snapshot/log range"), but the file is already
    truncated.
  - If not, the node starts without the discarded suffix and needs a leader that
    still holds it. If a majority is downgraded, committed entries are lost.
  - Do not run an older binary on data written by this one. The same rule
    applies to any later change to the limit.
- **Phase 12c:** entry-free heartbeats, an inflight window and reject echo of
  `LogIndex` stay in 12c. Each must keep a catch-up path that satisfies the bound
  above.

### D020 — The Raft log is versioned, and a binary refuses a log it cannot read instead of truncating it

Status: accepted 2026-10-05 (phase 12b hardening), conditional on the gate
below. If the gate fails, option B applies instead and this entry is amended to
say that pre-12b binaries are unprotected.

Context:

- **D019 made downgrades destructive.** Recovery treats every unexpected record
  as a torn tail. A record longer than the reader's limit, term 0, an index gap,
  a wrong first index or a CRC mismatch all end the read there, and `Open`
  truncates `raft.log` at that offset (`internal/raftstore/store.go:66, 276-296`).
  A binary from before D019 truncates a log holding an entry above 4 MiB, along
  with every later entry, committed or not.
- **A header alone cannot help binaries already built.** They parse the first
  bytes of `raft.log` as a record. A header there fails their checks at
  offset 0, so they would truncate the whole log to zero bytes.
- **Those binaries do fail cleanly on a directory.** They open `raft.log` with
  `O_RDWR|O_CREATE` (`store.go:56`). On a directory that returns "is a
  directory", and `Open` stops before reading or writing.
- **A node opens more than the Raft log.** At `bd67696`, `StartNode` opens the
  LSM state machine (`state/`) before the Raft store, so the guard protects the
  whole data directory only if that earlier open writes nothing. The gate checks
  exactly this.

Decision:

1. **The log moves to `raft-log.v1`.** It starts with a 16-byte header:
   - an 8-byte magic, `LSMDBRL\x00`;
   - a 4-byte big-endian format version, starting at 1;
   - a CRC32 of those 12 bytes.

   Records keep the existing encoding after the header.
2. **`raft.log` becomes an empty directory, the guard.** Binaries from before
   this decision then fail to open the store.
3. **A binary refuses, with an explicit error and no write:**
   - an unknown magic;
   - a version above the one it knows;
   - a record that is complete on disk, with a matching CRC, but longer than its
     entry limit.

   A short or CRC-failing final record is still a torn tail and is truncated.
4. **A headerless `raft.log` from before this decision is migrated on first
   open.** Each step is durable before the next:
   1. read the old log, with the rules above;
   2. publish `raft-log.v1` by tmp, fsync, rename and directory fsync;
   3. read `raft-log.v1` back and compare its records byte for byte with the
      old log's verified records, stopping with an error on any difference;
   4. rename `raft.log` to `raft.log.premigration`, then fsync the directory;
   5. create the guard, then fsync the directory.

   An interrupted migration resumes from the files present. Any layout that
   cannot be explained as an interrupted migration is an explicit error that
   names the files, and nothing is touched.
5. **`raft.log.premigration` is never deleted automatically.** It is the
   pre-upgrade log, kept for the operator, who removes it once the upgrade is
   trusted.
6. **Any later change to the record encoding or to `raft.MaxEntryBytes`
   increments the version.**

Gate, before item 3 is committed:

- Build `lsmdb-node` from `bd67696` and run it twice:
  - against a freshly migrated data directory;
  - against one created and used by the new code.
- Compare every file's hash and mtime, and every directory listing, before and
  after each run.
- **Passing:** the old binary refuses to start, and nothing other than the
  guard path changes. Any other change, including a manifest timestamp, fails
  the gate, and option B applies.

Consequences:

- **Upgrade:** nodes can be upgraded in any order, one at a time. The first open
  rewrites the retained log once and reads it back. The retained log's size is
  bounded by the snapshot threshold.
- **Disk:** `raft.log.premigration` keeps a copy of the pre-upgrade log until an
  operator removes it.
- **Downgrade below this decision:** the older binary refuses to start, with
  "is a directory". There is one exception: after a crash between migration
  steps 4 and 5, an older binary sees no `raft.log` and creates an empty one.
  Then either `raft.New` refuses (the applied index is above the snapshot
  index), or the node starts with an empty log. The versioned file is untouched
  either way.
  - On the next guard-aware open, an empty `raft.log` file is removed and the
    guard created.
  - A non-empty one is an explicit error that names the files.
- **Downgrade between versions that both have the guard:** the older binary
  refuses the newer version.
- **Not covered:**
  - `SNAPSHOT` and `HARDSTATE` stay unversioned.
  - A mid-file anomaly other than an over-limit record still truncates.
  - There is no supported downgrade path: a refused node needs its newer binary
    back.

## Decision Changes

Add a new numbered entry explaining the reason and consequences instead of
rewriting an accepted decision without history.
