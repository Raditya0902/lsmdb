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

### D021 — A voter in lease ignores a higher-term vote request

Status: accepted 2026-10-05 (phase 12b vote lease). Implemented in `69aeaea`,
with guard tests in `020f665`.

Context:

- The A3 diagnostics logged 15 elections after the first leader. In 2 of
  them, a follower granted a pre-vote while its lease had lapsed. It then
  heard the leader again, and still voted at the higher term 2.8 ms and 9.6 ms
  later.
- `handlePreVote` checks the lease (`internal/raft/node.go:413-414` at
  `658cfa0`). `handleVote` does not (`:447-462`).
- `Step` adopts any higher term before either handler runs (`:153-159`).
- etcd ignores higher-term pre-vote and vote requests in lease under
  check-quorum.

Decision:

- **In lease** (`inLease()`, vote path only):
  `role == Leader || (leaderID != 0 && electionElapsed < ElectionTickMin)`.
  - A follower is in lease from processing an append or snapshot from the
    leader until `ElectionTickMin` of its own ticks have passed.
  - A pre-candidate, a candidate, a node that has just granted a vote and a
    node that has just adopted a term are never in lease (`leaderID == 0`).
  - A leader is in lease while it is leader. Its `electionElapsed` still does
    not advance; the role test replaces the reliance on that frozen counter.
  - A leader leaves its lease only by leaving the role: through check-quorum
    (at most `2 × CheckQuorumTicks` ticks after its last quorum contact),
    through removal from the voter set, or by adopting a higher term from a
    message it accepts.
- **Length: the minimum election timeout, not the node's randomized draw.**
  The randomized timeout spreads campaigns so that two nodes rarely start
  together; it is not a safety window. A node that drew a timeout near the
  minimum would leave its lease sooner anyway, so the minimum is the only
  length every node guarantees. As in etcd, the lease never extends past
  `ElectionTickMin` ticks (`TestVoteLeaseEndsAtMinimumElectionTimeout`).
- **Pre-vote is unchanged.** It keeps its own check,
  `leaderID != 0 && electionElapsed < electionTimeout` on the randomized draw.
  An in-lease receiver still replies with a rejection that carries its own
  term (D017). Pre-vote requests never adopt a term. A follower that knows a
  leader never reaches its randomized timeout without first becoming a
  pre-candidate, which clears the leader, so for followers that check
  reduces to `leaderID != 0`.
- **Where:** in `Step`, after the malformed-append check and before the
  higher-term adoption. A `MsgVote` with a term above this node's, received
  in lease, is ignored. The term, vote, leader, election timer and log stay
  unchanged, and nothing is persisted or sent. Same-term vote requests are
  handled as before.
- **Silent, not rejected:** any reply would carry the voter's lower term. The
  candidate answers a lower-term message through `rejectStale` at its higher
  term (`:772-786`), and the voter would adopt that term, leaving its lease
  within one round trip.
- **Counted:** `raft.Status.VotesIgnoredInLease`. There is no cluster metric.
- **With D017:** D017 adopts a term only from a rejected pre-vote response,
  which only a pre-candidate receives, and a pre-candidate is never in lease.
  An ignored vote sends nothing, so it never feeds that path. The A2 deadlock
  has no leader and therefore no lease; its test still elects, and still
  fails without D017's adoption.

Consequences:

- An election like the two observed fails instead of deposing a leader that a
  majority can still hear (`TestVoterInLeaseIgnoresHigherTermVote`).
- **Failover after a leader stop is unchanged.** A survivor grants a pre-vote
  only once its own timeout has cleared its leader, so by the time any vote
  request reaches it, it is out of lease. The worst delay the lease can add
  is `ElectionTickMin` ticks, which is no longer than one election timeout.
  The tests found none: over 20 seeds and three staggers, no vote request
  was ignored after the leader stopped, and every per-seed tick count was
  identical before and after.
- **A stuck leader cannot block an election.** A leader that loses its
  quorum steps down within `2 × CheckQuorumTicks` ticks, clears its leader
  and is then out of lease.
- **A node that really holds a higher term** still costs exactly one leader
  change. Its votes are ignored, but its append responses at that term make
  the leader step down. This is required for it to rejoin, and etcd does the
  same (`TestRejoiningNodeWithHigherTermCausesOneLeaderChange`).
- **Real-time lease length follows tick drift.** The lease is counted in the
  node's own ticks. A busy event loop drops ticks and lengthens it in wall
  time; a buffered tick shortens it by up to one tick.
- **Not changed:** heartbeats and in-flight limits (phase 13 Step 1, which is
  D019's phase 12c), the check-quorum window, and `rejectStale` answering
  stale responses.

### D022 — One outstanding append per follower, then leader batches and follower coalescing

Status: accepted 2026-10-05; built 2026-10-06; measured 2026-10-06 (VM,
P1–P7 all PASS; see "Results" below).
- **Built:**
  - rank 14 in `6aa09ce` and the in-flight limit in `53b5d70`, which
    together form the prerequisite arm;
  - leader batching in `9cad487`;
  - follower coalescing in `333eb24`;
  - shared read probes in `b7ac098`.
- **Final arm:** `a76f182`, which is `b7ac098` plus one test. Also on the
  branch: a non-behavioral leader and follower split of the sync counters
  (`46fed0f`) and a test of cap + 1 proposals (`88f232c`).
- **Measured:** the pre-registered VM comparison (P1–P7) and the
  large-value stage, results in `a503f42`
  (`benchmarks/results/2026-10-06-p13-*`). Nothing in this entry needed
  amending.
- **Details:** in `dev/active/phase-13-group-commit/`, with line numbers at
  `d0e7c26`.

Approved with these values:
- the resend timeout is 5 ticks;
- the caps are 256 entries and 1 MiB on leader and followers alike;
- the follower cap bounds one persist, not one message;
- rank 14 is part of the prerequisite.

Context:

- Every committed entry costs exactly 3.0 log syncs at every client count.
  The official copy arm (`18369e8`, `2026-10-05-p12b-compare.json`) ran at
  440.83 ops/s at 4 clients and 408.93 at 16, with 1.00 entries per sync.
- Heartbeats, proposal broadcasts and commit broadcasts all ship the capped
  chunk from `nextIndex` (`internal/raft/node.go:750-765`). Each send is its
  own goroutine with a 500 ms deadline (`internal/raftnode/runtime.go:526-555`).
  - A3 diagnostics: a lagging follower was sent the same 2.5 MiB entry
    every 20 ms tick, and saw up to 204 deadline failures per node per run.
  - Followers went up to 1,013 ms without leader contact.
  - Two check-quorum step-downs occurred in one run.
- D018 and D019 rely on heartbeats carrying entries for catch-up:
  (R + C + k) × `HeartbeatTicks` ticks. Entry-less heartbeats alone would
  leave a lost append unsent once proposals stop, because a non-advancing ack
  sends nothing (`:699-704`).

Decision:

1. **In-flight limit, lite (this is D019's phase 12c).**
   - At most one entry-carrying append is outstanding per follower.
   - It stays outstanding until one of these happens: an ack advances
     `matchIndex` to its last index, a rejection arrives, or 5 ticks pass
     (`inflightResendTicks`, unexported).
   - While one is outstanding, every append to that follower is entry-less
     at `nextIndex - 1` and carries `LeaderCommit`. This covers heartbeats,
     proposal and commit broadcasts, and read probes.
   - Otherwise the capped chunk is sent, as today.
   - Snapshots are unchanged: the gRPC adapter already allows one stream
     per peer.
   - **Rejected alternatives:**
     - Entry-less heartbeats alone (option A) still send about one copy of
       a slow chunk per tick, because a heartbeat ack overtakes the chunk.
     - A full window (option C) needs optimistic `nextIndex`, reject echo
       and a new proof of D018's termination.
2. **The write result carries the entry's term** (report rank 14). The write
   handler then stops queuing a status event per write
   (`cluster/node.go:301`).
3. **Leader batching.**
   - `Node.ProposeBatch` appends in input order and returns one `Update`.
     Each oversize item fails on its own.
   - The runtime drains queued events without blocking, up to 256 proposals,
     1 MiB accounted as in `cappedEntries`, or the queue size, and always
     takes at least one proposal.
   - Other events drained go into a FIFO, handled in arrival order after the
     batch.
   - No linger. One `Persist` per batch. Each request completes once, at
     its own index.
4. **Follower coalescing.**
   - Queued appends from the same leader and term are stepped and merged,
     with the same caps.
   - Merging stops at an update with hard state, truncation, a snapshot or
     a role change: the prefix is persisted first, then that update alone.
   - One `Persist`, then every response is sent (none dropped, because they
     carry ReadIndex contexts), then every RPC completes.
5. **Reads drained together share one probe context.**

As built in `53b5d70` (where it differs from the proposal):

- **Waiting instead of empty appends:** a proposal sends nothing to a
  follower with an outstanding append, rather than an entry-less append, and
  the D018 follow-up is skipped when the commit advance just carried the next
  chunk. Entry-less appends are sent only where they carry something:
  heartbeats, commit advances and read probes.
- **Resend timing:** the resend goes out on the tick the append expires. If
  no heartbeat fires on that tick (`HeartbeatTicks` > 1), it is sent on its
  own, so the bound does not depend on `HeartbeatTicks`.
- **Copies are counted per tick.** A send between two ticks belongs to the
  earlier tick. In wall time, the first copy of a chain can overlap a sixth
  copy by the part of a tick between its send and the next tick, and the
  count follows tick drift as D021's lease does.
- **Tests that pinned D018/D019's heartbeat path now measure the resend:**
  - the catch-up bounds;
  - the cap tests that read the first message after a loss;
  - the origin tags (no proposal appends while the no-op is outstanding);
  - the joint consensus test, which ticks until node 3's lost append is
    re-sent;
  - the metrics test, which waits for both followers to match before its
    Put.
- **A3 under `-race`:**
  - 10 of 10 passed at ambient load and 10 of 10 with 8 busy processes, with
    0 elections and 0 failed writes in each run;
  - the `LSMDB_RACE_STRESS` skip and its `raceEnabled` constant are removed.

As built in `b7ac098` (shared read probes):

- **Which reads share a probe:** every read queued when the leader handles
  one, drained without blocking. If earlier draining left events deferred,
  only the reads at the head of that list share it, so other events keep
  their order.
- **Late reads:** a read that arrives after a probe was sent gets its own
  probe (`a76f182` tests this).
- **Read index:** taken when the probe reaches quorum, as before (f). It is
  never below the commit index at the read's arrival.
- **Reads may pass queued proposals.** A read can be answered before
  proposals queued ahead of it. Each such proposal is unacknowledged, so
  the write is concurrent with the read and may be ordered after it. This
  is an argument; no linearizability checker exists, and phase 13 makes no
  linearizability claim.

Consequences (expected; the results step records what was measured):

- **Catch-up:**
  - Delivered rounds chain on acks as today.
  - A lost round waits up to `inflightResendTicks` (5) instead of
    `HeartbeatTicks` (1). D019's worst case becomes
    (R + C + k) × `inflightResendTicks` ticks, plus one delivery and one
    persist per round.
  - At most 5 copies of a chunk are in flight per follower, against 25.
- **Heartbeats are small while bulk data is outstanding,** so check-quorum
  and follower election timers no longer wait behind multi-MiB transfers. The
  A3 `-race` skip is lifted only if 10 of 10 runs pass.
- **Followers receive batched appends** even before follower coalescing.
  While an append is outstanding, entries accumulate, and D018's follow-up
  carries them.
- **Unchanged:**
  - persist before any dependent response;
  - commit on a current-term majority;
  - dedup in one `ApplyBatch` per index;
  - truncation only of uncommitted conflicts;
  - apply in the event loop.
- **Out of scope:**
  - async leader persistence;
  - leveled compaction;
  - snapshots, flush and compaction off the loop;
  - batched apply;
  - linger;
  - the full window;
  - reject echo;
  - an ordered sender.
- **Acceptance:** the pre-registered VM thresholds in the phase plan
  (P1–P7), at 1, 4 and 16 clients, on four arms:
  - storm `bfa2a77`, which shows the cumulative arc only and enters no
    threshold;
  - copy `18369e8`;
  - the prerequisite arm `53b5d70`, which splits the gain and enters no
    threshold;
  - the final arm `a76f182`.

  P7 requires the cluster tests, the in-memory concurrency test and A3 to
  pass at the final arm. A throughput gain with a correctness regression is
  a miss. Phase 13 claims no linearizability coverage; the checker is
  separate work after phase 13.

Results (measured 2026-10-06, `benchmarks/results/2026-10-06-p13-compare.json`):

- **Setup:**
  - GCE e2-standard-4 with a 100 GB pd-ssd boot disk (network storage);
  - three nodes in one process on one disk;
  - closed-loop clients, 128-byte values;
  - official protocol (5 s warmup, 30 s window, 5 repetitions, arms
    interleaved).
  - 60 of 60 runs valid, 0 elections in any window, 0 failed operations.
- **Thresholds, final against copy (median [min, max]), all PASS:**
  - P2: 6.31 [6.29, 6.33] entries per log sync at 16 clients (0.47 syncs
    per committed entry, against 3.00).
  - P3: 1.62 [1.60, 1.64] at 4 clients, against copy's 1.00.
  - P4: 1531.7 [1477.1, 1575.7] ops/s at 16 clients against 419.4 [410.0,
    442.4], ×3.652, ranges disjoint.
  - P5: ×1.278 at 4 clients, ranges disjoint.
  - P6: 1-client p50 ×0.997 and throughput ×1.006.
  - P1 and P7 also pass. A3 passed 5 of 5 on the prerequisite and final arms
    on the VM, with no hangs.
- **Attribution:** the headline is "from `18369e8` to the final arm"; that
  range also holds D020 and D021. Against the prerequisite arm, batching is
  credited at 16 clients only: ×2.910 throughput, ranges disjoint. At 4
  clients (×1.109) and 1 client (×0.973) the ranges overlap, so nothing is
  credited there.
- **Final arm, leader and follower entries per sync:**
  - 1.13 and 2.06 at 4 clients;
  - 4.03 and 8.82 at 16 clients.
  The followers coalesce more than the leader batches.
- **Caveats:**
  - **Tail above p99 at 16 clients:** final's p99.9 is 363.2 [352.1,
    385.2] ms and its max 412.2 ms, about twice copy's 174.6 and 201.0 ms,
    while its p99 is 19.7 against 75.4 ms. The cause is unknown. No claim
    is made beyond p99.
  - **Deadline failures:** one final run (16 clients, repetition 3) had 5
    deadline send failures, against an expected 0, with 0 elections and 0
    failed operations.
  - **Platform dependence:** the gain depends on what a log sync costs. The
    pd-ssd here is network storage. Its 4 KiB fsync p50, sampled before
    and after each run, was 1.40–2.98 ms (median 1.73) across all 120
    samples. A disk with a cheaper or a more expensive fsync, another
    platform, or a multi-machine cluster would give different numbers.
    These numbers are not comparable to the Mac or to other platforms.
  - **P7's `-race` evidence** comes from the Mac at `21be5d5`, not the VM.
  - **A3 on GitHub runners:**
    - **Where P7's A3 was measured:** on the 4 vCPU VM (5 of 5 per arm)
      and on the Mac.
    - **Runner results:** on GitHub-hosted runners (ubuntu-24.04, Go 1.22.0
      from `go.mod`), A3 failed 2 of 4 CI runs at the final arm's code,
      with 2 and 3 elections and 0 failed writes. Both runs on `0ea54ac`
      passed; both on `a0659aa` failed, and that commit's diff does not
      touch A3. The logs do not report the runner's CPU count.
    - **Diagnosis, inconclusive:** not reproduced locally, 0 of 30 runs
      under `-race -cpu 2` (Go 1.26.1, load average 2.3–3.2), so no
      stall, check-quorum or campaign diagnostics were collected.
    - **Claim wording:** "A3 held with 0 elections on the VM
      (e2-standard-4, pd-ssd) and on the Mac. No claim is made for slower
      or shared machines: GitHub-hosted runners showed 2–3 elections in 2
      of 4 runs, with no failed writes."
    - **CI:** A3 is skipped when `CI=true` unless `LSMDB_RACE_STRESS=1`.
  - **Linearizability:** none claimed. Reads (shared probes) are covered by
    tests only, because clusterbench issues only writes.
  - **Attempt 1:** stage 1 first stopped at run 43 of 60 on a harness port
    race, before that run's measurement window. One re-run was approved, and
    the partial attempt is committed only with its INCOMPLETE label, not
    analyzed.

## Decision Changes

Add a new numbered entry explaining the reason and consequences instead of
rewriting an accepted decision without history.
