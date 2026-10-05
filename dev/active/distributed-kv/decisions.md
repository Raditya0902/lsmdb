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

## Decision Changes

Add a new numbered entry explaining the reason and consequences instead of
rewriting an accepted decision without history.
