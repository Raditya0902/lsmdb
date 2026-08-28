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

## Decision Changes

Add a new numbered entry explaining the reason and consequences instead of
rewriting an accepted decision without history.
