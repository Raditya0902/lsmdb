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

## Decision Changes

Add a new numbered entry explaining the reason and consequences instead of
rewriting an accepted decision without history.
