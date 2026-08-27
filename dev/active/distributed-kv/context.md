# Distributed KV Cluster: Context

## Current Engine

`lsmdb` is a Go 1.22 embedded LSM-tree store. Its write path is a CRC-protected
WAL followed by a memtable; memtables flush into immutable SSTables with Bloom
filters and a sparse index, and size-tiered compaction merges the complete file
set. The public interface is `db.Open`, `Set`, `Get`, `Delete`, `Scan`, and
`Close`.

The embedded WAL syncs on close rather than every append. SSTables and the
manifest are synchronously published, and obsolete files are deleted only after
the replacement generation becomes durable.

Baseline on 2026-08-26:

```text
go test ./...: PASS
git status --short: clean before Phase 0 files
```

## Target System

The first distributed release is a statically configured three-node cluster.
Clients send `Put`, `Delete`, and linearizable `Get` requests over gRPC. One Raft
leader orders mutations; a write succeeds only after majority persistence,
commit, and local application. Followers return a typed leader hint and the Go
client retries with the same request identity.

Embedded and replica modes intentionally use different durability authorities:

- Embedded mode retains the engine WAL and engine-assigned sequence numbers.
- Replica mode uses the durable Raft log as the WAL and the Raft index as the LSM
  sequence number. The LSM manifest records what prefix is durably materialized.

## Architectural Seams

The consensus module owns Raft behavior behind a small interface: propose,
linearizable read, status, and close. It depends on internal transport,
persistence, clock/tick, and state-machine seams. gRPC/filesystem adapters run in
production; deterministic in-memory adapters run in protocol and partition tests.

Consensus state is owned by one event loop. RPC handlers and timers enqueue input
instead of mutating term, role, log, or replication progress concurrently.

The state-machine adapter encodes user keys and client-session records into
separate namespaces. A committed command applies the user mutation and request
sequence as one externally indexed batch, making retries deterministic.

## Consistency and Failure Contract

- Successful writes are majority-persisted, committed, and locally applied.
- Successful reads are confirmed by a current-term quorum and wait for the
  confirmed commit index to apply.
- Minority partitions cannot acknowledge writes or linearizable reads.
- A leader steps down after it cannot confirm quorum for an election timeout.
- Pre-vote prevents an isolated follower from disrupting a healthy term when it
  reconnects.
- Uncommitted entries never reach the LSM state machine.
- Recovery loads durable Raft state and replays committed entries newer than the
  LSM applied watermark.
- A durable logical snapshot contains user data and client-session metadata at a
  committed index. Recovery restores it before replaying the retained log suffix.
- Snapshot publication precedes prefix deletion, so a crash may retain redundant
  log bytes but cannot lose the only durable copy of state.

## First-Release Limits

The MVP uses fixed three-node membership, plaintext local networking, single-key
operations, and leader-served point reads. It does not include transactions,
distributed scans, authentication, rolling upgrades, or dynamic membership.
Snapshot transfer is bounded to 256 MiB per image in this release.

## Deferred Evolution

Next: chunked snapshot streaming, a five-node profile, joint-consensus
membership, optional stale follower reads, sharding/multi-Raft, TLS, and
production operations.
