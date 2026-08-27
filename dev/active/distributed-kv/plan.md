# Distributed KV Cluster: Implementation Plan

## Goal

Evolve the embedded LSM engine into a three-node Raft-replicated key-value
database while preserving the local library interface and keeping every phase
buildable and tested.

## Phases

1. **Continuity and baseline** — add repository guidance, context, decisions,
   plan, and the authoritative task tracker.
2. **Crash-safe LSM seam** — implement an atomic manifest, crash-safe file-set
   publication, replica mode, externally indexed batch apply, and durable applied
   watermarks.
3. **Deterministic Raft core** — implement roles, pre-vote/election, heartbeats,
   replication, conflict repair, current-term commit, leader no-op, and quorum
   checking independently of files and networking.
4. **Durable runtime** — persist hard state and a CRC-protected Raft log before
   dependent responses, apply committed entries in order, and recover nodes.
5. **gRPC cluster** — add internal Raft RPCs, external KV operations, static peer
   configuration, typed leader hints, retrying Go client, deduplication, and
   ReadIndex-style point reads.
6. **Operations** — add the node executable, graceful shutdown, Prometheus,
   Grafana, and a persistent three-node Docker Compose environment.
7. **Faults and evidence** — add deterministic partitions/crashes, Docker
   failover coverage, race CI, and reproducible throughput/latency/failover
   benchmarks; update public documentation with measured results.

## Network Interface

- `Put(key, value, client_id, request_seq) -> term, log_index`
- `Delete(key, client_id, request_seq) -> term, log_index`
- `Get(key) -> found, value, read_index`
- `Status() -> node_id, role, term, leader_id, commit_index, applied_index, peers`

Followers return a typed leader hint. Keys are at most 16 KiB and values at most
4 MiB. The demo transport is plaintext.

## Acceptance Criteria

- Existing embedded tests and interfaces remain valid.
- A three-node cluster elects one leader and commits through a majority.
- The minority cannot acknowledge strong operations.
- The majority elects a replacement after leader failure and remains available.
- A restarted or healed node catches up and exposes identical committed data.
- Linearizable reads do not return state older than their confirmed read index.
- Retrying the same client sequence across failover does not reapply an older
  mutation.
- Corrupt/truncated Raft tails recover to the valid prefix without applying an
  uncommitted entry.
- `go test ./...`, `go test -race ./...`, `go vet ./...`, and the Docker smoke
  test pass.
- Benchmarks record throughput, P50/P95/P99 latency, failed operations, and
  leader-failure recovery time with reproducible configuration.

## Deferred

Snapshots, `InstallSnapshot`, Raft log compaction, five-node deployment, dynamic
membership, distributed scans, stale follower reads, sharding/multi-Raft, TLS,
authentication, and rolling upgrades follow the MVP.
