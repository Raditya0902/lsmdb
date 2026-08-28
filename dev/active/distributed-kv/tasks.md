# Distributed KV Cluster: Task Tracker

Last updated: 2026-08-27

## Current Phase

Runtime peer-address discovery is complete and verified.

## Completed

- [x] Confirm existing repository structure and embedded interface.
- [x] Run baseline `go test ./...` successfully.
- [x] Add root `AGENTS.md` with invariants and working rules.
- [x] Add distributed KV context, plan, decisions, and task tracker.
- [x] Implement atomic JSON manifest publication with file and directory sync.
- [x] Make flush and compaction publish the live SSTable generation before cleanup.
- [x] Preserve embedded WAL behavior and public embedded operations.
- [x] Add replica durability mode with externally indexed atomic batch apply.
- [x] Persist/recover replica applied watermarks, including no-op indexes.
- [x] Add manifest and replica recovery/idempotency tests.
- [x] Implement deterministic Raft roles, pre-vote, election, replication, conflict repair, commit, and quorum loss.
- [x] Add CRC-protected Raft log/hard-state persistence and tail recovery.
- [x] Add the event-loop runtime with persist-before-send and ordered application.
- [x] Define protobuf contracts and check in generated Go code.
- [x] Add gRPC Raft transport, KV node, typed leader hints, and retrying Go client.
- [x] Add replicated client-session deduplication and ReadIndex-style reads.
- [x] Pass a real three-node write/read/failover/restart/convergence integration test.
- [x] Add node/CLI commands, Prometheus metrics, health endpoints, Docker Compose, Prometheus, and Grafana.
- [x] Add a faultable in-memory transport with partition, drop, delay, pause, and reorder controls.
- [x] Add repeated failover tests, Docker smoke automation, race/vet CI, and cluster benchmark.
- [x] Record measured local throughput/latency/failover results and update public design documentation.
- [x] Add atomic logical LSM snapshot export and replacement, including client-session metadata.
- [x] Add CRC-protected durable Raft snapshots and prefix log compaction.
- [x] Add snapshot-aware Raft indexing and `InstallSnapshot` follower catch-up.
- [x] Restore a newer durable Raft snapshot into the LSM state machine on restart.
- [x] Trigger snapshots after a configurable applied-entry threshold (default 1,000).
- [x] Expose snapshot index and retained-log length through status and Prometheus.
- [x] Replace unary snapshot transfer with ordered 1 MiB client-streaming chunks.
- [x] Validate stream metadata, offsets, total length, and whole-image CRC.
- [x] Reject interrupted, oversized, or inconsistent streams before Raft delivery.
- [x] Preserve the transport seam and deterministic Raft message interface.
- [x] Prevent overlapping outbound snapshot streams to the same peer.
- [x] Exercise multi-chunk installation in the three-node offline-follower test.
- [x] Encode replicated joint and final voter configurations.
- [x] Require both old and new majorities for elections, commit, reads, and quorum checks.
- [x] Persist membership through retained logs and Raft snapshot metadata.
- [x] Add leader-only `ChangeMembership` gRPC and Go client operations.
- [x] Support preconfigured non-voters through the `-voters` bootstrap option.
- [x] Reject overlapping changes and make completed identical requests idempotent.
- [x] Test add, restart, leader removal, continued writes, joint partitions, and snapshot restore.
- [x] Add an optional five-node Compose profile with a three-voter bootstrap configuration.
- [x] Document membership expansion, verification, removal, and restart behavior.
- [x] Stream logical LSM snapshot export and atomic replacement.
- [x] Stream durable Raft snapshot publication, loading, and recovery.
- [x] Stream outbound and receive-side gRPC snapshot data through bounded buffers.
- [x] Keep production snapshot bytes out of the deterministic Raft module.
- [x] Validate a 257 MiB stream with a 1 MiB buffer and a 64 GiB safety ceiling.
- [x] Add a peer-directory interface with static and refreshable file adapters.
- [x] Rotate cached gRPC connections when a resolved address changes.
- [x] Resolve addresses for startup voters, membership changes, status, and leader hints.
- [x] Add a node CLI option and documented atomic-update workflow.
- [x] Test adding a voter whose address was not statically preconfigured.

## In Progress

None.

## Phase 1 — Crash-Safe LSM Seam

- [x] Implement atomic manifest read/write/recovery.
- [x] Publish flush and compaction file sets through the manifest.
- [x] Preserve existing embedded WAL behavior.
- [x] Add externally indexed, atomic replica batch apply.
- [x] Add durable applied watermark and replay seam.
- [x] Add manifest, batch idempotency, ordering, and compatibility tests.
- [x] Run and record `go test ./...`, race tests, and vet.

## Phase 2 — Deterministic Raft Core

- [x] Define entries, messages, roles, configuration, actions, and status types.
- [x] Implement ticks, pre-vote, voting, elections, and heartbeats.
- [x] Implement append replication, conflict repair, commit, and leader no-op.
- [x] Implement quorum contact tracking and leader stepdown.
- [x] Add deterministic election, replication, and partition tests.

## Phase 3 — Durable Runtime

- [x] Add durable hard state and CRC-protected log store.
- [x] Persist before sending dependent RPC responses.
- [x] Apply committed entries in order through the LSM adapter.
- [x] Recover committed-but-unapplied entries after restart.
- [x] Test truncation, corruption, restarts, and catch-up.

## Phase 4 — gRPC Cluster

- [x] Add protobuf source, generation command, and checked-in Go output.
- [x] Implement internal Raft transport and external KV handlers.
- [x] Add static peer configuration and typed leader hints.
- [x] Implement retrying Go client with stable request identity.
- [x] Add replicated client-session deduplication.
- [x] Add ReadIndex-style linearizable `Get`.
- [x] Test failover retries, validation, and strong reads.

## Phase 5 — Operations

- [x] Add node executable and graceful shutdown.
- [x] Add Prometheus instrumentation.
- [x] Add three-node Docker Compose with persistent volumes.
- [x] Provision Prometheus and Grafana dashboard/health checks.

## Phase 6 — Faults, CI, and Benchmarks

- [x] Add faultable in-memory transport and cluster harness.
- [x] Cover minority, failover, heal, restart, and convergence scenarios.
- [x] Add bounded Docker failover smoke test.
- [x] Extend CI with race and integration checks.
- [x] Add cluster throughput/latency/failover benchmark.
- [x] Update README and design documentation with measured evidence.

## Verification Log

- 2026-08-26 — pre-change `go test ./...` — PASS.
- 2026-08-26 — Phase 1 `go test ./...` — PASS.
- 2026-08-26 — Phase 1 `go test -race ./...` — PASS.
- 2026-08-26 — Phase 1 `go vet ./...` — PASS.
- 2026-08-26 — Phase 2 deterministic Raft tests and race run — PASS.
- 2026-08-26 — Phase 3 stable-store truncation/recovery tests — PASS.
- 2026-08-26 — Phase 4 three-node failover and recovery integration test — PASS.
- 2026-08-26 — three-node failover/recovery integration, five consecutive runs — PASS.
- 2026-08-26 — final `go test ./...` — PASS.
- 2026-08-26 — final `go test -race ./...` — PASS.
- 2026-08-26 — final `go vet ./...` — PASS.
- 2026-08-26 — `docker compose config` — PASS.
- 2026-08-26 — `./scripts/docker-smoke.sh` with leader stop/restart — PASS.
- 2026-08-26 — local cluster benchmark, 1,000 × 128-byte writes — 98.0 ops/sec, P99 16.23 ms, failover 113.77 ms, 0 failures.
- 2026-08-26 — snapshot/log-compaction unit and crash-window recovery tests — PASS.
- 2026-08-26 — offline-follower `InstallSnapshot` catch-up test, three consecutive runs — PASS.
- 2026-08-26 — post-snapshot `go test ./...` — PASS.
- 2026-08-26 — post-snapshot `go test -race ./...` — PASS.
- 2026-08-26 — post-snapshot `go vet ./...` — PASS.
- 2026-08-26 — post-snapshot `docker compose config` — PASS.
- 2026-08-26 — post-snapshot `./scripts/docker-smoke.sh` leader failover/restart — PASS.
- 2026-08-26 — streamed snapshot validation tests — PASS.
- 2026-08-26 — multi-chunk offline-follower catch-up, three consecutive runs — PASS.
- 2026-08-26 — post-streaming `go test ./...` — PASS.
- 2026-08-26 — post-streaming `go test -race ./...` — PASS.
- 2026-08-26 — post-streaming `go vet ./...` — PASS.
- 2026-08-26 — post-streaming `docker compose config` — PASS.
- 2026-08-26 — post-streaming `./scripts/docker-smoke.sh` — PASS.
- 2026-08-27 — deterministic joint-majority, election, read-quorum, removal, and snapshot tests — PASS.
- 2026-08-27 — four-node add/restart/remove-leader integration, five consecutive runs — PASS.
- 2026-08-27 — post-membership `go test ./...` — PASS.
- 2026-08-27 — post-membership `go test -race ./...` — PASS.
- 2026-08-27 — post-membership `go vet ./...` — PASS.
- 2026-08-27 — post-membership `docker compose config` — PASS.
- 2026-08-27 — post-membership `./scripts/docker-smoke.sh` — PASS.
- 2026-08-27 — default and `five-node` profile `docker compose config` — PASS.
- 2026-08-27 — isolated five-node Compose expansion from voters 1–3 to 1–5; all nodes converged — PASS.
- 2026-08-27 — post-profile `go test ./...` — PASS.
- 2026-08-27 — post-profile `go test -race ./...` — PASS.
- 2026-08-27 — post-profile `go vet ./...` — PASS.
- 2026-08-27 — post-profile `./scripts/docker-smoke.sh` three-node failover/restart — PASS.
- 2026-08-27 — streamed LSM replacement and interrupted-publication tests — PASS.
- 2026-08-27 — generated 257 MiB receive stream through a 1 MiB buffer — PASS.
- 2026-08-27 — streamed offline-follower snapshot catch-up, three consecutive runs — PASS.
- 2026-08-27 — post-disk-streaming `go test ./...` — PASS.
- 2026-08-27 — post-disk-streaming `go test -race ./...` — PASS.
- 2026-08-27 — post-disk-streaming `go vet ./...` — PASS.
- 2026-08-27 — post-disk-streaming default and five-node Compose configuration — PASS.
- 2026-08-27 — post-disk-streaming `./scripts/docker-smoke.sh` — PASS.
- 2026-08-27 — peer-directory refresh and gRPC connection-rotation tests — PASS.
- 2026-08-27 — discovered four-node membership expansion, five consecutive runs — PASS.
- 2026-08-27 — post-discovery `go test ./...` — PASS.
- 2026-08-27 — post-discovery `go test -race ./...` — PASS.
- 2026-08-27 — post-discovery `go vet ./...` — PASS.
- 2026-08-27 — post-discovery default and five-node Compose configuration — PASS.
- 2026-08-27 — post-discovery `./scripts/docker-smoke.sh` — PASS.

## Blockers

None. Runtime discovery is an operator-managed JSON directory; an integrated,
authenticated registry remains deferred.

## Next Task

Design optional stale follower reads without weakening linearizable reads by default.
