# Distributed KV Cluster: Task Tracker

Last updated: 2026-10-05

## Current Phase

Phase 11 (correctness fixes A1, A2, A4, A5 from
`dev/active/analysis-report.md`) is complete on branch `phase-11-correctness-fixes`
and awaiting review. Phase 10 (benchmark harness v2) is next. Phase details:
`dev/active/phase-11-correctness-fixes/`.

## Completed

- [x] Confirm existing repository structure and embedded interface.
- [x] Run baseline `go test ./...` successfully.
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
- [x] Add independent-client concurrency and isolated deadlines to the cluster benchmark.
- [x] Record five-run serial and four-client benchmark medians and ranges.
- [x] Refresh the public benchmark environment, results, limitations, and discovery wording.
- [x] Expand the README with the complete v1 architecture, guarantees, APIs, operations, testing, and roadmap.

## In Progress

- Phase 12a: duplicate-ack resend fix (D018), branch `phase-12a-dup-ack-fix` from
  the phase-10 tip `8f07b48`.

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

## Phase 11 — Correctness Fixes

- [x] A4: followers commit only through entries the leader verified (`f2fb1e6`).
- [x] A1: embedded sequence numbers monotonic across reopen; manifest version 2
  with `last_seq` and legacy upgrade (D016) (`648bda8`).
- [x] A2: pre-vote rejections carry the responder's term (D017) (`c4f8c3a`); direct
  grant-counting and persist-order tests (`dba010f`).
- [x] A5: `ChangeMembership` completes when its configuration commits (`4d51087`).
- [x] Reopen property test at 500 operations per seed by default; long mode via
  `LSMDB_LONG_TESTS=1`; skipped under `-short` (`e656571`).

## Phase 12a — Duplicate-Ack Resend Fix

- [x] Send failures per measurement window, classed as deadline or other, recorded
  by clusterbench through the `cluster` transport wrapper. The bench_compare
  summary leads with total AppendEntries per committed entry (`ed9359d`).
- [x] D018 written; RUNBOOK notes that `ack_resend` changes meaning at the fix.
- [ ] Leader sends a follow-up append only after an ack that advances `matchIndex`;
  tests for duplicate acks, rejection probes, heartbeat catch-up, ReadIndex,
  check-quorum, and a bound on AppendEntries per committed entry.
- [ ] Official Linux comparison and fix-arm duration sweep, judged against the
  thresholds pre-registered in `dev/active/phase-12a-dup-ack-fix/plan.md`.

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
- 2026-08-27 — five fresh serial cluster-benchmark runs; median 89.6 ops/sec, P99 17.26 ms, failover 296.42 ms, zero failures — PASS.
- 2026-08-27 — five fresh four-client cluster-benchmark runs; median 28.6 ops/sec, P99 426.66 ms, failover 284.41 ms, zero failures — PASS.
- 2026-08-27 — race-instrumented four-client benchmark run — PASS.
- 2026-08-27 — v1.0.0 `go test ./...` — PASS.
- 2026-08-27 — v1.0.0 `go test -race ./...` — PASS.
- 2026-08-27 — v1.0.0 `go vet ./...` — PASS.
- 2026-08-27 — v1.0.0 default and five-node Compose configuration — PASS.
- 2026-08-27 — v1.0.0 `./scripts/docker-smoke.sh` — PASS.
- 2026-08-27 — expanded README link, ignore-state, and whitespace validation — PASS.
- 2026-08-27 — post-documentation `go test ./...` — PASS.
- 2026-08-27 — post-documentation `go vet ./...` — PASS.
- 2026-08-27 — post-documentation default and five-node Compose configuration — PASS.
- 2026-10-05 — phase 11: each fix's new tests FAIL on the unmodified code and PASS
  after the fix. Guard tests fail under deliberate mutations. Details in
  `dev/active/phase-11-correctness-fixes/tasks.md`.
- 2026-10-05 — phase 11 after every commit: gofmt (changed files), `go vet ./...`,
  `go test ./...`, `go test -race ./...` — PASS.
- 2026-10-05 — phase 11: the reopen property test (500 operations) FAILS on all four
  seeds at `f2fb1e6` (steps 58, 318, 116, 86) and PASSES after A1.
- 2026-10-05 — phase 11: a binary built from `bd67696` rejects a version-2 manifest
  with `manifest version 2 is unsupported`.
- 2026-10-05 — phase 11: cluster restart, snapshot-install and membership tests under
  `-race` — PASS.
- 2026-10-05 — phase 11: clusterbench at 1 and 4 clients, main vs branch,
  interleaved — no regression. This was a sanity check, not a benchmark result.

## Blockers

None. Runtime discovery is an operator-managed JSON directory; an integrated,
authenticated registry remains deferred.

## Next Task

Phase 12a: the duplicate-ack resend fix (`dev/active/phase-12a-dup-ack-fix/`), then
its official VM comparison. The phase-10 harness and baseline are done on branch
`phase-10-benchmark-harness-v2`, which is not yet merged.
Optional stale follower reads remain deferred.
