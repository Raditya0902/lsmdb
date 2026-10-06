# Distributed KV Cluster: Task Tracker

Last updated: 2026-10-05

## Current Phase

Phase 12b hardening (six small fixes, D020) is in progress on branch
`phase-12b-hardening`, created from the phase-12b evidence commit `ca35b90`.

Phases 11, 10, 12a and 12b are complete on stacked branches:
`phase-11-correctness-fixes`, then `phase-10-benchmark-harness-v2`, then
`phase-12a-dup-ack-fix`, then `phase-12b-append-size-and-log-copy`. None is
merged into `main` (`bd67696`) yet.

Phase details are in `dev/active/phase-12b-hardening/`.

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

- Phase 12b hardening: Step 1 approved 2026-10-05. Commits in the order 0, 1, 2,
  5, 4, 6, 3; item 3 is gated on an old-binary check (D020).

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
- [x] Leader sends a follow-up append only after an ack that advances `matchIndex`;
  tests for duplicate acks, rejection probes, heartbeat catch-up, ReadIndex,
  check-quorum, and a bound on AppendEntries per committed entry (`56267a1`).
- [x] bench_compare passes machine and disk labels to arms whose clusterbench
  accepts them (`27ba9ce`).
- [x] Official Linux comparison and fix-arm duration sweep, judged against the
  thresholds pre-registered in `dev/active/phase-12a-dup-ack-fix/plan.md`. Results
  committed in `6d6dda7` (`benchmarks/results/2026-10-05-*`).
  - **Comparison:** the fix is meaningful at every client count. Appends/entry
    fell from 64–200 to 5.1–6.7, ops/s went from 21–38 to 137–151, and p99 at
    4 clients from 672 to 45.7 ms.
  - **Misses:** 8 of 10 thresholds pass. The duration-growth miss is explained by
    the retained log (flat with snapshots every 1000 entries). The 16-vs-1-client
    throughput miss is unexplained and deferred.

## Phase 12b — AppendEntries Size Cap and Follower Log Copy

Details: `dev/active/phase-12b-append-size-and-log-copy/`. Decision: D019.

- [x] Item 1: byte-capped AppendEntries (1 MiB, at least one entry), one entry-size
  limit (4 MiB + 32 KiB) across proposal, store and transport, and a `nextIndex`
  floor on rejection (`8c8a317`).
  - The report A3 run over localhost gRPC went from 16 elections to 0.
  - A 4 MiB value commits.
  - An oversized Put returns `InvalidArgument`, and the leader keeps serving.
- [x] Item 2: the follower copies and rescans its log only on truncation or a
  configuration entry. A heartbeat to a follower holding 100,000 entries went
  from 5.6 MB and 1.8 ms to 336 B and 0.5 µs.
- [x] Official three-arm VM comparison (base, cap, copy), copy-arm duration sweep
  and the large-value run, judged against the thresholds pre-registered in the
  phase plan. Results committed in `ca35b90`
  (`benchmarks/results/2026-10-05-p12b-*`).
  - **Passed:** item 1 small values; the item 2 sweep (−0.26% appends/entry,
    +2.19% ops/s from 10 to 60 s); item 2 throughput and p99 at every client
    count; p99.9 at 1, 2 and 16 clients; 0 invalid runs, 0 elections; about 3
    log syncs per entry.
  - **p99.9 at 8 clients: MISS** (copy min 106.89 > cap max 106.37 ms). Copy
    commits about 3× the entries per window, so this is not a like-for-like
    percentile. Compaction volume is the likely cause (correlational).
  - **p99.9 at 4 clients: INCONCLUSIVE.**
  - **Large values, cap arm alone: MISS, 0 of 5.** Copy 5 of 5; base 0 of 5 with
    15–19 elections. `8c8a317` and `18369e8` ship together; bisect skips
    `8c8a317`.

## Phase 12b Hardening

Details: `dev/active/phase-12b-hardening/`. Decision: D020 (accepted, gated).
Items in commit order:

- [x] The large-value test logs every node's term and commit index before
  failing.
- [x] Test timing under the race detector, without loosening any assertion or
  changing any timeout (the 4x tick scaling was tried and not committed):
  - (a) The in-memory chunking test waits until every node agrees on the
    leader before write 0 (`8d3a6fe`). Not reproduced: one failure in a full
    `-race` suite, fixed from reading `waitForMemoryLeader`.
  - (b) **A3 under the race detector** (`15cda40`): under `-race`,
    `TestLargeValueWritesKeepOneLeader` runs only with `LSMDB_RACE_STRESS=1`.
    The non-race run stays the gate. Lift the skip once the heartbeat
    prerequisite below is decided and implemented, and A3 passes under
    `-race` again.
  - The A3 failures are consistent with a transport backlog under race
    overhead. They are not shown to be caused by the race detector: 3 of 40
    against 0 of 40 is one-sided Fisher p ≈ 0.12 on its own.
- [x] Flush and compaction counted and timed in the engine; per-window deltas in
  clusterbench JSON (`bench_compare summarize` unchanged) (`e572346`).
- [x] `Runtime.Close` returns after the runtime has stopped itself (still nil)
  (`12547f5`).
- [x] Every entry of an incoming append validated before any in-memory change
  (index, term > 0, size, terms non-decreasing, at least `LogTerm`, at most the
  message term). A violation is dropped, counted and logged, with no reject
  (`f76d87e`).
  - The checks read only the message, so they run in `Step` before the term
    is adopted, the leader is recorded or the election timer is reset.
  - The plan had placed them after the log-match check. This follows the
    decision that every check runs before any in-memory change.
- [x] The client's message limit uses the server constant (`5fe0489`).
- [ ] Raft log format guard: versioned file, `raft.log` guard directory,
  refusal instead of truncation (D020), after the old-binary gate. Last and
  optional.

Findings from the A3 diagnostics (unfixed; line numbers at `e1123fc`):

- **Prerequisite decision for phase 13 Step 1, not work to start now:**
  heartbeats re-ship multi-MiB entries with no in-flight limit. A heartbeat
  is a full append from `nextIndex` (`internal/raft/node.go:700-715`), and
  `nextIndex` moves only on acks. Each send is its own goroutine with a
  500 ms deadline (`internal/raftnode/runtime.go:512-527`).
  - A lagging follower is sent the same 2.5 MiB entry every 20 ms tick: up to
    125 MiB/s per lagging follower.
  - Followers went up to 1,013 ms without leader contact. There were up to 204
    deadline failures per node per run.
  - Phase 13 Step 1 decides between entry-less heartbeats and an in-flight
    limit, lite or full. These are D019's phase-12c items, and each must keep
    a catch-up path that meets D019's recovery bound.
- **The vote handler has no lease check.** `handleVote` (`node.go:435-450`)
  has none, and `Step` adopts any higher term first (`:142-147`).
  `handlePreVote` has one (`:401-402`). In 2 of 15 elections a node voted 2.8
  and 9.6 ms after hearing the current leader.
- **A deposed leader's sends keep running for up to 500 ms**, because they
  are not cancelled on step-down. In one run, 43 sends from the old term were
  still expiring after the new leader took over.
- **The check-quorum window (5 ticks, 100 ms) is shorter than the election
  timeout (5–10 ticks).** A leader steps down when multi-MiB acks take longer.
  There were 2 step-downs in one run.
- **A buffered tick shortens an election timeout.** A 5-tick timeout fired
  83 ms after the timer reset: the runtime ticker can hold a tick that is
  delivered right after the reset.
- **The client retries ResourceExhausted on every node** (`cluster/client.go`
  `retry`): up to 60 attempts 25 ms apart. It then returns the last node's
  error, so an oversized response surfaces as "node is not the Raft leader".
  Found in item 6's receive-limit mutation.

## Phase 12b Vote Lease

Details: `dev/active/phase-12b-vote-lease/`. Branch `phase-12b-vote-lease`
from `658cfa0`. Decision: D021 (proposed).

- [x] Step 1 (docs): context, plan, tasks and D021. Re-verified `node.go`
  `:153-159`, `:413-414` and `:447-462` (at `e1123fc`: `:142-147`,
  `:401-402`, `:435-450`; the code is unchanged).
- [ ] Step 2: `inLease()` and the vote check in `Step`, tests (i)–(v) first,
  mutations for the guard tests. Waiting for approval.
- [ ] Secondary Mac check: `clusterbench -mode failover` before and after,
  interleaved, labeled secondary.

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

- 2026-10-05, phase 12b:
  - After each of `bc8e7c7`, `8c8a317` and the item 2 commit: gofmt clean,
    `go vet ./...` plus each script, and `go test -count=1 ./...` and
    `go test -count=1 -race ./...` pass.
  - The item 2 cost test passed 20 of 20 runs under `-race`, with time ratios
    0.50–1.08 against a bound of 3.
  - After `9213d83`, `f895b7e`, `0c9298e` and the results commit `ca35b90`: the
    same suite passes.

- 2026-10-05, phase 12b hardening:
  - After commit 0 (`966e951`, docs only): gofmt (no Go files), `go vet ./...`
    and `go test -count=1 ./...` pass. `go test -count=1 -race ./...` FAILED in
    `lsmdb/cluster` only: `TestMemoryClusterShipsLargeEntriesInCappedChunks`
    ("write 0: raft node is not leader") and `TestLargeValueWritesKeepOneLeader`
    (1 election, commit 26 on all nodes). The code equals `ca35b90`.
  - Re-run per the approved rule: `go test -race -count=10 ./cluster` passes, 10
    of 10 (139.9 s). Earlier, the two tests alone passed 20 of 20 under `-race`.
  - Item 1, on an `8c8a317` worktree under `-race`: the old test fails with only
    `status of node 1: context deadline exceeded`; the new one logs all three
    nodes (leader at commit 18, both followers' Status timing out, 0 elections
    among the 1 node that answered). The new file vets and lists on `6d6dda7`
    and `8c8a317`, and passes on the tip with and without `-race`.
  - Test timing, first attempt (stopped, nothing committed): A3 reproduced
    under 8 busy processes (3 of 10 failed, with 1–3 elections). A 4x tick
    scaling under a `race` build tag still had a 2-election run, so it stopped
    on stop condition 4. The in-memory failure did not reproduce in about 192
    runs.
  - (a) on `e1123fc`, before committing: gofmt, vet and `go test ./...` pass.
    `go test -race ./...` failed only in A3 (1 election). Under rule 1,
    `go test -race -count=10 ./cluster` passed 8 of 10, both failures A3 (11
    and 12 elections, 0 failed writes, equal commits), so it stopped there.
  - Control, A3 alone under `-race`, interleaved blocks of 10: `966e951` 0 of
    40, `e1123fc` 0 of 40 (load average 1.5–8.4). The rule needed at least 5
    failures, so it could not fire. At 0 of 40, each arm's true rate could
    still be up to about 7.5%.
  - Diagnostics, A3 alone, temporary and reverted. They recorded event-loop,
    persist and fsync times, elections, send failures and scheduler lateness.
    - With `-race`: 1 of 20 failed at ambient load, 2 of 20 with 8 busy
      processes.
    - Without `-race`: 0 of 40.
    - Worst stalls with `-race`: persist 189 ms, fsync 47 ms, event-loop
      iteration 420 ms, tick gap 733 ms, inbound queue wait 604 ms, scheduler
      107 ms late. Without `-race`: persist 27 ms, fsync 26 ms.
    - In the 500 ms before the election analysed in detail, no stall reached
      50 ms on any node.
    - Every pre-vote grant came from a node whose lease had lapsed, and the
      initiators rotated.
  - After (a) `8d3a6fe`: gofmt, vet, `go test ./...` and `-race` pass (17 ok
    each).
  - Before (b), under `-race` at ambient load, `-count=10`: the 4 MiB commit
    test and the oversized Put test, 10 of 10 each.
  - After (b) `15cda40`: `go test -race -count=10 ./cluster` passes all 10
    iterations (A3 skipped). `LSMDB_RACE_STRESS=1` runs A3 under `-race` (3 of
    3 pass). gofmt, vet, `go test ./...` and `-race` pass (17 ok each).
  - Items 2, 5, 4 and 6: each test failed first on the previous code.
    - Item 2: build failures. Item 6: two mutations (its test cannot fail on
      equal limits).
    - Item 5: "runtime 2 of 32: Close did not return within 2s".
    - Item 4: all 7 malformed cases were appended, and the conflict case also
      truncated; the runtime stopped on the store's refusal.
    - After each commit (`e572346`, `12547f5`, `f76d87e`, `5fe0489`): gofmt,
      vet, `go test ./...` and `-race` pass, 17 ok each. No rule-1 re-run was
      needed.
  - Item 2 secondary Mac check (not a result, not committed): `aedb0dd` vs
    `e572346`, 1 client, 5 repetitions, interleaved, 30 s windows.
    - Median ops/s 107.5 vs 105.8 (−1.6%), within ±5%: PASS. 10 of 10 runs
      valid.
    - In one counters-arm window: 9 flushes, 0.31 s of 0.41 s apply time.

## Blockers

None. Runtime discovery is an operator-managed JSON directory; an integrated,
authenticated registry remains deferred.

## Next Task

Vote lease (D021): Step 1 is done and waits for approval before Step 2. Phase
12b hardening item 3 (the log format guard) comes last and is optional.

The phase-11, phase-10, phase-12a and phase-12b branches are to be pushed as a
stack and merged with merge commits, in that order.
Optional stale follower reads remain deferred.
