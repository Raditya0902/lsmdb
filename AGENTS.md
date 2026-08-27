# Repository Agent Guide

## Purpose

This repository contains an embedded LSM-tree key-value engine and a three-node
Raft-replicated database built on top of it. Preserve the embedded library while
adding distributed behavior behind separate modules.

## Source of Truth

- `dev/active/distributed-kv/plan.md` contains the agreed implementation plan.
- `dev/active/distributed-kv/tasks.md` is the authoritative status tracker.
- `dev/active/distributed-kv/context.md` explains the architecture and invariants.
- `dev/active/distributed-kv/decisions.md` records decisions that must not be
  silently revisited.
- Update the task tracker after every meaningful batch and before ending work.
  Do not put changing status in this file.

## Architecture Invariants

- Embedded mode remains source-compatible: `db.Open`, `Set`, `Get`, `Delete`,
  `Scan`, and `Close` must continue to work.
- Raft is the only ordering authority in replica mode. Committed Raft indexes are
  the LSM sequence numbers; never allocate a second sequence for replicated data.
- A state-machine command is visible only after it is committed and applied in
  log order.
- Persistent term, vote, and log changes must reach stable storage before a node
  sends an RPC response that depends on them.
- A leader without quorum must not acknowledge writes or linearizable reads.
- Reads use a current-term quorum confirmation and wait for the confirmed commit
  index to apply.
- Client retries retain the same client ID and request sequence. Application and
  deduplication metadata are one ordered state-machine batch.
- Never truncate or discard a Raft entry that may be committed.
- Never publish an SSTable set or applied watermark before its files are synced.

## Module Seams

- Keep the consensus module deep. Its external interface is limited to proposing
  commands, performing linearizable reads, reporting status, and closing.
- Consensus logic must not import gRPC, Prometheus, the filesystem, or the LSM
  implementation.
- Production gRPC and deterministic in-memory networking are adapters at the
  transport seam.
- Disk-backed and in-memory stable stores are adapters at the persistence seam.
- Prefer tests through module interfaces instead of assertions on private fields.

## Repository Layout

- `db/`: public embedded LSM interface and replica apply support.
- `internal/`: storage, consensus, runtime, and transport implementations.
- `cmd/`: runnable node, client/benchmark commands, and existing local benchmark.
- `tests/`: black-box storage and cluster behavior tests.
- `api/`: protobuf sources and checked-in generated Go code.
- `deploy/`: Docker Compose, Prometheus, and Grafana provisioning.
- `dev/active/distributed-kv/`: plan, context, decisions, and status.

## Generated Code

- Edit `.proto` sources, not generated `.pb.go` files.
- Check generated Go protobuf files into the repository.
- Keep the exact generation command documented next to the protobuf source and
  ensure regeneration produces no diff.

## Required Verification

Run the narrowest relevant tests while iterating. Before completing a phase run:

```bash
gofmt -w <changed-go-files>
go test ./...
go test -race ./...
go vet ./...
```

Run Docker integration and benchmark commands when their phases exist. Record the
commands and outcomes in `dev/active/distributed-kv/tasks.md`.

## Working Rules

- Keep each phase buildable and tested before starting the next.
- Preserve unrelated user changes in a dirty worktree.
- Prefer explicit errors over silently weakening durability or consistency.
- Document deviations from the plan in `decisions.md` before implementing them.
- Do not claim performance, failover time, or fault tolerance without a recorded,
  reproducible measurement or test.
