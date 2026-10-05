# Benchmark Runbook

How to produce benchmark numbers that can support a before/after claim. Only runs
made on a dedicated Linux machine with `-official` count as results. Runs on any
other machine, or without `-official`, are labeled `secondary` in their JSON. They
are smoke tests and must not be quoted as results.

## Tools

| Tool | Measures | Output |
|---|---|---|
| `cmd/clusterbench` | Replicated writes on fresh in-process 3-node clusters: throughput, latency, Raft counters | `<date>-<sha12>-cluster.json` |
| `scripts/bench_compare.go` | Runs `clusterbench` on several git revisions, interleaved, and merges the results | `<date>-<sha12>-compare.json` |
| `cmd/bench` | Embedded LSM vs SQLite workloads | `<date>-<sha12>-embedded.json` |

The tools have these properties:
- They never overwrite a file. A name collision gets `-2`, `-3`, and so on.
- Secondary runs write to the OS temp dir. Official runs write to `benchmarks/results/`.
- Neither tool writes `results.json` unless that path is passed with `-out` and does
  not exist yet.

## 1. Provision the VM

Use a dedicated Linux VM or host with:
- local SSD or NVMe;
- at least 4 vCPUs and 8 GiB RAM;
- no other tenants' work scheduled during the run.

Network disks such as EBS gp3 or a GCE persistent disk are acceptable, but record
them. Their fsync latency dominates every cluster number.

```bash
sudo apt-get update && sudo apt-get install -y git build-essential   # cgo for go-sqlite3
# Install a Go release at or above the go line in go.mod, from https://go.dev/dl/
go version

git clone https://github.com/Raditya0902/lsmdb.git && cd lsmdb
git config user.name "bench" && git config user.email "bench@localhost"  # cherry-pick needs an identity
git fetch origin phase-10-benchmark-harness-v2 && git checkout phase-10-benchmark-harness-v2
go test ./...
```

The comparison commits must be reachable from the VM's clone. If the branch is not
on the remote yet, carry it over with a bundle from the development machine:

```bash
git bundle create phase10.bundle origin/main..phase-10-benchmark-harness-v2  # dev machine
git fetch /path/to/phase10.bundle phase-10-benchmark-harness-v2:phase-10-benchmark-harness-v2  # VM
```

Pick one directory on the target disk for all benchmark data. The commands below
call it `$BENCH`.

```bash
export BENCH=/mnt/bench && mkdir -p "$BENCH"
export MACHINE="n2-standard-8"   # the provider's machine type, or the host's model
export DISK="local NVMe SSD"     # the disk under $BENCH, e.g. local NVMe or pd-ssd
```

`$BENCH` must be outside `/home`. `bench_compare` records its pass-through
arguments, including the `-data-dir` path, in `clusterbench_args`, and the checker
fails any file that contains a home path.

Official cluster runs refuse to start without `-machine-type` and `-disk-type`.
`bench_compare` takes these two flags itself, before `--`. It records them in the
combined file instead of passing them to the arms, because older arms'
`clusterbench` binaries do not accept them.

## 2. Pre-run checklist

Run these checks and save their output next to the results (see section 5).

```bash
mkdir -p benchmarks/results
{
  date -u; uname -a; go version; git rev-parse HEAD; git status --short
  lscpu
  free -g
  lsblk -d -o NAME,ROTA,MODEL,SIZE
  findmnt -T "$BENCH" -o SOURCE,FSTYPE,OPTIONS
  cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo "no cpufreq"
  uptime
  top -bn1 | head -15
} > "benchmarks/results/$(date -u +%F)-checklist.txt" 2>&1
```

Check before starting:
- **CPU governor:** set to `performance` if cpufreq exists:
  `sudo cpupower frequency-set -g performance`.
- **Load:** `uptime` load average is near 0 and `top` shows no other busy process.
- **Disk:** `findmnt` shows `$BENCH` on the intended local disk, not `tmpfs`.
- **Working tree:** `git status --short` is empty, so the recorded `git_dirty` is
  false.
- **Long runs:** use `tmux` or `nohup` so the run survives an SSH disconnect.

## 3. Official cluster comparison

### Official protocol

Every official cluster number uses exactly this protocol. A run with any other
setting is not official, even on Linux with `-official`.

| Setting | Value |
|---|---|
| Warmup | `-warmup 5s` |
| Measurement window | `-duration 30s` |
| Repetitions | `-repetitions 5`, each on a fresh cluster |
| Arm order | interleaved through `scripts/bench_compare.go`; the first arm rotates every repetition |

Never compare runs that used different `-duration` values. Counter ratios and
throughput both drift with how long the cluster has been running (section 5), so a
change in window length alone changes the numbers.

The failover run (3c) is a single-arm run, so it is not interleaved. It uses the same
warmup, window and repetition count.

### Arms

The arms are defined by role. The SHAs are correct for the branch as committed; if
the branch is rebased, substitute the new SHAs for the same roles.

| Role | Commit |
|---|---|
| Pre-phase-11 engine (`main` at v1.0) | `bd67696` |
| Item 1: clusterbench rewrite | `ed0e92e` |
| Item 2: instrumentation counters | `8eae967` |

Item 2 sits directly on the phase-11 tip `5d5952f` plus item 1. So `tip=8eae967` is
the phase-11 engine with the same two harness commits that `base` gets by
cherry-pick.

### 3a. Instrumentation overhead

Item 1 vs item 2 at 1 client. The medians must agree within 5%.

```bash
go run scripts/bench_compare.go run \
  -arm nocounters=ed0e92e -arm counters=8eae967 \
  -clients 1 -repetitions 5 -work "$BENCH/overhead" \
  -out "benchmarks/results/$(date -u +%F)-overhead-compare.json" \
  -machine-type "$MACHINE" -disk-type "$DISK" \
  -- -official -warmup 5s -duration 30s -data-dir "$BENCH"
```

### 3b. Baseline: bd67696 vs the phase-11 tip

```bash
go run scripts/bench_compare.go run \
  -arm base=bd67696+ed0e92e,8eae967 -arm tip=8eae967 \
  -clients 1,2,4,8,16 -repetitions 5 -work "$BENCH/baseline" \
  -out "benchmarks/results/$(date -u +%F)-baseline-compare.json" \
  -machine-type "$MACHINE" -disk-type "$DISK" \
  -- -official -warmup 5s -duration 30s -data-dir "$BENCH"
```

That is 50 runs of about 40 s each, roughly 35 minutes. While a comparison runs:
- each arm is built in its own detached worktree under `-work`, from committed
  revisions only;
- arms alternate, and the arm that goes first rotates every repetition;
- the combined file is rewritten after every run and records `"complete": false`
  until the last run finishes;
- a cherry-pick conflict stops the run, lists the conflicting paths and resolves
  nothing.

To print the summary again later:

```bash
go run scripts/bench_compare.go summarize benchmarks/results/<file>-compare.json
```

### 3c. Failover

After each run's throughput window, the tool:
1. stops all clients;
2. stops the node that reports Leader at the highest term;
3. times one write through a client that was using that node.

```bash
go run ./cmd/clusterbench -official -mode failover -clients 1,4 -repetitions 5 \
  -warmup 5s -duration 30s -data-dir "$BENCH" -machine-type "$MACHINE" -disk-type "$DISK"
```

The output has three timing fields:
- `failover_ms` starts when the leader's shutdown begins.
- `failover_after_close_ms` starts once `Close` returns. It matches the pre-rewrite
  tool (`bd67696`) that produced the README failover column.
- `leader_close_ms` is the gap between the two.

The probe client retries every 25 ms for at most 60 attempts, so failover is
resolved to about 25 ms.

### 3d. Duration sweep

This sweep checks whether the duplicate-ack growth seen on macOS (section 5) also
happens on Linux. It is not official: windows other than 30 s break the protocol,
so it never passes `-official` and its files are labeled `secondary`.

It measures only the tip arm, at 1 client, with durations of 10, 20, 30 and 60 s
and 5 repetitions each. `bench_compare` needs at least two arms, so the sweep runs
`clusterbench` directly from this checkout. That binary runs the tip arm's engine:
no engine file changed between `8eae967` and the branch head, which the first
command confirms. `clusterbench` takes `-machine-type` and `-disk-type` itself, with
the same `$MACHINE` and `$DISK` as the baseline.

```bash
git diff --quiet 8eae967 HEAD -- . ':!cmd' ':!scripts' ':!benchmarks' ':!internal/bench*' \
  && echo "engine matches the tip arm"
mkdir -p "$BENCH/sweep"
for d in 10 20 30 60; do
  go run ./cmd/clusterbench -clients 1 -repetitions 5 -warmup 5s -duration "${d}s" \
    -data-dir "$BENCH" -machine-type "$MACHINE" -disk-type "$DISK" \
    -out "$BENCH/sweep/tip-${d}s.json" || break
done
for f in "$BENCH"/sweep/tip-*s.json; do
  go run scripts/vm_smoke_check.go -expect head="$(git rev-parse HEAD)" "$f"
done
go run scripts/sweep_table.go "$BENCH"/sweep/tip-*s.json
```

The sweep takes about 13 minutes. Each file's check should pass everything except
`protocol`, which warns. `sweep_table` prints one row per run, ordered by duration.

Two columns are the evidence for the storm. Read them against the duration:
- **`appends/entry`:** AppendEntries messages per committed entry;
- **`ack_resend share`:** the fraction of those messages that were duplicate-ack
  resends.

On Linux, a ratio that keeps growing with duration confirms the duplicate-ack
storm. A flat ratio would mean the macOS behavior was platform-specific. Count it
as growth only if the median rises at every step and the 10 s and 60 s min–max
ranges are disjoint. With 5 runs each, disjoint ranges happen by chance about 0.8%
of the time. The sweep is still a diagnosis, not a result. Ignore rows whose
`valid` column is not `yes`.

Do not cite throughput (`ops/s`) against duration as a storm effect. The retained
Raft log grows for the whole run, because the snapshot threshold of 1,000,000
entries is never reached. On every accepted append, `handleAppend` copies the
whole retained log and `rebuildMembership` decodes all of it. So throughput can fall
with duration whether or not the storm exists.

## 4. Official embedded run

```bash
go run ./cmd/bench -official -repetitions 5 -data-dir "$BENCH"
```

The embedded workloads are small: each timed section lasts milliseconds. Compare
min–max ranges, not single medians.

`cmd/bench` records no machine or disk type, even with `-official`. Its result
files must not be committed until phase 14.

## 5. Reading and archiving results

**Validity.** A cluster run is invalid if any node's term changed during the
measurement window, which means an election happened while it was being measured.
Invalid runs are counted but excluded from every statistic. An invalid run leaves
its cell with fewer than 5 valid runs, so the summary prints `insufficient runs` for
it. Find the cause and rerun the whole comparison.

**Meaningful differences.** A difference is meaningful only when both arms have at
least 5 valid runs and their min–max ranges over valid runs are disjoint. With
fewer than 5 valid runs on either arm, the summary prints `insufficient runs`
instead of a verdict.

At 5 runs per arm, two identical distributions produce disjoint ranges by chance
2 / C(10,5) ≈ 0.8% of the time. At 3 runs per arm the chance is 2 / C(6,3) = 10%.

**Counter ratios and throughput depend on window length.** Both are comparable only
between runs with the same `-duration`, and every report must state its window
length. Duplicate-ack AppendEntries traffic keeps growing while the cluster runs,
even at 1 client. The table below is an observation from secondary smoke runs on
an Apple M4 (macOS) at 1 client. Their raw result files were not retained, so it
cannot be re-verified. It is why the protocol fixes the window length; it is not a
result.

| Window | AppendEntries per committed entry | Throughput |
|---|---:|---:|
| 3 s (1 run) | 12.6 | 89.9 ops/s |
| 10 s (11 runs, min–max) | 33.0–70.4 | 89.6–92.6 ops/s |
| 20 s (1 run) | 112 | 78.9 ops/s |

The 3 s and 20 s rows are single runs, so the throughput decay between them is an
indication, not a measured effect. If the growth is real, duplicate-ack chains do
not die out between writes at 1 client, as was previously assumed. The duration
sweep (3d) tests this on Linux.

**Checking a result file.** Before quoting any number, run the checker on the
compare file (or on a single clusterbench report, with at most one `-expect`):

```bash
go run scripts/vm_smoke_check.go -expect base=bd67696+ed0e92e,8eae967 -expect tip=8eae967 \
  benchmarks/results/<file>-compare.json
```

The checker prints PASS, FAIL, WARN or SKIP for each of these:
- Linux, the CPU count, and the data dir's filesystem and mount options;
- no hostname, username, home path or IP address anywhere in the file;
- `fsync(2)`, with fsync p50 and p99 before and after every run;
- the machine and disk labels;
- the expected arms and SHAs;
- no run invalidated by an election;
- the protocol, which warns when it is not 5 s / 30 s / 5.

It then prints a table of every run, and exits 1 if any check failed. It searches
for the hostname and username of the machine it runs on. To check a VM's file
elsewhere, add `-forbid <vm-hostname> -forbid <vm-user>`.

The IP check matches any four dot-separated numbers, so a version string such as
the kernel release `5.15.153.1` can trip it. If it flags one:
1. Inspect the file by hand to confirm the match is not an address.
2. Fix the pattern in `internal/benchcheck/privacy.go`, with a test case for that
   string.

Never loosen the pattern without a test.

**CPU count.** `bench_compare` records its own `num_cpu` (`runtime.NumCPU()`) and
`gomaxprocs` in the combined file. It runs on the same machine as the arms, whose
pinned `clusterbench` binaries predate `num_cpu`. A `clusterbench` report built
from this checkout records `num_cpu` in its environment block. The checker fails a
file in two cases:
- `num_cpu` is missing;
- `num_cpu` differs from `gomaxprocs`, in the driver or in any report.

This catches an explicit `GOMAXPROCS` setting, but not every container limit:
- The `go 1.22.0` line in `go.mod` turns off Go's container-aware GOMAXPROCS
  (`containermaxprocs=0`), so a CPU quota changes neither value.
- `runtime.NumCPU()` counts only the CPUs the process may use, so a cpuset limit
  lowers both values together.

So keep a notes file next to each official result:

```bash
{
  echo "nproc: $(nproc)  nproc --all: $(nproc --all)"
  echo "cpu.max: $(cat "/sys/fs/cgroup$(cut -d: -f3- /proc/self/cgroup)/cpu.max" 2>/dev/null || echo none)"
  lscpu
} > "benchmarks/results/$(date -u +%F)-notes.txt"
```

The notes must show three things:
- `nproc` equals `nproc --all`;
- `nproc` equals the machine type's vCPU count;
- `cpu.max` starts with `max` (no quota) or is `none`.

**Archiving.** Keep together:
- the compare JSON files;
- the embedded JSON;
- the checklist text and the notes file;
- the per-run reports, in the `runs/` directory under each `-work` directory.

```bash
tar czf "benchmarks/results/$(date -u +%F)-$(git rev-parse --short=12 HEAD)-archive.tgz" \
  benchmarks/results/*.json benchmarks/results/*-checklist.txt \
  benchmarks/results/*-notes.txt "$BENCH"/*/runs
```

`benchmarks/results/` is not ignored by git. Files there show as untracked until a
decision is made to commit them.
