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
```

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
  -- -official -warmup 5s -duration 30s -data-dir "$BENCH"
```

### 3b. Baseline: bd67696 vs the phase-11 tip

```bash
go run scripts/bench_compare.go run \
  -arm base=bd67696+ed0e92e,8eae967 -arm tip=8eae967 \
  -clients 1,2,4,8,16 -repetitions 5 -work "$BENCH/baseline" \
  -out "benchmarks/results/$(date -u +%F)-baseline-compare.json" \
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

## 4. Official embedded run

```bash
go run ./cmd/bench -official -repetitions 5 -data-dir "$BENCH"
```

The embedded workloads are small: each timed section lasts milliseconds. Compare
min–max ranges, not single medians.

## 5. Reading and archiving results

**Validity.** A cluster run is invalid if any node's term changed during the
measurement window, which means an election happened while it was being measured.
Invalid runs are counted but excluded from every statistic. If any cell has more
than one invalid run, find the cause and rerun the whole comparison.

**Meaningful differences.** A difference is meaningful only when the two arms'
min–max ranges over valid runs are disjoint.

At 5 runs per arm, two identical distributions produce disjoint ranges by chance
2 / C(10,5) ≈ 0.8% of the time. At 3 runs per arm the chance is 2 / C(6,3) = 10%,
so do not draw conclusions from 3-run comparisons.

**Counter ratios.** Report the counter ratios together with the window length.
AppendEntries per committed entry grows with how long the cluster has been
running, so ratios from different `-duration` values are not comparable.

**Archiving.** Keep together:
- the compare JSON files;
- the embedded JSON;
- the checklist text;
- the per-run reports, in the `runs/` directory under each `-work` directory.

```bash
tar czf "benchmarks/results/$(date -u +%F)-$(git rev-parse --short=12 HEAD)-archive.tgz" \
  benchmarks/results/*.json benchmarks/results/*-checklist.txt "$BENCH"/*/runs
```

`benchmarks/results/` is not ignored by git. Files there show as untracked until a
decision is made to commit them.
