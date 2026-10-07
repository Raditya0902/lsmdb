#!/usr/bin/env python3
"""Check every figure in README.md's "Results" section against the committed results.

What it checks:
  It rebuilds each table row and each figure quoted in the prose of the
  README's "Results" section (up to "Earlier embedded-engine results") from the
  result files below. It reports every one that does not appear in the README.
  Figures follow the rules of internal/benchcompare:
    - valid runs only;
    - the median of the sorted values, or the mean of the middle two for an
      even count;
    - a difference counts only when the two min-max ranges are disjoint.
  Every figure has two decimals. Whitespace and line breaks are ignored when
  matching. It also checks facts the prose states without a figure, such as
  which arms passed the large-value test, and the arm commits the tables name.

Inputs, relative to the repository root:
  README.md
  benchmarks/results/2026-10-06-p13-compare.json
  benchmarks/results/2026-10-05-p12a-dup-ack-compare.json
  benchmarks/results/2026-10-05-p12b-compare.json
  benchmarks/results/2026-10-06-p13-large-{storm,copy,prereq,final}.txt

Run from the repository root. It needs Python 3.6 or later and the standard
library only:
  python3 scripts/verify_readme.py

It prints one line per figure, ok or MISSING, and one line per checked fact. It
exits 1 if a figure is missing or a fact does not hold. It only reads files.
"""
import json
import os
import re
import sys
from typing import Dict, List, Tuple

RESULTS = "benchmarks/results"
P13 = "2026-10-06-p13-compare.json"
P12A = "2026-10-05-p12a-dup-ack-compare.json"
P12B = "2026-10-05-p12b-compare.json"
CLIENTS = (1, 4, 16)

METRIC = {
    "ops": lambda r: r["throughput_ops_per_sec"],
    "p50": lambda r: r["latency_ms"]["p50"],
    "p99": lambda r: r["latency_ms"]["p99"],
    "p999": lambda r: r["latency_ms"]["p99_9"],
    "max": lambda r: r["latency_ms"]["max"],
    "appends": lambda r: r["counters"]["append_messages_per_committed_entry"],
    "per_sync": lambda r: r["counters"]["entries_per_log_sync"],
    "leader_per_sync": lambda r: r["counters"]["leader_entries_per_log_sync"],
    "follower_per_sync": lambda r: r["counters"]["follower_entries_per_log_sync"],
}

failures: List[str] = []
# Valid and invalid runs of one compare file, keyed by (arm, clients), as (repetition, report).
Cells = Dict[Tuple[str, int], List[Tuple[int, dict]]]


def fact(holds: bool, description: str) -> None:
    """Records a stated fact that has no figure of its own to match."""
    print(("fact ok " if holds else "FACT FAILED ") + description)
    if not holds:
        failures.append(description)


def squash(text: str) -> str:
    return " ".join(text.split())


def median(values: List[float]) -> float:
    ordered = sorted(values)
    mid = len(ordered) // 2
    return ordered[mid] if len(ordered) % 2 else (ordered[mid - 1] + ordered[mid]) / 2


def load(name: str) -> Tuple[dict, Dict[str, str], Cells]:
    """Returns the compare file, its arm commits, and its valid runs by (arm, clients)."""
    with open(os.path.join(RESULTS, name)) as f:
        data = json.load(f)
    cells = {}
    for run in data["runs"]:
        report = run["report"]["runs"][0]
        cells.setdefault((run["arm"], run["clients"]), []).append((run["repetition"], report))
    return data, {arm["name"]: arm["rev"] for arm in data["arms"]}, cells


def stat(cells: Cells, arm: str, clients: int, metric: str) -> Tuple[float, float, float]:
    values = [METRIC[metric](r) for _, r in cells[(arm, clients)] if r["valid"]]
    return median(values), min(values), max(values)


def two(x: float) -> str:
    return f"{x:.2f}"


def span(t: Tuple[float, float, float]) -> str:
    return f"{two(t[0])} [{two(t[1])}, {two(t[2])}]"


def step(cells_a: Cells, a: str, cells_b: Cells, b: str, clients: int) -> str:
    """b's throughput median over a's, noting overlapping ranges."""
    sa, sb = stat(cells_a, a, clients, "ops"), stat(cells_b, b, clients, "ops")
    text = f"×{two(sb[0] / sa[0])}"
    return text if (sa[2] < sb[1] or sb[2] < sa[1]) else text + ", ranges overlap"


def fsync_p50s(cells: Cells) -> List[float]:
    return [r[key]["p50_ms"] for runs in cells.values() for _, r in runs for key in ("fsync_before", "fsync_after")]


def tables(c13: Dict[str, str], k13: Cells, k12a: Cells, k12b: Cells) -> List[str]:
    rows = []
    for clients in CLIENTS:
        for arm in ("storm", "copy", "prereq", "final"):
            s = lambda m: stat(k13, arm, clients, m)
            rows.append(f"| {clients} | {arm} | `{c13[arm]}` | {span(s('ops'))} | {two(s('p50')[0])} "
                        f"| {span(s('p99'))} | {two(s('appends')[0])} | {two(s('per_sync')[0])} |")
    for a, b in (("storm", "copy"), ("copy", "prereq"), ("prereq", "final"), ("copy", "final"), ("storm", "final")):
        rows.append(f"| {a} → {b} | " + " | ".join(step(k13, a, k13, b, c) for c in CLIENTS) + " |")
    for clients in CLIENTS:
        ops = lambda cells, arm: two(stat(cells, arm, clients, "ops")[0])
        rows.append(f"| {clients} | {ops(k12a, 'base')} → {ops(k12a, 'fix')} ({step(k12a, 'base', k12a, 'fix', clients)}) "
                    f"| {ops(k12b, 'base')} → {ops(k12b, 'cap')} ({step(k12b, 'base', k12b, 'cap', clients)}) "
                    f"| {ops(k12b, 'base')} → {ops(k12b, 'copy')} ({step(k12b, 'base', k12b, 'copy', clients)}) |")
    for arm13, cells, arm, label in (("storm", k12a, "base", "p12a"), ("copy", k12b, "copy", "p12b")):
        for clients in CLIENTS:
            rows.append(f"| {arm13} `{c13[arm13]}` | {clients} | {span(stat(cells, arm, clients, 'ops'))} ({label}) "
                        f"| {span(stat(k13, arm13, clients, 'ops'))} |")
    return rows


def prose(p13: dict, k13: Cells, k12a: Cells) -> List[str]:
    s = lambda arm, clients, m: two(stat(k13, arm, clients, m)[0])
    runs = [x["report"]["runs"][0] for x in p13["runs"]]
    figures = [
        f"All {len(runs)} runs in the phase-13 file were valid, with {sum(r['elections_in_window'] for r in runs)} "
        f"elections in any measurement window and {sum(r['ops_failed'] for r in runs)} failed operations",
        f"{s('storm', 1, 'appends')}, {s('storm', 4, 'appends')} and {s('storm', 16, 'appends')} AppendEntries",
        f"Only final against copy is disjoint ({step(k13, 'copy', k13, 'final', 4)})",
        f"Prereq → final ({step(k13, 'prereq', k13, 'final', 16)})",
        f"Copy → prereq ({step(k13, 'copy', k13, 'prereq', 16)})",
        f"the leader wrote {s('final', 16, 'leader_per_sync')} entries per log sync and the followers "
        f"{s('final', 16, 'follower_per_sync')} ({s('final', 4, 'leader_per_sync')} and "
        f"{s('final', 4, 'follower_per_sync')} at 4 clients)",
        f"{s('prereq', 4, 'per_sync')} entries per log sync at 4 clients and {s('prereq', 16, 'per_sync')} at 16, "
        f"against copy's {s('copy', 4, 'per_sync')}",
    ]
    base = [two(stat(k12a, "base", c, "appends")[0]) for c in CLIENTS]
    fix = [two(stat(k12a, "fix", c, "appends")[0]) for c in CLIENTS]
    figures.append(f"from {base[0]}, {base[1]} and {base[2]} to {fix[0]}, {fix[1]} and {fix[2]}")
    for arm, label in (("final", "Final"), ("copy", "Copy")):
        maxima = sorted(METRIC["max"](r) for _, r in k13[(arm, 16)] if r["valid"])
        figures.append(f"**{label}:** p99.9 is {span(stat(k13, arm, 16, 'p999'))} ms, and the per-run maximum is "
                       f"{two(maxima[0])}–{two(maxima[-1])} ms")
    figures.append(f"its p99 is {s('final', 16, 'p99')} ms against copy's {s('copy', 16, 'p99')}")
    deadline = [(arm, c, rep, r) for (arm, c), runs in k13.items() if arm != "storm"
                for rep, r in runs if r["counters"]["send_failures_deadline"]]
    fact(len(deadline) == 1, f"exactly one non-storm run has deadline send failures ({len(deadline)} found)")
    if deadline:
        arm, c, rep, r = deadline[0]
        figures.append(f"One {arm}-arm run ({c} clients, repetition {rep}) recorded "
                       f"{r['counters']['send_failures_deadline']:g}")
        figures.append(f"It had {r['elections_in_window']} elections and {r['ops_failed']} failed operations")
    p50s = fsync_p50s(k13)
    block_kib = k13[("final", 16)][0][1]["fsync_before"]["block_bytes"] // 1024
    figures.append(f"In the phase-13 file, the {block_kib} KiB fsync p50 on the instance's network disk was "
                   f"{two(min(p50s))}–{two(max(p50s))} ms (median {two(median(p50s))}), across {len(p50s)} samples")
    return figures


def large_value(figures: List[str]) -> None:
    """Parses the go test -v output of the large-value stage, one file per arm."""
    found = {}
    for arm in ("storm", "copy", "prereq", "final"):
        with open(os.path.join(RESULTS, f"2026-10-06-p13-large-{arm}.txt")) as f:
            text = f.read()
        found[arm] = {
            "pass": len(re.findall(r"^--- PASS", text, re.M)),
            "fail": len(re.findall(r"^--- FAIL", text, re.M)),
            "elections": [int(x) for x in re.findall(r"(\d+) elections among", text)],
            "failed_writes": sum(int(x) for x in re.findall(r"(\d+) of 24 writes failed", text)),
            "unanswered": len(re.findall(r"did not report a status", text)),
        }
        print(f"large-value {arm}: {found[arm]}")
    clean = {"pass": 5, "fail": 0, "elections": [0] * 5, "failed_writes": 0, "unanswered": 0}
    fact(all(found[arm] == clean for arm in ("copy", "prereq", "final")),
         "copy, prereq and final passed 5 of 5 with 0 elections and 0 failed writes")
    storm = found["storm"]
    fact(storm["pass"] == 0 and storm["failed_writes"] == 0, "storm passed no run and failed no write")
    figures.append("**copy, prereq and final:** each passed 5 of 5, with 0 elections and 0 failed writes")
    elections = storm["elections"]
    figures.append(f"**storm:** failed {storm['fail']} of 5, with {', '.join(map(str, elections[:-1]))} and "
                   f"{elections[-1]} elections. No write failed, but in {storm['unanswered']} of those runs")


def main() -> None:
    with open("README.md") as f:
        readme = f.read()
    section = squash(readme[readme.index("## Results\n"):readme.index("## Earlier embedded-engine results")])
    p13, c13, k13 = load(P13)
    _, c12a, k12a = load(P12A)
    _, c12b, k12b = load(P12B)
    fact(c12a == {"base": "bfa2a77", "fix": "56267a1"}, f"p12a arms are base=bfa2a77 fix=56267a1 ({c12a})")
    fact(c12b == {"base": "6d6dda7", "cap": "8c8a317", "copy": "18369e8"},
         f"p12b arms are base=6d6dda7 cap=8c8a317 copy=18369e8 ({c12b})")
    fact(two(stat(k13, "copy", 16, "per_sync")[0]) == "1.00", "copy writes 1.00 entries per log sync at 16 clients too")

    figures = tables(c13, k13, k12a, k12b) + prose(p13, k13, k12a)
    large_value(figures)
    for name in (P12A, P12B):
        p50s = fsync_p50s(load(name)[2])
        print(f"(report only) {name}: fsync p50 {two(min(p50s))}–{two(max(p50s))} ms, "
              f"median {two(median(p50s))}, {len(p50s)} samples")

    missing = 0
    for figure in figures:
        present = squash(figure) in section
        missing += not present
        print(("ok      " if present else "MISSING ") + figure)
    print(f"{len(figures) - missing} of {len(figures)} figures found verbatim in README Results; "
          f"{len(failures)} failed facts")
    sys.exit(1 if missing or failures else 0)


if __name__ == "__main__":
    main()
