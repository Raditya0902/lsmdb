#!/usr/bin/env python3
"""Check that every repository path, link and anchor a Markdown file cites exists.

What it checks, for each file given with --file (README.md by default):
  - Relative Markdown links, such as [text](path#anchor). The path is resolved
    from the directory of the file being checked and must exist. An anchor
    must match a heading slug in the target file, using GitHub's rule:
    lowercase, punctuation dropped, spaces turned into hyphens. A bare
    [text](#anchor) refers to the file being checked.
  - Backticked tokens outside code blocks that look like paths: they contain a
    "/" or end in a known extension. These name repository paths, so they are
    resolved from the repository root.
    - A glob, such as db/replica_*, must match at least one file.
    - A bare file name must exist somewhere in the repository.
    - A path with a line reference, such as internal/raft/node.go:155-163, must
      exist and have at least that many lines.
    - Absolute paths and HTTP endpoints, such as /mnt/bench or /metrics, are not
      repository paths and are skipped, as are the file extensions, units and
      names listed in NOT_PATHS.
  - Paths under scripts/, cmd/, benchmarks/, api/ and deploy/ inside code
    blocks, from the repository root.
  - The target of every "go run" command, in prose or in code blocks, from the
    repository root.

Inputs: the Markdown files and the repository tree.

Run from the repository root. It needs Python 3.6 or later and the standard
library only:
  python3 scripts/check_paths.py
  python3 scripts/check_paths.py --file DESIGN.md --file benchmarks/RUNBOOK.md

It prints one line per path, ok, skip or MISSING. It exits 1 if anything is
missing in any file. It only reads files.
"""
import argparse
import glob
import os
import re
import sys
from typing import List, Set, Tuple

NOT_PATHS = {
    ".pb.go": "file extension",
    ".proto": "file extension",
    "appends/entry": "metric name",
    "ops/s": "unit",
    "runs/": "directory under each bench_compare -work directory, outside the repository",
}
# A path followed by a line reference, such as internal/raft/node.go:155-163.
LINE_REFERENCE = re.compile(r"^(?P<path>.+\.\w+):(?P<first>\d+)(?:-(?P<last>\d+))?$")
PATH_EXTENSIONS = r"\.(go|md|json|sh|proto|yml|yaml|txt)$"

missing: List[str] = []


def report(ok: bool, item: str, note: str = "") -> None:
    print(f"{'ok     ' if ok else 'MISSING'} {item}{'  (' + note + ')' if note else ''}")
    if not ok:
        missing.append(item)


def code_blocks(markdown: str) -> Tuple[List[str], str]:
    """Splits Markdown into its fenced code blocks and the prose around them."""
    blocks, prose, current = [], [], None
    for line in markdown.splitlines():
        if line.strip().startswith("```"):
            if current is None:
                current = []
            else:
                blocks.append("\n".join(current))
                current = None
        elif current is not None:
            current.append(line)
        else:
            prose.append(line)
    return blocks, "\n".join(prose)


def slugs(markdown: str) -> Set[str]:
    _, prose = code_blocks(markdown)
    headings = re.findall(r"^#{1,6} (.+)$", prose, flags=re.M)
    return {re.sub(r"[^\w\- ]", "", h.strip().lower()).replace(" ", "-") for h in headings}


def find_file(name: str) -> List[str]:
    """Returns repository-relative paths of files called name, skipping .git."""
    matches = []
    for directory, subdirs, files in os.walk("."):
        subdirs[:] = [d for d in subdirs if d != ".git"]
        if name in files:
            matches.append(os.path.relpath(os.path.join(directory, name)))
    return matches


def check_links(prose: str, source: str) -> None:
    print("== Markdown links")
    for link in sorted(set(re.findall(r"\]\(([^)\s]+)\)", prose))):
        if link.startswith(("http://", "https://", "mailto:")):
            continue
        path, _, anchor = link.partition("#")
        target = os.path.normpath(os.path.join(os.path.dirname(source), path)) if path else source
        ok = os.path.exists(target)
        if ok and anchor:
            with open(target) as f:
                ok = anchor in slugs(f.read())
        report(ok, link, "" if target == link else f"resolves to {target}")


def check_backticks(prose: str) -> None:
    print("== Backticked paths outside code blocks")
    for token in sorted(set(re.findall(r"`([^`\s]+)`", prose))):
        looks_like_path = "/" in token or re.search(PATH_EXTENSIONS, token)
        if not looks_like_path or token.startswith(("http", "-", "$")) or "<" in token:
            continue
        if token.startswith("/"):
            print(f"skip    {token}  (absolute path or endpoint, not a repository path)")
        elif token in NOT_PATHS:
            print(f"skip    {token}  ({NOT_PATHS[token]})")
        elif LINE_REFERENCE.match(token):
            check_line_reference(token)
        elif "*" in token:
            matches = glob.glob(token)
            report(bool(matches), token, f"glob: {len(matches)} files")
        elif "/" not in token:
            matches = find_file(token)
            report(bool(matches), token, "found at " + ", ".join(matches) if matches else "not found")
        else:
            report(os.path.exists(token.rstrip("/")), token)


def check_line_reference(token: str) -> None:
    match = LINE_REFERENCE.match(token)
    path, last = match.group("path"), int(match.group("last") or match.group("first"))
    if not os.path.isfile(path):
        report(False, token, "file not found")
        return
    with open(path) as f:
        lines = sum(1 for _ in f)
    report(last <= lines, token, f"{path} has {lines} lines")


def check_code_blocks(blocks: List[str]) -> None:
    print(f"== Paths inside code blocks ({len(blocks)} blocks)")
    for block in blocks:
        for token in sorted(set(re.findall(r"(?:\./)?(?:scripts|cmd|benchmarks|api|deploy)/[\w./-]+", block))):
            report(os.path.exists(token[2:] if token.startswith("./") else token.rstrip("/.")), token)


def check_go_run(markdown: str) -> None:
    print("== go run targets")
    flat = " ".join(markdown.split())  # a command in prose may wrap across lines
    for target in sorted(set(re.findall(r"go run (\S+)", flat))):
        target = target.split("`")[0].rstrip(".,;:)")  # end at the code span, drop sentence punctuation
        path = target[2:] if target.startswith("./") else target
        if path.endswith("/..."):
            path = path[:-4]
        report(os.path.exists(path), f"go run {target}")


def check_file(source: str) -> None:
    print(f"#### {source}")
    with open(source) as f:
        markdown = f.read()
    blocks, prose = code_blocks(markdown)
    check_links(prose, source)
    check_backticks(prose)
    check_code_blocks(blocks)
    check_go_run(markdown)


def main() -> None:
    parser = argparse.ArgumentParser(description="Check the paths, links and anchors a Markdown file cites.")
    parser.add_argument("--file", action="append", dest="files", metavar="PATH",
                        help="Markdown file to check, relative to the repository root; repeatable "
                             "(default: README.md)")
    files = parser.parse_args().files or ["README.md"]
    for source in files:
        if not os.path.isfile(source):
            report(False, source, "file to check not found")
            continue
        check_file(source)
    print(f"MISSING: {len(missing)}")
    sys.exit(1 if missing else 0)


if __name__ == "__main__":
    main()
