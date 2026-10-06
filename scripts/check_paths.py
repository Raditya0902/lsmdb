#!/usr/bin/env python3
"""Check that every repository path, link and anchor README.md cites exists.

What it checks:
  - Relative Markdown links, such as [text](path#anchor). The path must exist.
    An anchor must match a heading slug in the target file, using GitHub's
    rule: lowercase, punctuation dropped, spaces turned into hyphens.
  - Backticked tokens outside code blocks that look like paths: they contain a
    "/" or end in a known extension.
    - A glob, such as db/replica_*, must match at least one file.
    - A bare file name must exist somewhere in the repository.
    - File extensions and HTTP endpoints are listed in NOT_PATHS and skipped.
  - Paths under scripts/, cmd/, benchmarks/, api/ and deploy/ inside code
    blocks.
  - The target of every "go run" command, in prose or in code blocks.

Inputs: README.md and the repository tree, relative to the repository root.

Run from the repository root. It needs Python 3.6 or later and the standard
library only:
  python3 scripts/check_paths.py

It prints one line per path, ok, skip or MISSING. It exits 1 if anything is
missing. It only reads files.
"""
import glob
import os
import re
import sys
from typing import List, Set, Tuple

NOT_PATHS = {
    ".pb.go": "file extension",
    ".proto": "file extension",
    "/healthz": "HTTP endpoint",
    "/metrics": "HTTP endpoint",
}
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


def check_links(prose: str) -> None:
    print("== Markdown links")
    for link in sorted(set(re.findall(r"\]\(([^)\s]+)\)", prose))):
        if link.startswith(("http://", "https://", "mailto:")):
            continue
        path, _, anchor = link.partition("#")
        target = path or "README.md"
        ok = os.path.exists(target)
        if ok and anchor:
            with open(target) as f:
                ok = anchor in slugs(f.read())
        report(ok, link)


def check_backticks(prose: str) -> None:
    print("== Backticked paths outside code blocks")
    for token in sorted(set(re.findall(r"`([^`\s]+)`", prose))):
        looks_like_path = "/" in token or re.search(PATH_EXTENSIONS, token)
        if not looks_like_path or token.startswith(("http", "-", "$")) or "<" in token:
            continue
        if token in NOT_PATHS:
            print(f"skip    {token}  ({NOT_PATHS[token]})")
        elif "*" in token:
            matches = glob.glob(token)
            report(bool(matches), token, f"glob: {len(matches)} files")
        elif "/" not in token:
            matches = find_file(token)
            report(bool(matches), token, "found at " + ", ".join(matches) if matches else "not found")
        else:
            report(os.path.exists(token.rstrip("/")), token)


def check_code_blocks(blocks: List[str]) -> None:
    print(f"== Paths inside code blocks ({len(blocks)} blocks)")
    for block in blocks:
        for token in sorted(set(re.findall(r"(?:\./)?(?:scripts|cmd|benchmarks|api|deploy)/[\w./-]+", block))):
            report(os.path.exists(token[2:] if token.startswith("./") else token.rstrip("/.")), token)


def check_go_run(readme: str) -> None:
    print("== go run targets")
    flat = " ".join(readme.split())  # a command in prose may wrap across lines
    for target in sorted(set(re.findall(r"go run (\S+)", flat))):
        target = target.strip("`")
        path = target[2:] if target.startswith("./") else target
        if path.endswith("/..."):
            path = path[:-4]
        report(os.path.exists(path), f"go run {target}")


def main() -> None:
    with open("README.md") as f:
        readme = f.read()
    blocks, prose = code_blocks(readme)
    check_links(prose)
    check_backticks(prose)
    check_code_blocks(blocks)
    check_go_run(readme)
    print(f"MISSING: {len(missing)}")
    sys.exit(1 if missing else 0)


if __name__ == "__main__":
    main()
