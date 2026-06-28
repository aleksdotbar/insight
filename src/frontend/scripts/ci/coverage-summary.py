#!/usr/bin/env python3
"""Render a global coverage summary as GitHub-flavored markdown from the
Cobertura report `vitest run --coverage` emits. Mirrors the approach in
constructorfabric/insight.

CI-ONLY: this script is invoked exclusively by .github/workflows/ci.yml to
process the coverage artifact produced by `pnpm test:coverage`. pnpm itself is
Python-free — local devs use `pnpm test:coverage` and open coverage/index.html.

This is the GLOBAL coverage view and is a non-failing WARNING — it never exits
non-zero. The enforcing gate is the new-code diff gate (scripts/ci/diff-coverage.py).

Prints markdown to stdout; redirect into $GITHUB_STEP_SUMMARY in CI, e.g.
    python3 scripts/ci/coverage-summary.py >> "$GITHUB_STEP_SUMMARY"
"""
from __future__ import annotations

import os
import sys

# Reports may be PR-derived in CI, so parse defensively when defusedxml is
# present; fall back to stdlib otherwise (e.g. local runs).
try:
    from defusedxml.ElementTree import parse as _xml_parse
except ImportError:
    from xml.etree.ElementTree import parse as _xml_parse

REPORT = os.environ.get("COBERTURA", "coverage/cobertura-coverage.xml")

# Colour thresholds (percent), matching the previous irongut "60 80" config.
GOOD = 80   # >= GOOD  -> green
WARN = 60   # >= WARN  -> yellow, else red


def icon(pct: float) -> str:
    return "🟢" if pct >= GOOD else "🟡" if pct >= WARN else "🔴"


def pct(cov: int, tot: int) -> float:
    return (100.0 * cov / tot) if tot else 100.0


def top_area(filename: str) -> str:
    """Group key: the top-level area under src (e.g. src/components), or 'src'."""
    parts = filename.replace("\\", "/").split("/")
    dirs = parts[:-1]  # drop the file name
    if len(dirs) >= 2:
        return "/".join(dirs[:2])
    return dirs[0] if dirs else "(root)"


def main() -> int:
    if not os.path.isfile(REPORT):
        print(f"_No coverage summary produced — `{REPORT}` not found "
              "(tests likely failed before coverage ran)._")
        return 0

    root = _xml_parse(REPORT).getroot()
    a = root.attrib
    lc, lv = int(a.get("lines-covered", 0)), int(a.get("lines-valid", 0))
    bc, bv = int(a.get("branches-covered", 0)), int(a.get("branches-valid", 0))
    line_pct, branch_pct = pct(lc, lv), pct(bc, bv)

    # Per-area line aggregation from <line> elements.
    areas: dict[str, list[int]] = {}  # area -> [covered, total]
    for cls in root.iter("class"):
        area = top_area(cls.get("filename", ""))
        agg = areas.setdefault(area, [0, 0])
        for ln in cls.iter("line"):
            try:
                hits = int(ln.get("hits", 0))
            except ValueError:
                continue
            agg[1] += 1
            if hits > 0:
                agg[0] += 1

    out = [
        "## Coverage summary",
        "",
        "_Global coverage — informational only (warning, not a gate). The "
        "new-code diff gate is what blocks PRs._",
        "",
        "| Metric | Covered | Total | Coverage |",
        "| --- | ---: | ---: | ---: |",
        f"| Lines | {lc} | {lv} | {icon(line_pct)} {line_pct:.1f}% |",
        f"| Branches | {bc} | {bv} | {icon(branch_pct)} {branch_pct:.1f}% |",
        "",
        "<details><summary>By area</summary>",
        "",
        "| Area | Lines | Coverage |",
        "| --- | ---: | ---: |",
    ]
    for area in sorted(areas):
        cov, tot = areas[area]
        cell = "—" if tot == 0 else f"{icon(pct(cov, tot))} {pct(cov, tot):.1f}%"
        out.append(f"| {area} | {cov}/{tot} | {cell} |")
    out += ["", "</details>", ""]

    print("\n".join(out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
