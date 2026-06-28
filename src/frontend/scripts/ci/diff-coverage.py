#!/usr/bin/env python3
"""New-code (diff) coverage gate, rendered like constructorfabric/insight's
coverage.py: a per-file table (New lines / Coverage / Min / Result) plus the
raw diff-cover output, written to the GitHub job summary. Single source of
truth for local + CI.

It runs `diff-cover` with `--fail-under 0` (so diff-cover never fails) against
the Cobertura report, then decides PASS/FAIL itself: the diff's overall new-line
coverage must be >= DIFF_MIN. Exit non-zero on FAIL so the PR is blocked.

Scope note: coverage is logic-line based (statements/branches/functions). Bare
JSX markup lines are NOT counted by the v8 provider, so a purely-markup change
has no coverable lines and passes. The gate enforces coverage of new LOGIC
(handlers, hooks, conditionals, data transforms).

Env: COBERTURA (report path), BASE_REF (base branch), DIFF_MIN (threshold %),
GITHUB_STEP_SUMMARY (if set, the markdown is appended there too).
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from shutil import which

REPORT = os.environ.get("COBERTURA", "coverage/cobertura-coverage.xml")
BASE_REF = os.environ.get("BASE_REF", "main")
DIFF_MIN = float(os.environ.get("DIFF_MIN", "80"))
SUMMARY = os.environ.get("GITHUB_STEP_SUMMARY")


def icon(ok: bool) -> str:
    return "✅" if ok else "🔴"


def emit(markdown: str) -> None:
    """Print to stdout (CI log + local) and append to the job summary if set."""
    print(markdown)
    if SUMMARY:
        with open(SUMMARY, "a", encoding="utf-8") as fh:
            fh.write(markdown + "\n")


def main() -> int:
    if which("diff-cover") is None:
        print("error: diff-cover not found — install with 'pipx install diff-cover' "
              "(or 'pip install diff-cover').", file=sys.stderr)
        return 1
    if not os.path.isfile(REPORT):
        print(f"error: coverage report '{REPORT}' not found — run 'pnpm test:coverage' first.",
              file=sys.stderr)
        return 1

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tf:
        json_path = tf.name
    # --fail-under 0: diff-cover never fails here; we decide below.
    cmd = ["diff-cover", REPORT, "--compare-branch", f"origin/{BASE_REF}",
           "--json-report", json_path, "--fail-under", "0"]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        Path(json_path).unlink(missing_ok=True)
        sys.stderr.write(proc.stdout + proc.stderr)
        print(f"error: diff-cover failed (exit {proc.returncode}) — see output above.",
              file=sys.stderr)
        return 1
    try:
        data = json.loads(Path(json_path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        print("error: diff-cover produced no JSON report.", file=sys.stderr)
        return 1
    finally:
        Path(json_path).unlink(missing_ok=True)

    total = int(data.get("total_num_lines", 0))
    violations = int(data.get("total_num_violations", 0))
    covered = total - violations
    pct = float(data.get("total_percent_covered", 100.0))
    passed = total == 0 or pct >= DIFF_MIN
    min_disp = f"{DIFF_MIN:.0f}"

    out = ["## New-code coverage", ""]
    if total == 0:
        out.append(f"**{icon(True)} PASS** — no changed coverable lines in this diff "
                   f"(≥ {min_disp}% required).")
    else:
        out += [
            f"**{icon(passed)} {'PASS' if passed else 'FAIL'}** — new code "
            f"(≥ {min_disp}%): {covered}/{total} lines covered ({pct:.1f}%).",
            "",
            "| File | New lines | Coverage | Min | Result |",
            "| --- | ---: | ---: | ---: | :---: |",
        ]
        for path in sorted(data.get("src_stats", {})):
            st = data["src_stats"][path]
            fcov = len(st.get("covered_lines", []))
            ftot = fcov + len(st.get("violation_lines", []))
            fpct = float(st.get("percent_covered", 100.0))
            out.append(f"| {path} | {fcov}/{ftot} | {fpct:.1f}% | {min_disp}% | "
                       f"{icon(fpct >= DIFF_MIN)} |")

    raw = (proc.stdout or "").strip()
    out += [
        "",
        "<details><summary>diff-cover output</summary>",
        "",
        "```",
        raw or "(no output)",
        "```",
        "",
        "</details>",
    ]
    emit("\n".join(out))
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
