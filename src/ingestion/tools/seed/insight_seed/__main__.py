"""
Insight sample-data seed — orchestrator.

Subcommands:
    identity   Persons, org_chart, account_person_map (MariaDB).
    silver     CREATE silver tables + apply gold view migrations + INSERT
               sample rows (ClickHouse). Phase 2 — placeholder for now.
    analytics  The catalogue rows no endpoint can create — a
               tenant metric-definition override (MariaDB, analytics database).
    all        Run every step.

Run as a module from the tool directory:

    python3 -m insight_seed all

See the README one level up for the flags, the environment contract and the
ruff/mypy/venv setup; the per-domain generators under `generators/` document
the row shape each one emits.
"""

from __future__ import annotations

import argparse
import logging

LOG = logging.getLogger("seed")

STEPS = ("identity", "silver", "analytics")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        # Named for how it is invoked, not for the file argparse found itself in
        # — `__main__.py` in a usage line tells a reader nothing.
        prog="python3 -m insight_seed",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("identity", help="MariaDB identity seed")
    sub.add_parser("silver", help="ClickHouse silver seed")
    sub.add_parser("analytics", help="MariaDB analytics catalogue seed")
    sub.add_parser("all", help="run every step")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    steps = STEPS if args.cmd == "all" else (args.cmd,)

    # Before anything is written: a seed takes minutes and writes to three
    # places, so every answerable question is answered first. Raises with the
    # whole list of problems rather than the first one.
    from .preflight import check as preflight_check

    preflight_check(steps=steps)

    seeded: list[str] = []
    catalogue: dict[str, object] | None = None

    if args.cmd in ("identity", "all"):
        from .identity import run as run_identity

        run_identity()
        seeded.append("identity")

    if args.cmd in ("silver", "all"):
        from .silver import run as run_silver

        run_silver()
        seeded.append("silver")

    # After the others: these tables are created by ANALYTICS' own migrations at
    # its startup, so the service has to have booted. On `all` that is already
    # true — the stand starts every service before seeding.
    if args.cmd in ("analytics", "all"):
        from .analytics import run as run_analytics

        catalogue = run_analytics()
        seeded.append("analytics")

    # Emit the manifest only after every requested step returned without
    # raising, so its presence means "this stand is seeded" rather than
    # "seeding was attempted". Built from the real environment, unlike the
    # committed PROFILE.md.
    import os

    from .manifest import (
        assert_no_credentials,
        build_manifest,
        manifest_path,
        render_manifest,
        write_manifest,
    )

    doc = build_manifest(os.environ, seeded=seeded, catalogue=catalogue)

    # A cluster Job's filesystem dies with the pod, so the log is the only
    # record of what a run seeded. Printed before the write, so it survives even
    # a run that cannot persist the file — but never before the credential
    # guard: a log line is as public as the file it stands in for.
    assert_no_credentials(doc)
    print(render_manifest(doc))

    try:
        path = write_manifest(doc)
        LOG.info("manifest written: %s", path)
    except OSError as exc:
        # The seed container has historically mounted /app read-only. Fail
        # loudly rather than leaving downstream consumers reading a stale
        # manifest from a previous run.
        raise RuntimeError(
            f"could not write {manifest_path()}: {exc}. The seed source mount "
            "must be writable — see docker-compose.yml seed-sample.volumes."
        ) from exc

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
