"""Refuse to seed before anything is written.

The seed writes to three places and takes minutes to finish. Every failure this
module reports would otherwise surface either as a stack trace halfway through a
run or — worse — as a successful run whose data nobody can see. It answers one
question per check and reports every answer at once.

Two of the checks are safety gates rather than correctness ones: the seeder is
demo data, it now ships inside the same image a cluster uses for migrations, and
a CLI can point it at any stand.

* Identity rows are additive, and every one the seeder writes carries a `reason`
  starting with `seed.py ` — so a tenant holding person rows with any other
  reason is a tenant somebody else's data lives in.
* Silver rows are NOT additive: the generators TRUNCATE each table before
  writing, across every tenant, because a partially rewritten silver table
  produces metrics that are wrong rather than absent. Rows there for any other
  tenant would be destroyed, so their presence is a refusal too.

Both are overridable with `SEED_FORCE=1`, which is the only way to say "yes,
clear it" out loud.
"""

from __future__ import annotations

import logging
import re
import uuid as uuid_mod
from collections.abc import Iterable, Sequence
from pathlib import Path

import pymysql

from . import config
from .config import SEED_REASON_PREFIX, ClickHouse, EnvContractError, MariaDb

LOG = logging.getLogger("seed.preflight")

STEPS = ("identity", "silver", "analytics")


class PreflightError(Exception):
    """The stand cannot take this seed. Carries every reason, not the first."""

    def __init__(self, problems: Sequence[str]) -> None:
        self.problems = tuple(problems)
        super().__init__(
            "preflight refused to seed this stand:\n" + "\n".join(f"  - {p}" for p in self.problems)
        )


def table_missing_problem(database: str, table: str, *, needed_for: str) -> str:
    """The message for a database that does not hold a table the seed writes to.

    Names the database it looked in, because the whole class of bug this catches
    is being pointed at the wrong one.
    """
    return (
        f"`{database}` has no `{table}` table, so it is not the database that holds "
        f"{needed_for}. Point the seed at the database whose migrations created it."
    )


def foreign_rows_problem(count: int, tenant: str, database: str) -> str:
    return (
        f"tenant {tenant} already holds {count} `{database}.persons` row(s) this seeder "
        f"did not write (their `reason` does not start with {SEED_REASON_PREFIX!r}), so "
        "this stand carries identity data from somewhere else. Seed a tenant of your own, "
        f"or set {config.FORCE_ENV}=1 to add demo people to this one anyway."
    )


def _table_exists(cur: pymysql.cursors.Cursor, database: str, table: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s LIMIT 1",
        (database, table),
    )
    return cur.fetchone() is not None


def _count_foreign_persons(cur: pymysql.cursors.Cursor, tenant: str) -> int:
    cur.execute(
        "SELECT COUNT(*) FROM persons "
        "WHERE insight_tenant_id = %s AND (reason IS NULL OR reason NOT LIKE %s)",
        (uuid_mod.UUID(tenant).bytes, f"{SEED_REASON_PREFIX}%"),
    )
    row = cur.fetchone()
    return int(row[0]) if row else 0


def _connect(target: MariaDb) -> pymysql.connections.Connection:
    return pymysql.connect(
        host=target.host,
        port=target.port,
        user=target.user,
        password=target.password,
        database=target.database,
        autocommit=True,
        cursorclass=pymysql.cursors.Cursor,
    )


def _check_identity(target: MariaDb, tenant: str, *, force: bool) -> list[str]:
    try:
        conn = _connect(target)
    except pymysql.MySQLError as exc:
        return [f"cannot reach MariaDB `{target.database}` at {target.host}:{target.port}: {exc}"]

    try:
        cur = conn.cursor()
        if not _table_exists(cur, target.database, "persons"):
            return [
                table_missing_problem(
                    target.database, "persons", needed_for="the identity projection"
                )
            ]
        if force:
            LOG.warning("%s=1: skipping the foreign-rows check", config.FORCE_ENV)
            return []
        foreign = _count_foreign_persons(cur, tenant)
        if foreign:
            return [foreign_rows_problem(foreign, tenant, target.database)]
        return []
    finally:
        conn.close()


def _check_analytics(target: MariaDb) -> list[str]:
    try:
        conn = _connect(target)
    except pymysql.MySQLError as exc:
        return [f"cannot reach MariaDB `{target.database}` at {target.host}:{target.port}: {exc}"]

    try:
        if not _table_exists(conn.cursor(), target.database, "metric_definitions"):
            return [
                table_missing_problem(
                    target.database,
                    "metric_definitions",
                    needed_for="the analytics catalogue",
                )
            ]
        return []
    finally:
        conn.close()


#: Column names a reset target may carry its tenant in, most specific first.
TENANT_COLUMNS = ("insight_tenant_id", "tenant_id")

_IDENTIFIER = re.compile(r"^[a-z0-9_]+$")


def foreign_silver_problem(rows: int, tables: Sequence[tuple[str, int]], tenant: str) -> str:
    """The message for reset targets holding another tenant's rows.

    The silver step TRUNCATEs before it inserts — a reset, not a merge, and not
    tenant-scoped, because a partial silver table produces metrics that are
    wrong rather than absent. That makes somebody else's rows in those tables a
    refusal, not a warning.
    """
    worst = ", ".join(f"{name} ({count})" for name, count in tables)
    return (
        f"the silver step clears {rows} row(s) belonging to another tenant than {tenant} "
        f"({worst}). It TRUNCATEs every table it writes, so those rows would be destroyed. "
        f"Seed a stand of your own, or set {config.FORCE_ENV}=1 to clear them deliberately."
    )


def _tenant_columns(client: object) -> dict[tuple[str, str], str]:
    """Which column carries the tenant, per reset target that has one."""
    from .generators.base import RESET_TARGETS

    schemas = sorted({schema for schema, _ in RESET_TARGETS})
    found = client.query(  # type: ignore[attr-defined]
        "SELECT database, table, name FROM system.columns "
        "WHERE database IN {dbs:Array(String)} AND name IN {cols:Array(String)}",
        parameters={"dbs": schemas, "cols": list(TENANT_COLUMNS)},
    )

    by_target: dict[tuple[str, str], str] = {}
    for database, table, column in found.result_rows:
        target = (str(database), str(table))
        if target not in RESET_TARGETS:
            continue
        current = by_target.get(target)
        # Most specific wins, so a table carrying both is counted once.
        if current is None or TENANT_COLUMNS.index(str(column)) < TENANT_COLUMNS.index(current):
            by_target[target] = str(column)
    return by_target


def _foreign_silver_rows(
    client: object, tenant: str, *, limit: int = 4
) -> tuple[int, list[tuple[str, int]], list[tuple[str, str]]]:
    """Foreign rows in the reset surface, plus the targets that cannot be judged.

    Scans exactly what the generators clear — `generators.base.RESET_TARGETS` —
    rather than a name pattern: a pattern both misses targets in other databases
    and refuses stands over tables the step never touches.
    """
    from .generators.base import RESET_TARGETS

    tenant_column = _tenant_columns(client)
    unattributable = [t for t in RESET_TARGETS if t not in tenant_column]

    # One statement rather than one per table: the answer is a single number plus
    # the worst offenders.
    #
    # A NULL tenant is NOT foreign. Several generators carry the tenant in
    # another column (`class_people` uses `workspace_id`) and leave the tenant
    # column unset, so counting NULLs would make this seeder's own rows look like
    # somebody else's and refuse every re-seed of a stand it seeded itself.
    parts = [
        f"SELECT '{schema}.{table}' AS tbl, count() AS n FROM `{schema}`.`{table}` "
        f"WHERE `{column}` IS NOT NULL AND `{column}` != {{tenant:String}}"
        for (schema, table), column in sorted(tenant_column.items())
        if _IDENTIFIER.match(schema) and _IDENTIFIER.match(table) and _IDENTIFIER.match(column)
    ]
    if not parts:
        return 0, [], unattributable

    result = client.query(  # type: ignore[attr-defined]
        f"SELECT tbl, n FROM ({' UNION ALL '.join(parts)}) WHERE n > 0 ORDER BY n DESC",
        parameters={"tenant": tenant},
    )
    rows = [(str(row[0]), int(row[1])) for row in result.result_rows]
    return sum(count for _, count in rows), rows[:limit], unattributable


def _check_clickhouse(target: ClickHouse, scripts: Path, tenant: str, *, force: bool) -> list[str]:
    problems: list[str] = []

    # Imported here, not at module scope: the identity step alone must not need
    # a ClickHouse driver installed to run its own preflight.
    import clickhouse_connect

    client = None
    try:
        client = clickhouse_connect.get_client(
            host=target.host,
            port=target.http_port,
            username=target.user,
            password=target.password,
        )
        client.command("SELECT 1")
    # The driver raises a wide family here (network, auth, protocol), and every
    # one of them means the same thing to the operator reading this.
    except Exception as exc:
        problems.append(f"cannot reach ClickHouse at {target.url} as {target.user!r}: {exc}")

    if client is not None:
        if force:
            LOG.warning("%s=1: skipping the foreign-silver-rows check", config.FORCE_ENV)
        else:
            try:
                rows, worst, unattributable = _foreign_silver_rows(client, tenant)
            except Exception as exc:
                # A stand with no silver database yet is the normal fresh case,
                # and the placeholder script creates it. Anything else here is
                # still worth reporting rather than swallowing.
                LOG.info("foreign-silver-rows check skipped: %s", exc)
                rows, worst, unattributable = 0, [], []
            if rows:
                problems.append(foreign_silver_problem(rows, worst, tenant))
            elif unattributable:
                # Said out loud rather than silently ignored: these targets are
                # cleared too and carry no tenant, so nobody can tell whose rows
                # they hold. In practice a stand holding foreign rows here also
                # holds them in a tenant-bearing sibling (silver is derived from
                # the same bronze), which is what the count above catches.
                LOG.warning(
                    "reset targets with no tenant column, cleared without attribution: %s",
                    ", ".join(f"{schema}.{table}" for schema, table in unattributable),
                )

    missing = [
        name
        for name in ("create-bronze-placeholders.sh", "apply-ch-migrations.sh")
        if not (scripts / name).is_file()
    ]
    if missing:
        problems.append(
            f"{scripts} does not hold {', '.join(missing)} — the silver step runs the "
            "ingestion tree's own DDL and gold-build scripts and cannot substitute for them."
        )
    return problems


def check(env: dict[str, str] | None = None, steps: Iterable[str] = STEPS) -> None:
    """Verify the environment and the stand, or raise with every problem found."""
    import os

    environ = dict(os.environ if env is None else env)
    requested = tuple(steps)
    problems: list[str] = []

    try:
        config.parse_tenant_id(environ)
    except EnvContractError as exc:
        problems.extend(exc.problems)

    analytics_db: str | None = None
    if "analytics" in requested:
        try:
            analytics_db = config.parse_analytics_database(environ)
        except EnvContractError as exc:
            problems.extend(exc.problems)

    # Both steps that build the roster need the persona it is built around, and
    # `profiles.get_dev_user_email` only complains once the step is already
    # running — which for silver is after it has applied DDL.
    if "identity" in requested or "silver" in requested:
        try:
            config.parse_dev_user_email(environ)
        except EnvContractError as exc:
            problems.extend(exc.problems)

    # One try per reader: sharing one would let a malformed first value hide the
    # second, and reporting the whole list in one run is the point of this module.
    for reader in (
        config.cross_tenant_fixture_enabled,
        config.force_enabled,
        config.parse_anchor_date,
        config.parse_seed_days,
    ):
        try:
            reader(environ)
        except EnvContractError as exc:
            problems.extend(exc.problems)

    if problems:
        # Nothing below can run without these, and every one of them is
        # answerable without touching a database.
        raise PreflightError(problems)

    # Re-read rather than carried down from the try above: past this point it is
    # a plain `str`, and nothing has to reason about how it got that way.
    tenant = config.parse_tenant_id(environ)

    if "identity" in requested:
        problems += _check_identity(
            config.parse_mariadb(environ, database=config.parse_identity_database(environ)),
            tenant,
            force=config.force_enabled(environ),
        )

    if analytics_db is not None:
        problems += _check_analytics(config.parse_mariadb(environ, database=analytics_db))

    if "silver" in requested:
        # Imported inside the branch, not at the top: importing `silver` pulls the
        # ClickHouse driver, which the identity step must not need.
        from .silver import _ingestion_scripts_dir

        problems += _check_clickhouse(
            config.parse_clickhouse(environ),
            _ingestion_scripts_dir(),
            tenant,
            force=config.force_enabled(environ),
        )

    if problems:
        raise PreflightError(problems)

    LOG.info(
        "preflight ok: tenant=%s steps=%s",
        tenant,
        ",".join(requested),
    )
