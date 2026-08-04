"""
MariaDB analytics seed: the catalogue rows no endpoint can create.

Two tables the product provisions by operator or migration rather than through
its API, so a suite has nothing to create them with:

  table_columns       the drilldown column catalogue `/v1/columns/{table}`
                      serves. Empty on a fresh stand, which makes the per-table
                      filter untestable — every table answers `{"items": []}`
                      and a broken filter looks identical to a correct one.
  metric_definitions  a TENANT-scoped row overriding a product default. The
                      listing is supposed to resolve the tenant's label over the
                      product's; with no tenant row anywhere, nothing proves it.

Seeded here rather than inserted by a test fixture on purpose. The compose-stand
suite holds no database connection — that would hand every test a back door
around the deployed path it exists to exercise (see
tests/stand/api/analytics/test_columns.py) — so anything a test needs and no
endpoint creates has to be seeded, and then NAMED IN THE MANIFEST. A test reads
the name from there; it never hardcodes one.

Runs after analytics has migrated: these tables are created by its SeaORM
migrations at startup, not by this seed.
"""

from __future__ import annotations

import logging
import os
import re
import uuid as uuid_mod
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any

import pymysql

LOG = logging.getLogger("seed.analytics")

#: Deterministic ids, so re-seeding an un-torn-down stand replaces its own rows
#: instead of accumulating a new pair every run.
COLUMN_ROW_IDS = (
    "e1e1e1e1-0000-4000-8000-000000000001",
    "e1e1e1e1-0000-4000-8000-000000000002",
)
DEFINITION_ROW_ID = "e1e1e1e1-0000-4000-8000-000000000010"

#: The table/label constants live in `manifest` rather than here: `PROFILE.md`
#: is rendered by a tool that must import no third-party package, and this
#: module needs pymysql. The manifest owns the NAMES; this module owns writing
#: the rows.
from manifest import CATALOGUED_TABLES, OVERRIDE_LABEL  # noqa: E402


def _bin(u: str) -> bytes:
    """UUID string → 16 raw bytes, matching the identity seed's convention."""
    return uuid_mod.UUID(u).bytes


@contextmanager
def _connect() -> Iterator[pymysql.connections.Connection]:
    conn = pymysql.connect(
        host=os.environ.get("MARIADB_HOST", "mariadb"),
        port=int(os.environ.get("MARIADB_PORT", "3306")),
        user=os.environ.get("MARIADB_USER", "insight"),
        password=os.environ.get("MARIADB_PASSWORD", "insight-local"),
        # Not MARIADB_DB: that one names the IDENTITY database. These tables
        # belong to analytics, which owns a database of its own.
        database=os.environ.get("MARIADB_ANALYTICS_DB", "analytics"),
        autocommit=False,
        cursorclass=pymysql.cursors.Cursor,
    )
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def seed_table_columns(cur: pymysql.cursors.Cursor) -> list[dict[str, str]]:
    """Two platform-visible column rows, in two distinct tables.

    `insight_tenant_id` NULL means platform-visible: the handler shows a
    NULL-tenant row to every tenant (`InsightTenantId.is_null()` OR equals the
    caller's). Seeding them tenant-less keeps the fixture usable from any
    persona, which is what a catalogue is for.
    """
    rows = []
    for row_id, (table, field) in zip(COLUMN_ROW_IDS, CATALOGUED_TABLES, strict=True):
        cur.execute(
            "REPLACE INTO table_columns "
            "(id, insight_tenant_id, clickhouse_table, field_name, created_at, updated_at) "
            "VALUES (%s, NULL, %s, %s, UTC_TIMESTAMP(), UTC_TIMESTAMP())",
            (_bin(row_id), table, field),
        )
        rows.append({"table": table, "field": field})

    LOG.info("  table_columns%s", "".join(f"\n    {r['table']}.{r['field']}" for r in rows))
    return rows


#: MySQL identifier shape, for the one statement in this module still built
#: around a column list (`seed_definition_override`'s write — see the comment
#: there for why it cannot be otherwise). Deliberately narrower than what MySQL
#: accepts: every column in this schema is snake_case ASCII, so anything else
#: is either a schema nobody expected or an answer `information_schema` should
#: not have given.
_IDENTIFIER = re.compile(r"\A[A-Za-z_][A-Za-z0-9_]{0,63}\Z")


def _column_list(columns: Sequence[str]) -> str:
    """A backtick-quoted column list, every name checked as it goes in.

    Values in this module are bound; a column LIST cannot be, because it is not
    a parameter — so it is validated instead, at the point of use rather than
    asserted in a comment somewhere above it.

    Not defence against a hostile caller; there is none, the names come from
    `information_schema`. Defence against a silent schema: an answer this module
    did not expect should stop the seed with the name in the message, not be
    pasted into a statement and produce a syntax error three frames away.
    """
    for column in columns:
        if not _IDENTIFIER.match(column):
            raise ValueError(
                f"refusing to build SQL around {column!r}: information_schema returned "
                "a column name that is not the snake_case ASCII this schema uses."
            )
    return ", ".join(f"`{column}`" for column in columns)


def _writable_columns(cur: pymysql.cursors.Cursor, table: str) -> list[str]:
    """A table's column names, in order, minus the ones the engine computes.

    Read from `information_schema` rather than written down here. A hardcoded
    column list tracks whatever the schema looked like the day it was typed and
    silently stops matching after the next migration — the clone below has to
    copy EVERY column, so it cannot be the thing that knows what they are.
    """
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = DATABASE() AND table_name = %s "
        "AND (extra IS NULL OR extra NOT LIKE '%%GENERATED%%') "
        "ORDER BY ordinal_position",
        (table,),
    )
    return [str(r[0]) for r in cur.fetchall()]


def _child_row_id(table: str, source_id: bytes) -> bytes:
    """A deterministic id for a cloned child row.

    Derived from the override's own id so re-seeding an un-torn-down stand
    replaces the same rows rather than accumulating a fresh set every run —
    the same reason the parent id is a constant.
    """
    return uuid_mod.uuid5(uuid_mod.UUID(DEFINITION_ROW_ID), f"{table}:{source_id.hex()}").bytes


#: The child clone is done BY THE DATABASE — `CREATE … LIKE` plus
#: `INSERT … SELECT *` copy whatever the schema currently holds, so this module
#: never enumerates the child tables' columns and cannot drift from them.
#:
#: Only possible because neither child table has a generated column. The parent
#: does (`tenant_id_sentinel`), which rules the implicit column list out
#: entirely: carrying the column over fails with "value specified for generated
#: column", omitting it fails with "column count doesn't match". That is why
#: `seed_definition_override` still builds its write from a column list and this
#: does not.
#:
#: One temp-table NAME serves both tables, so every statement that touches only
#: the copy is shared below and just the three naming a real table differ.
_CHILD_CLONE_SQL: dict[str, tuple[str, str, str]] = {
    "metric_definition_inputs": (
        "CREATE TEMPORARY TABLE seed_child_clone LIKE metric_definition_inputs",
        "INSERT INTO seed_child_clone SELECT * FROM metric_definition_inputs WHERE metric_definition_id = %s",
        "REPLACE INTO metric_definition_inputs SELECT * FROM seed_child_clone",
    ),
    "metric_definition_dimensions": (
        "CREATE TEMPORARY TABLE seed_child_clone LIKE metric_definition_dimensions",
        "INSERT INTO seed_child_clone SELECT * FROM metric_definition_dimensions WHERE metric_definition_id = %s",
        "REPLACE INTO metric_definition_dimensions SELECT * FROM seed_child_clone",
    ),
}

#: Statements that touch only the copy, identical for either table.
#: `CREATE TEMPORARY TABLE` and `DROP TEMPORARY TABLE` are the documented
#: exceptions to MySQL's implicit-commit rule, so none of this escapes the
#: transaction `_connect` opens.
_CLONE_DROP = "DROP TEMPORARY TABLE IF EXISTS seed_child_clone"
_CLONE_REKEY_PARENT = "UPDATE seed_child_clone SET metric_definition_id = %s"
_CLONE_SELECT_IDS = "SELECT id FROM seed_child_clone"
_CLONE_REKEY_ID = "UPDATE seed_child_clone SET id = %s WHERE id = %s"


def _clone_children(cur: pymysql.cursors.Cursor, table: str, src: bytes, dst: bytes) -> int:
    """Re-key one child table's rows onto the cloned definition.

    Through a temporary copy rather than a read-then-write loop, so the column
    inventory stays the database's business. The ids are still chosen here —
    `_child_row_id` keeps them deterministic — and applied as bound UPDATEs.
    """
    create, fill, write_back = _CHILD_CLONE_SQL[table]
    cur.execute(_CLONE_DROP)
    cur.execute(create)
    cur.execute(fill, (src,))
    cur.execute(_CLONE_REKEY_PARENT, (dst,))

    # Re-keyed one row at a time: the id is derived from the SOURCE id, which
    # only Python knows how to compute. Safe to do in place because a derived id
    # never collides with a source id still waiting its turn.
    cur.execute(_CLONE_SELECT_IDS)
    child_ids = [row[0] for row in cur.fetchall()]
    for child_id in child_ids:
        cur.execute(_CLONE_REKEY_ID, (_child_row_id(table, child_id), child_id))

    cur.execute(write_back)
    cur.execute(_CLONE_DROP)
    return len(child_ids)


#: WHICH product definition gets overridden. `SELECT *` rather than a column
#: list because the copy below wants every column anyway, and reading them off
#: the cursor keeps this statement a plain literal.
_SELECT_PRODUCT_DEFINITION = (
    "SELECT * FROM metric_definitions WHERE tenant_id IS NULL ORDER BY metric_key LIMIT 1"
)


def seed_definition_override(
    cur: pymysql.cursors.Cursor, tenant_uuid: str
) -> dict[str, str] | None:
    """Override one product definition's label for this tenant.

    WHICH key is chosen at seed time rather than pinned here: the product's
    definitions come from migrations and this seed must not carry a second copy
    of that list to go stale. The lowest key by sort order is deterministic
    given the same migrations, and the choice is recorded in the manifest so the
    test reads it instead of guessing.

    A FAITHFUL clone — every column, plus the input and dimension rows that hang
    off the definition. A tenant row SHADOWS the product default rather than
    decorating it, so a partial copy does not produce a definition that is
    mostly right: it produces one the resolver rejects outright (`missing Value
    input for …`), and every metric-results call touching that key answers 500.
    The override is meant to change the LABEL and nothing else, so everything
    else has to come across.

    Returns None when the product has no definitions at all — a stand whose
    migrations have not run, which is the migrations' problem to report, not
    this seed's to fail on.
    """
    cur.execute(_SELECT_PRODUCT_DEFINITION)
    row = cur.fetchone()
    if row is None:
        LOG.warning("  metric_definitions: no product definitions to override — skipped")
        return None

    # Column names off the cursor rather than from a list built for the query:
    # `SELECT *` means the read no longer has to know them.
    present = [str(d[0]) for d in cur.description]
    values = dict(zip(present, row))  # noqa: B905 — 3.9-compatible, lengths are equal by construction
    source_id = values["id"]
    metric_key = str(values["metric_key"])

    values["id"] = _bin(DEFINITION_ROW_ID)
    values["tenant_id"] = _bin(tenant_uuid)
    values["label"] = OVERRIDE_LABEL
    if "origin" in values:
        values["origin"] = "custom"

    # The write cannot use `SELECT *`'s column set: it includes the generated
    # `tenant_id_sentinel`, which no statement may assign.
    columns = _writable_columns(cur, "metric_definitions")
    selected = _column_list(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    # The one statement in this module still built around a column list, and it
    # cannot be otherwise. Every implicit-column-list form is closed off by that
    # generated column: carrying it over gives "value specified for generated
    # column", omitting it gives "column count doesn't match". Naming the columns
    # is what is left, and the names come from `information_schema` so the list
    # cannot go stale the way a hardcoded one would.
    cur.execute(  # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
        f"REPLACE INTO `metric_definitions` ({selected}) VALUES ({placeholders})",
        tuple(values[c] for c in columns),
    )

    cloned = {
        table: _clone_children(cur, table, source_id, values["id"])
        for table in ("metric_definition_inputs", "metric_definition_dimensions")
    }

    LOG.info(
        "  metric_definitions\n    %s → %r (%s)",
        metric_key,
        OVERRIDE_LABEL,
        ", ".join(f"{n} {t.removeprefix('metric_definition_')}" for t, n in cloned.items()),
    )
    return {"metric_key": metric_key, "label": OVERRIDE_LABEL}


def run() -> dict[str, Any]:
    """Seed both catalogues; return what was written, for the manifest.

    Returned rather than written to the manifest here because `build_manifest`
    is pure — it reads its own sources and nothing else, so a fact discovered at
    seed time has to be handed to it.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    tenant = os.environ.get("TENANT_DEFAULT_ID", "00000000-df51-5b42-9538-d2b56b7ee953")

    LOG.info("analytics catalogue seed (tenant %s)", tenant)
    with _connect() as conn:
        cur = conn.cursor()
        columns = seed_table_columns(cur)
        override = seed_definition_override(cur, tenant)

    return {"table_columns": columns, "definition_override": override}
