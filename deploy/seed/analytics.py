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
tests/stand/api/test_analytics_columns.py) — so anything a test needs and no
endpoint creates has to be seeded, and then NAMED IN THE MANIFEST. A test reads
the name from there; it never hardcodes one.

Runs after analytics has migrated: these tables are created by its SeaORM
migrations at startup, not by this seed.
"""

from __future__ import annotations

import logging
import os
import uuid as uuid_mod
from collections.abc import Iterator
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


def seed_definition_override(cur: pymysql.cursors.Cursor, tenant_uuid: str) -> dict[str, str] | None:
    """Override one product definition's label for this tenant.

    WHICH key is chosen at seed time rather than pinned here: the product's
    definitions come from migrations and this seed must not carry a second copy
    of that list to go stale. The lowest key by sort order is deterministic
    given the same migrations, and the choice is recorded in the manifest so the
    test reads it instead of guessing.

    Returns None when the product has no definitions at all — a stand whose
    migrations have not run, which is the migrations' problem to report, not
    this seed's to fail on.
    """
    cur.execute(
        "SELECT metric_key, format, direction, entity_type, computation_type "
        "FROM metric_definitions WHERE tenant_id IS NULL ORDER BY metric_key LIMIT 1"
    )
    row = cur.fetchone()
    if row is None:
        LOG.warning("  metric_definitions: no product definitions to override — skipped")
        return None

    metric_key, fmt, direction, entity_type, computation_type = row
    cur.execute(
        "REPLACE INTO metric_definitions "
        "(id, tenant_id, metric_key, label, format, direction, entity_type, computation_type, origin) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'custom')",
        (
            _bin(DEFINITION_ROW_ID),
            _bin(tenant_uuid),
            metric_key,
            OVERRIDE_LABEL,
            fmt,
            direction,
            entity_type,
            computation_type,
        ),
    )
    LOG.info("  metric_definitions\n    %s → %r", metric_key, OVERRIDE_LABEL)
    return {"metric_key": str(metric_key), "label": OVERRIDE_LABEL}


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
