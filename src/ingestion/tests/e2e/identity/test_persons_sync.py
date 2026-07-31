"""Contract: persons-sync — the `sync` CLI trigger + the GET journal routes.

The sync copies the ENTIRE MariaDB `persons` observation log into ClickHouse
`identity.identity_persons` (full snapshot, atomic swap) so the metrics
pipeline can resolve `email -> person_id` at dbt-build time. CLI-only from
birth, mirroring the persons-seed trigger model (#1690): `identity-resolution
sync` run synchronously (Helm CronJob / manual Job), exit codes
0 ok / 1 failed / 2 lock busy / 3 empty-log guard; only the GET journal
routes exist over HTTP. Rust-only module (`supports_persons_sync`) — on the
`dotnet` run every case skips.

The fixture dataset (lib/identity_seed.py) gives the log a deterministic
floor — the fixture people's observations — which the end-to-end case
asserts against in ClickHouse, keyed by fixed person UUIDs.

The empty-log guard (exit 3) is deliberately NOT covered here: triggering it
requires an empty `persons` table, and this suite shares one seeded database
across modules — wiping it would destroy the fixture tree every read test
depends on. The guard decision is a pure function unit-tested in the binary
(sync_runner::tests).

Unlike persons-seed there is no tenant scoping to isolate: the sync copies
the whole log verbatim (single-tenant reality, #1550) and writes nothing back
to MariaDB, so it cannot disturb the fixture tree. The journal row lands
under TEST_TENANT_ID (passed as the run's tenant) so the fixture admin
(alice) reads it over the GETs.
"""

from __future__ import annotations

import uuid

import pytest

from identity.contract import items_of
from lib import clickhouse
from lib import identity_seed as seed
from lib.config import TEST_TENANT_ID, SessionConfig

pytestmark = [pytest.mark.identity, pytest.mark.mutating]

IDENTITY_PERSONS = "identity.identity_persons"

# Author the CLI stamps on its journal rows (no JWT on that path) — the
# SYSTEM_AUTHOR nil-UUID convention shared with the seed CLI.
SYSTEM_AUTHOR = "00000000-0000-0000-0000-000000000000"


@pytest.fixture(autouse=True)
def _rust_only(identity_svc):
    if not identity_svc.supports_persons_sync:
        pytest.skip("the sync CLI exists only in the Rust implementation")


def _run_sync(identity_svc, **kwargs):
    """One CLI run under the fixture tenant, asserted successful."""
    proc = identity_svc.run_sync_cli(tenant=str(TEST_TENANT_ID), **kwargs)
    assert proc.returncode == 0, f"rc={proc.returncode}\nstdout={proc.stdout}\nstderr={proc.stderr}"
    return proc


def _latest_op(api) -> dict:
    """Newest persons-sync journal row (the CLI is synchronous, so the run
    that just returned is terminal and first in the DESC-ordered list)."""
    r = api.get("/v1/persons-sync?limit=1")
    assert r.status_code == 200, f"status={r.status_code} body={r.text}"
    ops = items_of(r.json())
    assert len(ops) == 1, ops
    return ops[0]


def _snapshot_count(cfg: SessionConfig) -> int:
    return clickhouse.query(cfg, f"SELECT count() FROM {IDENTITY_PERSONS}")[0][0]


def test_persons_sync_end_to_end(identity_svc, api, compose_stack: SessionConfig) -> None:
    """CLI exit 0 → journal row completed (system author, cli trigger) →
    ClickHouse holds the snapshot: row count matches the summary, the fixture
    people's email observations arrived with their fixed person UUIDs, and
    the watermark is stamped."""
    _run_sync(identity_svc)

    op = _latest_op(api)
    assert op["status"] == "completed", op
    assert op["operation_type"] == "persons-sync", op
    assert op["author_person_id"] == SYSTEM_AUTHOR, op
    assert (op.get("request") or {}).get("trigger") == "cli", op
    summary = op.get("summary") or {}
    # The fixture dataset alone puts dozens of observations in the log.
    assert summary.get("rows", 0) > 0, op
    assert summary.get("max_id"), op
    assert summary.get("synced_at"), op

    # identity_persons carries EXACTLY the snapshot the summary reports — the
    # binary's own count-verify ran before the swap; this re-checks it
    # end-to-end through an independent client.
    assert _snapshot_count(compose_stack) == summary["rows"]

    # Fixture people arrived intact: raw email observation, fixed person UUID.
    rows = clickhouse.query(
        compose_stack,
        f"SELECT DISTINCT person_id FROM {IDENTITY_PERSONS} "  # noqa: S608 — fixed table, fixed test literal
        f"WHERE value_type = 'email' AND value_id = '{seed.ALICE_EMAIL}'",
    )
    assert [str(row[0]) for row in rows] == [str(seed.ALICE)], rows

    # The shared-email pair kept DISTINCT person ids — identity_persons is the
    # raw log, not a resolution: collapsing dup1/dup2 is the resolve macro's
    # call, downstream in dbt.
    dup_rows = clickhouse.query(
        compose_stack,
        f"SELECT DISTINCT person_id FROM {IDENTITY_PERSONS} "  # noqa: S608 — fixed table, fixed test literal
        f"WHERE value_type = 'email' AND value_id = '{seed.DUP_EMAIL}'",
    )
    assert {str(row[0]) for row in dup_rows} == {str(seed.DUP1), str(seed.DUP2)}, dup_rows

    # Every copied row is stamped with the run's watermark.
    stamped = clickhouse.query(
        compose_stack,
        f"SELECT count() FROM {IDENTITY_PERSONS} WHERE _synced_at > toDateTime64(0, 3)",
    )
    assert stamped[0][0] == summary["rows"], stamped


def test_persons_sync_replaces_not_appends(
    identity_svc, api, compose_stack: SessionConfig
) -> None:
    """Two consecutive runs leave the table equal to ONE snapshot — the
    replace-swap semantics: a re-run must never double it."""
    _run_sync(identity_svc)
    first = _latest_op(api)
    assert first["status"] == "completed", first
    _run_sync(identity_svc)
    second = _latest_op(api)
    assert second["status"] == "completed", second
    assert second["operation_id"] != first["operation_id"], (first, second)

    # The log only grows (append-only), and the table equals the LAST
    # snapshot — not the sum of both.
    assert second["summary"]["rows"] >= first["summary"]["rows"], (first, second)
    assert _snapshot_count(compose_stack) == second["summary"]["rows"]


def test_persons_sync_journal_get_by_id(identity_svc, api) -> None:
    """The single-operation GET returns the run the list shows."""
    _run_sync(identity_svc)
    listed = _latest_op(api)
    r = api.get(f"/v1/persons-sync/{listed['operation_id']}")
    assert r.status_code == 200, f"status={r.status_code} body={r.text}"
    assert r.json()["operation_id"] == listed["operation_id"], r.json()
    assert r.json()["status"] == "completed", r.json()


def test_persons_sync_journal_lists_only_sync_ops(identity_svc, api) -> None:
    """Operation-type scoping both ways: the sync journal carries ONLY
    persons-sync rows, and a sync operation is 404 on the SEED journal — one
    `operations` table, two disjoint API surfaces."""
    _run_sync(identity_svc)
    ops = items_of(api.get("/v1/persons-sync").json())
    assert ops, "at least the run above must be listed"
    assert all(op["operation_type"] == "persons-sync" for op in ops), ops

    sync_op = ops[0]["operation_id"]
    r = api.get(f"/v1/persons-seed/{sync_op}")
    assert r.status_code == 404, f"status={r.status_code} body={r.text}"
    seed_ops = items_of(api.get("/v1/persons-seed").json())
    assert sync_op not in {op["operation_id"] for op in seed_ops}, seed_ops


def test_persons_sync_journal_status_filter(identity_svc, api) -> None:
    """`?status=` filters: a finished CLI run is `completed` (the CLI is
    synchronous — no queued/running to race against) and absent from the
    `failed` view."""
    _run_sync(identity_svc)
    op = _latest_op(api)
    completed = items_of(api.get("/v1/persons-sync?status=completed").json())
    assert op["operation_id"] in {o["operation_id"] for o in completed}, completed
    failed = items_of(api.get("/v1/persons-sync?status=failed").json())
    assert op["operation_id"] not in {o["operation_id"] for o in failed}, failed


def test_persons_sync_get_unknown_id_404(api) -> None:
    r = api.get(f"/v1/persons-sync/{uuid.uuid4()}")
    assert r.status_code == 404, f"status={r.status_code} body={r.text}"


def test_persons_sync_journal_403_non_admin(bob_api) -> None:
    """bob is not an admin anywhere — the journal is refused."""
    assert bob_api.get("/v1/persons-sync").status_code == 403
    assert bob_api.get(f"/v1/persons-sync/{uuid.uuid4()}").status_code == 403


def test_persons_sync_journal_401_unauthenticated(anon_api) -> None:
    assert anon_api.get("/v1/persons-sync").status_code == 401
