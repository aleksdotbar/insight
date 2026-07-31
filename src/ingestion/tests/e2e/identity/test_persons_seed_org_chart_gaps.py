"""Versatility gaps flagged in #1602's QA review of the persons-seed org-chart
projection (#1690): every case in `test_persons_seed.py` proves the projection
against identity_inputs rows the fixture INSERTs by hand — the real
connector -> dbt -> `identity.identity_inputs` path is never exercised, and
several BR-8/BR-9 shapes (arbitrary depth, cycles, a second HR source) have no
test at all. This module closes those gaps one at a time.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest
import yaml
from lib import clickhouse
from lib import identity_seed as seed
from lib.ch_seeder import CHSeeder
from lib.config import SessionConfig
from lib.dbt_runner import DbtRunner
from lib.worker import WorkerContext

pytestmark = [pytest.mark.identity, pytest.mark.mutating]

_SCHEMAS_PATH = Path(__file__).parents[1] / "metrics" / "schemas" / "bronze_bamboohr.employees.yaml"
_BAMBOOHR_SCHEMAS = yaml.safe_load(_SCHEMAS_PATH.read_text(encoding="utf-8"))["schemas"]


def _bamboohr_employee(
    *, run_tag: str, entity_id: str, email: str, display_name: str, supervisor_email: str | None
) -> dict:
    """A minimal `bronze_bamboohr.employees` row — the real shape the bamboohr
    connector would append, not a hand-crafted identity_inputs row."""
    return {
        # Non-nullable Airbyte CDK columns — real connector rows always carry
        # these; some staging transformations (e.g. latest-row selection)
        # rely on `_airbyte_extracted_at`.
        "_airbyte_raw_id": str(uuid.uuid4()),
        "_airbyte_extracted_at": "2026-01-05T00:00:00",
        "_airbyte_meta": "{}",
        "_airbyte_generation_id": 0,
        "id": entity_id,
        "unique_key": f"pipeline-{run_tag}-bamboohr-{entity_id}",
        "tenant_id": f"pipeline-tenant-{run_tag}",
        "source_id": f"pipeline-source-{run_tag}",
        "workEmail": email,
        "displayName": display_name,
        "firstName": display_name.split(" ")[0],
        "lastName": display_name.split(" ")[-1],
        "employeeNumber": entity_id,
        "jobTitle": "Engineer",
        "department": "Engineering",
        "division": "Engineering",
        "status": "Active",
        "supervisorEmail": supervisor_email,
        "supervisorEId": None,
    }


def _person_id_by_email(identity_svc, email: str) -> str:
    with identity_svc.client(
        sub=str(seed.SEED_ADMIN), tenant=str(seed.SEED_TENANT), sub_type="service", roles="service"
    ) as svc:
        r = svc.get(f"/internal/persons/by-email/{email}")
        assert r.status_code == 200, f"status={r.status_code} body={r.text}"
        return r.json()["insight_source_id"]


def _open_parent(cfg: SessionConfig, child: str) -> str | None:
    """The single OPEN (valid_to IS NULL) org_chart parent for `child`, under
    SEED_TENANT. Raw SQL on purpose — this asserts the seed's WRITE, mirroring
    `test_persons_seed.py::_org_chart_edges`."""
    with seed._connection(cfg) as conn, conn.cursor() as cur:  # noqa: SLF001 — harness-internal helper
        cur.execute(
            "SELECT LOWER(HEX(parent_person_id))"
            " FROM org_chart"
            " WHERE insight_tenant_id = %s AND child_person_id = %s AND valid_to IS NULL",
            (seed.SEED_TENANT.bytes, uuid.UUID(child).bytes),
        )
        rows = cur.fetchall()
        assert len(rows) <= 1, f"expected at most one open edge for {child}, got {rows}"
        return rows[0][0] if rows else None


def _hex(person: str) -> str:
    return uuid.UUID(person).hex


def test_seed_org_chart_from_real_bamboohr_connector_pipeline(
    identity_svc,
    ch_seeder: CHSeeder,
    dbt_runner: DbtRunner,
    worker_ctx: WorkerContext,
    compose_stack: SessionConfig,
) -> None:
    """The org_chart projection holds when `identity.identity_inputs` is
    populated through the REAL path (bronze -> bamboohr connector dbt models ->
    the shared `identity_inputs` union), not a hand-inserted row. Every other
    org-chart test in this suite bypasses that path entirely; this is the one
    proof that the bamboohr connector's own dbt models actually feed the seed.
    """
    if not identity_svc.supports_seed_cli:
        pytest.skip("the seed CLI exists only on the Rust implementation (#1690)")

    run_tag = uuid.uuid4().hex[:10]
    manager_email = f"pipeline.manager.{run_tag}@e2e.test"
    report_email = f"pipeline.report.{run_tag}@e2e.test"

    ch_seeder.truncate_touched()
    ch_seeder.seed_bronze(
        {
            "bronze_bamboohr.employees": [
                _bamboohr_employee(
                    run_tag=run_tag,
                    entity_id=f"mgr-{run_tag}",
                    email=manager_email,
                    display_name="Pipeline Manager",
                    supervisor_email=None,
                ),
                _bamboohr_employee(
                    run_tag=run_tag,
                    entity_id=f"rep-{run_tag}",
                    email=report_email,
                    display_name="Pipeline Report",
                    supervisor_email=manager_email,
                ),
            ]
        },
        _BAMBOOHR_SCHEMAS,
    )

    staging, silver = dbt_runner.derive_selectors({("bronze_bamboohr", "employees")})
    dbt_runner.build(" ".join(f"+{m}" for m in staging), worker_ctx=worker_ctx)
    assert "identity_inputs" in silver, (
        f"bamboohr__identity_inputs did not surface a silver:identity_inputs tag (silver={silver}) "
        "— derive_selectors no longer sees the connector's identity path"
    )
    dbt_runner.run("identity_inputs", worker_ctx=worker_ctx)

    landed = clickhouse.query(
        compose_stack,
        "SELECT count() FROM identity.identity_inputs"
        f" WHERE insight_source_type = 'bamboohr' AND source_account_id = 'rep-{run_tag}'"
        "   AND value_type = 'parent_email'",
    )
    assert landed[0][0] >= 1, (
        "the bamboohr connector's own dbt models never produced a parent_email row in "
        "identity.identity_inputs for the seeded report — the connector/dbt path is broken, "
        "not just untested"
    )

    res = identity_svc.run_seed_cli(tenant=str(seed.SEED_TENANT), force=True)
    assert res.returncode == 0, f"rc={res.returncode}\n{res.stdout}\n{res.stderr}"

    manager = _person_id_by_email(identity_svc, manager_email)
    report = _person_id_by_email(identity_svc, report_email)
    assert _open_parent(compose_stack, report) == _hex(manager)
