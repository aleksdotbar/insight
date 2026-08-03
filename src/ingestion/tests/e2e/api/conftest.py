"""Fixtures for the endpoint contract tests (`api/test_*.py`).

Every resource a case needs is a function-scoped fixture that creates the row
through the same recording client the test uses and removes it afterwards —
tests stay one-case (path, method, status code) and order-independent.
Teardown deletes are best-effort on purpose: a delete-case test already
removed its row, so a 404 there is expected, not a failure.
"""

from __future__ import annotations

import uuid

import pytest
from lib import mariadb
from lib.analytics import AnalyticsProcess
from lib.config import TEST_TENANT_ID, SessionConfig

from api.endpoint_helpers import create_scratch_saved_query


@pytest.fixture
def api(analytics: AnalyticsProcess):
    """Recording httpx client (the coverage chokepoint), one per test."""
    with analytics.client() as c:
        yield c


@pytest.fixture
def anon_api(analytics: AnalyticsProcess):
    """Recording client with NO Authorization header (401 cases)."""
    import httpx
    from lib import api_coverage

    with httpx.Client(
        base_url=analytics.base_url, timeout=30.0, event_hooks={"response": [api_coverage.record_response]}
    ) as c:
        yield c


@pytest.fixture
def scratch_saved_query(api) -> dict:
    """A scratch saved query (`e2e-scratch-query-*`, `SELECT 1 FROM system.one`);
    hard-deleted in teardown so it never leaks into `GET /v1/queries`."""
    q = create_scratch_saved_query(api, "e2e-scratch-query")
    yield q
    api.delete(f"/v1/queries/{q['id']}")


@pytest.fixture
def tenant_override_definition(api, session_cfg: SessionConfig) -> dict:
    """A tenant-scoped `metric_definitions` row that overrides an existing
    product metric_key with a distinguishable label, inserted directly into
    MariaDB (definitions are migration-seeded; there is no write endpoint).
    The listing must resolve the tenant label over the product default.
    Removed in teardown so it never leaks into other cases."""
    base = api.get("/v1/metric-definitions")
    assert base.status_code == 200, f"override setup: status={base.status_code} body={base.text}"
    metric_key = base.json()["metrics"][0]["metric_key"]
    row_id = uuid.uuid4().hex.upper()
    label = f"e2e-override-{uuid.uuid4().hex[:8]}"
    mariadb.query(
        session_cfg,
        "INSERT INTO metric_definitions "
        "(id, tenant_id, metric_key, label, format, direction, entity_type, computation_type, origin) "
        f"VALUES (UNHEX('{row_id}'), UNHEX('{TEST_TENANT_ID.hex.upper()}'), '{metric_key}', "
        f"'{label}', 'integer', 'higher_is_better', 'person', 'sum', 'custom')",
    )
    yield {"metric_key": metric_key, "label": label}
    mariadb.query(session_cfg, f"DELETE FROM metric_definitions WHERE id = UNHEX('{row_id}')")
