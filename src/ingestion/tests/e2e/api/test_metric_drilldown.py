from __future__ import annotations

import pytest

from api.endpoint_helpers import text_body_request

pytestmark = pytest.mark.api


def _request() -> dict:
    return {
        "metric_key": "tasks.closed",
        "entity": {"type": "person", "id": ""},
        "period": {"from": "2026-01-01", "to": "2026-01-31"},
        "filters": [],
        "display_dimensions": [],
        "limit": 100,
    }


def test_metric_drilldown_400_empty_entity(api) -> None:
    response = api.post("/v1/metric-drilldown", json=_request())
    assert response.status_code == 400, f"status={response.status_code} body={response.text}"


def test_metric_drilldown_415_wrong_content_type(api) -> None:
    response = text_body_request(api, "POST", "/v1/metric-drilldown")
    assert response.status_code == 415, f"status={response.status_code} body={response.text}"
