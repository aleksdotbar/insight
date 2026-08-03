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


def test_metric_drilldown_400_pre_cutover_email_entity(api) -> None:
    """entity.id is a canonical person UUID since the identity cutover — the
    email shape is a loud 400, same as every other person-keyed route."""
    body = _request() | {"entity": {"type": "person", "id": "somebody@example.com"}}
    response = api.post("/v1/metric-drilldown", json=body)
    assert response.status_code == 400, f"status={response.status_code} body={response.text}"


def test_metric_drilldown_400_nil_uuid_entity(api) -> None:
    """The nil UUID parses but is never a person."""
    body = _request() | {"entity": {"type": "person", "id": "00000000-0000-0000-0000-000000000000"}}
    response = api.post("/v1/metric-drilldown", json=body)
    assert response.status_code == 400, f"status={response.status_code} body={response.text}"


def test_metric_drilldown_415_wrong_content_type(api) -> None:
    response = text_body_request(api, "POST", "/v1/metric-drilldown")
    assert response.status_code == 415, f"status={response.status_code} body={response.text}"


def test_metric_drilldown_export_400_empty_entity(api) -> None:
    request = _request()
    request["format"] = "csv"
    request.pop("limit")
    response = api.post("/v1/metric-drilldown/export", json=request)
    assert response.status_code == 400, f"status={response.status_code} body={response.text}"


def test_metric_drilldown_export_415_wrong_content_type(api) -> None:
    response = text_body_request(api, "POST", "/v1/metric-drilldown/export")
    assert response.status_code == 415, f"status={response.status_code} body={response.text}"
