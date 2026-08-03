"""`/v1/metric-drilldown` and its export — the rows behind a metric value.

    POST /v1/metric-drilldown         400 empty-entity · 415 wrong-ct
    POST /v1/metric-drilldown/export  400 empty-entity · 415 wrong-ct

Error contracts only, deliberately. A 200 here needs a metric_key that resolves
against seeded observations AND an entity the caller may see AND a period those
rows fall in — three seeded facts that would make the test an assertion about
the seed rather than about the route. The rig covers the same two codes for the
same reason.

What the stand adds over the rig is that both requests cross a real gateway
carrying a real session, so a 400 here is the handler's own validation rather
than the edge's: an anonymous call to either url is 401 long before any body is
read (`test_gateway.py`), which is what makes these two codes attributable.

`/export` is the newer of the pair (#2074) and the only operation in this suite
that does not answer JSON — it serves CSV or XLSX. Its error contract is still
the shared one, which is exactly why the boilerplate 403 it declares is
subtracted in `coverage.py`: the handler has no gate to produce one.
"""

from __future__ import annotations

import pytest
from insight_stand import ApiClient, analytics_path
from insight_stand.api import JsonValue

DRILLDOWN = analytics_path("/v1/metric-drilldown")
DRILLDOWN_EXPORT = analytics_path("/v1/metric-drilldown/export")

#: Well-formed apart from the one field under test. An empty entity id is the
#: cheapest rejection that is unambiguously the HANDLER's — it needs no seeded
#: metric to reach, and no lookup can turn it into a 404 on the way.
_EMPTY_ENTITY_ID = ""


def _request() -> dict[str, JsonValue]:
    return {
        "metric_key": "tasks.closed",
        "entity": {"type": "person", "id": _EMPTY_ENTITY_ID},
        "period": {"from": "2026-01-01", "to": "2026-01-31"},
        "filters": [],
        "display_dimensions": [],
        "limit": 100,
    }


@pytest.mark.parametrize("path", [DRILLDOWN, DRILLDOWN_EXPORT], ids=["drilldown", "export"])
def test_drilldown_415_wrong_content_type(api: ApiClient, path: str) -> None:
    """A body the service must refuse on its media type, not parse.

    Worth asserting through the gateway specifically: a proxy that rewrote or
    dropped `Content-Type` would turn this into a 422 or a 200, and the
    in-process rig cannot see that happen.
    """
    response = api.post(path, content="{}", headers={"Content-Type": "text/plain"})
    assert response.status_code == 415, f"status={response.status_code} {response.text[:300]}"


def test_drilldown_400_empty_entity_id(api: ApiClient) -> None:
    response = api.post(DRILLDOWN, json_body=_request())
    assert response.status_code == 400, f"status={response.status_code} {response.text[:300]}"


def test_drilldown_export_400_empty_entity_id(api: ApiClient) -> None:
    """Same rejection, and it must happen before any format negotiation.

    `format` is what makes this operation different; validating the entity
    first is what keeps it the same route. A 200 with an empty CSV would be the
    regression worth catching here.
    """
    request = _request()
    request["format"] = "csv"
    del request["limit"]

    response = api.post(DRILLDOWN_EXPORT, json_body=request)
    assert response.status_code == 400, f"status={response.status_code} {response.text[:300]}"
