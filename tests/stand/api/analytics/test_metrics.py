"""The `/v1/metrics` path group on analytics, through the gateway.

    GET    /v1/metrics              200 list · 200 excludes soft-deleted
    POST   /v1/metrics              201 · 415 wrong-ct · 422 off-schema
    GET    /v1/metrics/{id}         200 · 400 non-uuid · 404 unknown · 404 soft-deleted
    PUT    /v1/metrics/{id}         200 · 404 unknown
    DELETE /v1/metrics/{id}         204 · 404 unknown
    POST   /v1/metrics/{id}/query   200 · 404 unknown
    POST   /v1/metrics/queries      200 batch

Every case here runs with a REAL session against the deployed stack. The
in-process rig covers the same operations more exhaustively — every validation
permutation, every media-type variant — and that division is deliberate: the
rig owns contract correctness, this file owns the deployed path. What it adds
over the rig is that the request survived the gateway with a session cookie: the
prefix was stripped, the JWT was minted and verified, and the tenant was derived
from it rather than from a header. The 401 half lives in `test_gateway.py`,
swept over every operation at once.

The scratch metric's `query_ref` runs the real engine end to end — parsed,
validated, wrapped and executed on ClickHouse, returning one deterministic row.
"""

from __future__ import annotations

from insight_stand import ApiClient, analytics_path

from ..schemas import Metric, MetricListResponse, QueryResponse
from ..scratch import SCRATCH_QUERY_REF, UNKNOWN_ID, create_metric

METRICS = analytics_path("/v1/metrics")


def _metric_path(metric_id: object, suffix: str = "") -> str:
    return analytics_path(f"/v1/metrics/{metric_id}{suffix}")


def _catalogue(api: ApiClient) -> set[str]:
    """Every metric name the listing reports, validated on the way through."""
    response = api.get(METRICS)
    assert response.status_code == 200, f"status={response.status_code} {response.text[:300]}"
    return {item.name for item in response.parse(MetricListResponse).items}


def test_list_metrics_200_returns_the_seeded_catalog(api: ApiClient) -> None:
    """The stand's own catalog, with no scratch row involved.

    Separate from the listing test below on purpose: a 200 carrying an empty
    body would be a different defect from a 200 that omits a row just created,
    and only this one would catch a stand whose seed never ran.
    """
    assert _catalogue(api), "the metric catalog is empty"


def test_list_metrics_200(api: ApiClient, scratch_metric: Metric) -> None:
    assert scratch_metric.name in _catalogue(api)


def test_list_metrics_200_excludes_soft_deleted(api: ApiClient, scratch_metric: Metric) -> None:
    """A soft delete has to disappear from the listing, not merely be flagged."""
    assert api.delete(_metric_path(scratch_metric.id)).status_code == 204
    assert scratch_metric.name not in _catalogue(api)


def test_create_metric_201(api: ApiClient) -> None:
    """The helper asserts the 201 and the echoed definition; this owns cleanup."""
    created = create_metric(api, "create")
    assert api.delete(_metric_path(created.id)).status_code == 204


def test_create_metric_422_off_schema_body(api: ApiClient) -> None:
    """Well-formed JSON that is not the request type.

    422 with Axum's own plain-text envelope rather than a canonical Problem
    document — the legacy `axum::Json` extractor, tracked upstream as #1670.
    Asserted as it behaves; when the extractor is made canonical this fails and
    is the reminder to update it.
    """
    response = api.post(METRICS, json_body={"not": "a metric"})
    assert response.status_code == 422, f"status={response.status_code} body={response.text[:300]}"


def test_get_metric_200(api: ApiClient, scratch_metric: Metric) -> None:
    response = api.get(_metric_path(scratch_metric.id))
    assert response.status_code == 200, f"status={response.status_code} body={response.text[:300]}"
    assert response.parse(Metric).id == scratch_metric.id


def test_get_metric_404_unknown(api: ApiClient) -> None:
    response = api.get(_metric_path(UNKNOWN_ID))
    assert response.status_code == 404, f"status={response.status_code} body={response.text[:300]}"


def test_get_metric_404_after_soft_delete(api: ApiClient, scratch_metric: Metric) -> None:
    assert api.delete(_metric_path(scratch_metric.id)).status_code == 204
    response = api.get(_metric_path(scratch_metric.id))
    assert response.status_code == 404, f"status={response.status_code} body={response.text[:300]}"


def test_update_metric_200(api: ApiClient, scratch_metric: Metric) -> None:
    response = api.put(
        _metric_path(scratch_metric.id),
        json_body={
            "name": scratch_metric.name,
            "description": "updated by the stand suite",
            "query_ref": SCRATCH_QUERY_REF,
        },
    )
    assert response.status_code == 200, f"status={response.status_code} body={response.text[:300]}"
    assert response.parse(Metric).description == "updated by the stand suite"


def test_update_metric_404_unknown(api: ApiClient) -> None:
    response = api.put(
        _metric_path(UNKNOWN_ID),
        json_body={"name": "absent", "description": "x", "query_ref": SCRATCH_QUERY_REF},
    )
    assert response.status_code == 404, f"status={response.status_code} body={response.text[:300]}"


def test_delete_metric_204(api: ApiClient, scratch_metric: Metric) -> None:
    response = api.delete(_metric_path(scratch_metric.id))
    assert response.status_code == 204, f"status={response.status_code} body={response.text[:300]}"


def test_delete_metric_404_unknown(api: ApiClient) -> None:
    response = api.delete(_metric_path(UNKNOWN_ID))
    assert response.status_code == 404, f"status={response.status_code} body={response.text[:300]}"


def test_query_metric_200(api: ApiClient, scratch_metric: Metric) -> None:
    """The deterministic row comes back — the query really reached ClickHouse."""
    response = api.post(_metric_path(scratch_metric.id, "/query"), json_body={})
    assert response.status_code == 200, f"status={response.status_code} body={response.text[:300]}"
    assert response.parse(QueryResponse).items == [{"one": 1}], (
        f"the scratch metric's query should return one deterministic row: {response.text[:300]}"
    )


def test_query_metric_404_unknown(api: ApiClient) -> None:
    response = api.post(_metric_path(UNKNOWN_ID, "/query"), json_body={})
    assert response.status_code == 404, f"status={response.status_code} body={response.text[:300]}"


def test_batch_queries_200(api: ApiClient, scratch_metric: Metric) -> None:
    response = api.post(
        analytics_path("/v1/metrics/queries"),
        json_body={"queries": [{"metric_id": str(scratch_metric.id)}]},
    )
    assert response.status_code == 200, f"status={response.status_code} body={response.text[:300]}"
