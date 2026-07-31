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

from insight_stand import ApiClient, ApiResponse, JsonValue, analytics_path

from .scratch import NON_UUID, SCRATCH_QUERY_REF, UNKNOWN_ID, create_metric

METRICS = analytics_path("/v1/metrics")


def _metric_path(metric_id: object, suffix: str = "") -> str:
    return analytics_path(f"/v1/metrics/{metric_id}{suffix}")


def _names(response: ApiResponse) -> list[str]:
    """Names in a listing, or a readable failure about its shape."""
    body = response.json()
    assert isinstance(body, dict), f"expected a JSON object from {response.url}: {response.text[:300]}"
    items = body.get("items")
    assert isinstance(items, list), f"listing has no 'items' array: {response.text[:300]}"
    return [str(item["name"]) for item in items if isinstance(item, dict) and "name" in item]


def test_list_metrics_200_returns_the_seeded_catalog(api: ApiClient) -> None:
    """The stand's own catalog, with no scratch row involved.

    Separate from the listing test below on purpose: a 200 carrying an empty
    body would be a different defect from a 200 that omits a row just created,
    and only this one would catch a stand whose seed never ran.
    """
    response = api.get(METRICS)
    assert response.status_code == 200, f"status={response.status_code} body={response.text[:300]}"
    assert _names(response), f"the metric catalog is empty: {response.text[:400]}"


def test_list_metrics_200(api: ApiClient, scratch_metric: dict[str, JsonValue]) -> None:
    response = api.get(METRICS)
    assert response.status_code == 200, f"status={response.status_code} body={response.text[:300]}"
    assert scratch_metric["name"] in _names(response)


def test_list_metrics_200_excludes_soft_deleted(
    api: ApiClient, scratch_metric: dict[str, JsonValue]
) -> None:
    """A soft delete has to disappear from the listing, not merely be flagged."""
    assert api.delete(_metric_path(scratch_metric["id"])).status_code == 204
    assert scratch_metric["name"] not in _names(api.get(METRICS))


def test_create_metric_201(api: ApiClient) -> None:
    """The helper asserts the 201 and the echoed definition; this owns cleanup."""
    created = create_metric(api, "create")
    assert api.delete(_metric_path(created["id"])).status_code == 204


def test_create_metric_415_wrong_content_type(api: ApiClient) -> None:
    """A body the service must refuse on its media type, not parse.

    Worth asserting through the gateway specifically: a proxy that rewrote or
    dropped `Content-Type` would turn this into a 422 or a 201, and the
    in-process rig cannot see that happen.
    """
    response = api.post(METRICS, content="{}", headers={"Content-Type": "text/plain"})
    assert response.status_code == 415, f"status={response.status_code} body={response.text[:300]}"


def test_create_metric_422_off_schema_body(api: ApiClient) -> None:
    """Well-formed JSON that is not the request type.

    422 with Axum's own plain-text envelope rather than a canonical Problem
    document — the legacy `axum::Json` extractor, tracked upstream as #1670.
    Asserted as it behaves; when the extractor is made canonical this fails and
    is the reminder to update it.
    """
    response = api.post(METRICS, json_body={"not": "a metric"})
    assert response.status_code == 422, f"status={response.status_code} body={response.text[:300]}"


def test_get_metric_200(api: ApiClient, scratch_metric: dict[str, JsonValue]) -> None:
    response = api.get(_metric_path(scratch_metric["id"]))
    assert response.status_code == 200, f"status={response.status_code} body={response.text[:300]}"
    body = response.json()
    assert isinstance(body, dict) and body["id"] == scratch_metric["id"]


def test_get_metric_400_non_uuid(api: ApiClient) -> None:
    """`Path<Uuid>` fails to deserialize before any handler logic runs."""
    response = api.get(_metric_path(NON_UUID))
    assert response.status_code == 400, f"status={response.status_code} body={response.text[:300]}"


def test_get_metric_404_unknown(api: ApiClient) -> None:
    response = api.get(_metric_path(UNKNOWN_ID))
    assert response.status_code == 404, f"status={response.status_code} body={response.text[:300]}"


def test_get_metric_404_after_soft_delete(
    api: ApiClient, scratch_metric: dict[str, JsonValue]
) -> None:
    assert api.delete(_metric_path(scratch_metric["id"])).status_code == 204
    response = api.get(_metric_path(scratch_metric["id"]))
    assert response.status_code == 404, f"status={response.status_code} body={response.text[:300]}"


def test_update_metric_200(api: ApiClient, scratch_metric: dict[str, JsonValue]) -> None:
    response = api.put(
        _metric_path(scratch_metric["id"]),
        json_body={
            "name": scratch_metric["name"],
            "description": "updated by the stand suite",
            "query_ref": SCRATCH_QUERY_REF,
        },
    )
    assert response.status_code == 200, f"status={response.status_code} body={response.text[:300]}"
    body = response.json()
    assert isinstance(body, dict) and body["description"] == "updated by the stand suite"


def test_update_metric_404_unknown(api: ApiClient) -> None:
    response = api.put(
        _metric_path(UNKNOWN_ID),
        json_body={"name": "absent", "description": "x", "query_ref": SCRATCH_QUERY_REF},
    )
    assert response.status_code == 404, f"status={response.status_code} body={response.text[:300]}"


def test_delete_metric_204(api: ApiClient, scratch_metric: dict[str, JsonValue]) -> None:
    response = api.delete(_metric_path(scratch_metric["id"]))
    assert response.status_code == 204, f"status={response.status_code} body={response.text[:300]}"


def test_delete_metric_404_unknown(api: ApiClient) -> None:
    response = api.delete(_metric_path(UNKNOWN_ID))
    assert response.status_code == 404, f"status={response.status_code} body={response.text[:300]}"


def test_query_metric_200(api: ApiClient, scratch_metric: dict[str, JsonValue]) -> None:
    """The deterministic row comes back — the query really reached ClickHouse."""
    response = api.post(_metric_path(scratch_metric["id"], "/query"), json_body={})
    assert response.status_code == 200, f"status={response.status_code} body={response.text[:300]}"


def test_query_metric_404_unknown(api: ApiClient) -> None:
    response = api.post(_metric_path(UNKNOWN_ID, "/query"), json_body={})
    assert response.status_code == 404, f"status={response.status_code} body={response.text[:300]}"


def test_batch_queries_200(api: ApiClient, scratch_metric: dict[str, JsonValue]) -> None:
    response = api.post(
        analytics_path("/v1/metrics/queries"),
        json_body={"queries": [{"metric_id": scratch_metric["id"]}]},
    )
    assert response.status_code == 200, f"status={response.status_code} body={response.text[:300]}"
