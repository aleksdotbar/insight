"""Thresholds — the per-metric kind, and the tenant-scope admin kind.

    GET    /v1/metrics/{id}/thresholds          200
    POST   /v1/metrics/{id}/thresholds          xfail #1663
    PUT    /v1/metrics/{id}/thresholds/{tid}    blocked by the same bug
    DELETE /v1/metrics/{id}/thresholds/{tid}    blocked by the same bug

    GET    /v1/admin/metric-thresholds          200
    POST   /v1/admin/metric-thresholds          201
    GET    /v1/admin/metric-thresholds/{id}     200 · 404 unknown
    PUT    /v1/admin/metric-thresholds/{id}     200
    DELETE /v1/admin/metric-thresholds/{id}     204

Two families in one module because they are the same concept at two scopes, and
because the contrast is the point: the admin one round-trips cleanly while the
per-metric one cannot be created at all.

The 401 half is in `test_gateway.py`, swept over every operation at once.
"""

from __future__ import annotations

import pytest
from insight_stand import ApiClient, analytics_path

from . import scratch
from .schemas import (
    AdminMetricThresholdList,
    AdminMetricThresholdView,
    CatalogResponse,
    Metric,
    ProblemDocument,
    ThresholdListResponse,
)

ADMIN_THRESHOLDS = analytics_path("/v1/admin/metric-thresholds")


def _catalog_metric_id(api: ApiClient) -> str:
    """A real `metric_catalog` row — admin thresholds validate against one.

    Read rather than created: the catalogue is out of bounds for this suite (it
    is the metric-coverage gate's universe), so a threshold test borrows an
    existing row and cleans up only its own threshold.
    """
    response = api.post(analytics_path("/v1/catalog/get_metrics"), json_body={})
    assert response.status_code == 200, f"catalog: {response.status_code} {response.text[:300]}"
    metrics = response.parse(CatalogResponse).metrics
    assert metrics, "the metric catalogue is empty — was this stand seeded?"
    return str(metrics[0].id)


# ---------------------------------------------------------------------------
# Per-metric thresholds
# ---------------------------------------------------------------------------


def test_metric_thresholds_listing_is_200(api: ApiClient, scratch_metric: Metric) -> None:
    """A fresh metric has no thresholds, and says so with an empty list.

    Worth asserting despite being empty: `{"items": []}` and a 200 with no body
    are different answers, and only the model tells them apart.
    """
    response = api.get(analytics_path(f"/v1/metrics/{scratch_metric.id}/thresholds"))
    assert response.status_code == 200, f"status={response.status_code} {response.text[:300]}"
    assert response.parse(ThresholdListResponse).items == []


@pytest.mark.xfail(
    reason="#1663: creating a metric threshold 500s on read-back (DECIMAL value vs f64 entity)",
    strict=True,
)
def test_metric_threshold_create_round_trip(api: ApiClient, scratch_metric: Metric) -> None:
    """`POST` 201 → listed → `DELETE` 204 → gone.

    Currently unreachable: the create answers 500. The same defect blocks this
    in the in-process rig, so it is a product bug rather than anything about the
    deployed path — reproducing it HERE is the useful part, because it says the
    500 is not an artefact of running services in-process.

    `strict=True` deliberately: when #1663 is fixed this XPASSes and fails the
    run, which is the notification to delete this marker. A non-strict xfail
    would let the fix land unnoticed and leave the endpoint uncovered.
    """
    thresholds = analytics_path(f"/v1/metrics/{scratch_metric.id}/thresholds")
    created = api.post(
        thresholds,
        json_body={"field_name": "one", "operator": "ge", "value": 1.0, "level": "good"},
    )
    assert created.status_code == 201, f"create: {created.status_code} {created.text[:300]}"

    body = created.json()
    assert isinstance(body, dict)
    threshold_id = body["id"]

    listed = api.get(thresholds).parse(ThresholdListResponse)
    assert str(threshold_id) in {str(item.id) for item in listed.items}

    assert api.delete(f"{thresholds}/{threshold_id}").status_code == 204
    assert api.get(thresholds).parse(ThresholdListResponse).items == []


# ---------------------------------------------------------------------------
# Admin (tenant-scope) thresholds
# ---------------------------------------------------------------------------


def test_admin_thresholds_listing_is_200(api: ApiClient) -> None:
    response = api.get(ADMIN_THRESHOLDS)
    assert response.status_code == 200, f"status={response.status_code} {response.text[:300]}"
    response.parse(AdminMetricThresholdList)


def test_admin_threshold_create_read_update_delete_round_trip(api: ApiClient) -> None:
    """One cycle at tenant scope, on a real catalogue metric.

    `good == warn` passes the sanity-bounds check whichever direction the metric
    is scored in, so the case does not depend on which catalogue row it borrowed.

    The pre-clean is not optional. `(metric, tenant, scope)` is UNIQUE and a
    stand keeps its database between runs, so a row left by an earlier run would
    turn every later create into a 409.
    """
    metric_id = _catalog_metric_id(api)
    scoped = {"metric_id": metric_id, "scope": "tenant"}

    existing = api.get(ADMIN_THRESHOLDS, params=scoped).parse(AdminMetricThresholdList)
    for row in existing.items:
        api.delete(f"{ADMIN_THRESHOLDS}/{row.id}")

    created = api.post(ADMIN_THRESHOLDS, json_body={**scoped, "good": 0.0, "warn": 0.0})
    assert created.status_code == 201, f"create: {created.status_code} {created.text[:300]}"
    threshold = created.parse(AdminMetricThresholdView)
    assert str(threshold.metric_id) == metric_id
    threshold_id = scratch.track(ADMIN_THRESHOLDS, "id", threshold.id)

    fetched = api.get(f"{ADMIN_THRESHOLDS}/{threshold_id}")
    assert fetched.status_code == 200, f"read back: {fetched.status_code} {fetched.text[:300]}"
    assert str(fetched.parse(AdminMetricThresholdView).id) == threshold_id

    updated = api.put(f"{ADMIN_THRESHOLDS}/{threshold_id}", json_body={"good": 1.0, "warn": 1.0})
    assert updated.status_code == 200, f"update: {updated.status_code} {updated.text[:300]}"
    assert updated.parse(AdminMetricThresholdView).good == 1.0

    assert api.delete(f"{ADMIN_THRESHOLDS}/{threshold_id}").status_code == 204
    assert api.get(f"{ADMIN_THRESHOLDS}/{threshold_id}").status_code == 404


def test_admin_threshold_404_unknown(api: ApiClient) -> None:
    response = api.get(f"{ADMIN_THRESHOLDS}/{scratch.UNKNOWN_ID}")
    assert response.status_code == 404, f"status={response.status_code} {response.text[:300]}"
    assert response.parse(ProblemDocument).status == 404
