"""Thresholds — the per-metric kind, and the tenant-scope admin kind.

    GET    /v1/metrics/{id}/thresholds          200
    POST   /v1/metrics/{id}/thresholds          xfail #1663
    PUT    /v1/metrics/{id}/thresholds/{tid}    404 unknown · create blocked by #1663
    DELETE /v1/metrics/{id}/thresholds/{tid}    404 unknown · create blocked by #1663

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

from collections.abc import Iterator

import pytest
from insight_stand import OTHER_TENANT_FIXTURE, ApiClient, PersonaSession, analytics_path
from insight_stand.api import JsonValue

from .. import scratch
from ..schemas import (
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


@pytest.mark.parametrize("method", ["PUT", "DELETE"])
def test_metric_threshold_write_to_an_unknown_id_is_404(
    api: ApiClient, scratch_metric: Metric, method: str
) -> None:
    """A real metric, a threshold id nobody holds — 404, not 500 and not 204.

    The only thing #1663 leaves reachable on these two operations. Its create
    500s, so no threshold exists to update or delete, and without this the
    routes are touched by nothing but the anonymous sweep — proving they refuse
    a stranger while saying nothing about whether they work.

    The metric in the path is real on purpose: a 404 for a metric that does not
    exist either would pass while telling us only that SOMETHING was missing.
    """
    path = analytics_path(f"/v1/metrics/{scratch_metric.id}/thresholds/{scratch.UNKNOWN_ID}")
    response = api.request(method, path, json_body={"value": 1} if method == "PUT" else None)

    assert response.status_code == 404, (
        f"{method} on a threshold that does not exist under a metric that does "
        f"answered {response.status_code}: {response.text[:300]}"
    )


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


@pytest.mark.requires_seed(OTHER_TENANT_FIXTURE)
def test_admin_threshold_403_across_tenants(
    api: ApiClient, other_tenant_session: PersonaSession
) -> None:
    """A row belonging to one tenant is refused to a caller from another.

    The only authorization property a single-tenant stand cannot show: with one
    tenant nobody should ever be refused, so a service that ignored `tenant_id`
    entirely would pass every other test in this file. `deploy/seed` therefore
    provisions a second tenant holding one person, whose whole purpose is to be
    this caller.

    Refused as a real login, not a forged token — the same Keycloak flow every
    other persona uses, differing only in the tenant claim the realm carries for
    that user. Update AND delete, because they are separate handlers and a gate
    applied to one is not evidence about the other.
    """
    metric_id = _catalog_metric_id(api)
    scoped = {"metric_id": metric_id, "scope": "tenant"}
    for row in api.get(ADMIN_THRESHOLDS, params=scoped).parse(AdminMetricThresholdList).items:
        api.delete(f"{ADMIN_THRESHOLDS}/{row.id}")

    created = api.post(ADMIN_THRESHOLDS, json_body={**scoped, "good": 0.0, "warn": 0.0})
    assert created.status_code == 201, f"create: {created.status_code} {created.text[:300]}"
    threshold_id = scratch.track(ADMIN_THRESHOLDS, "id", created.parse(AdminMetricThresholdView).id)

    intruder = other_tenant_session.client
    update = intruder.put(f"{ADMIN_THRESHOLDS}/{threshold_id}", json_body={"good": 9.0, "warn": 9.0})
    assert update.status_code == 403, (
        f"a caller from another tenant updated the row (status {update.status_code}): "
        f"{update.text[:300]}"
    )
    delete = intruder.delete(f"{ADMIN_THRESHOLDS}/{threshold_id}")
    assert delete.status_code == 403, (
        f"a caller from another tenant deleted the row (status {delete.status_code}): "
        f"{delete.text[:300]}"
    )

    # The owner still has it, unchanged — proof the refusals refused rather than
    # 403-ing after the fact.
    survives = api.get(f"{ADMIN_THRESHOLDS}/{threshold_id}")
    assert survives.status_code == 200, f"read back: {survives.status_code} {survives.text[:300]}"
    assert survives.parse(AdminMetricThresholdView).good == 0.0

    assert api.delete(f"{ADMIN_THRESHOLDS}/{threshold_id}").status_code == 204


def _clean_tenant_scope(api: ApiClient, metric_id: str) -> None:
    """Remove any tenant-scope row on this metric.

    Not optional, and not a tidiness habit: `(metric, tenant, scope)` is UNIQUE
    and a stand keeps its database between runs, so a row an earlier run left
    behind turns every later create into a 409 and the failure looks like the
    endpoint rather than the leftover.
    """
    existing = api.get(ADMIN_THRESHOLDS, params={"metric_id": metric_id, "scope": "tenant"})
    for row in existing.parse(AdminMetricThresholdList).items:
        api.delete(f"{ADMIN_THRESHOLDS}/{row.id}")


@pytest.fixture
def admin_threshold_row(api: ApiClient) -> Iterator[AdminMetricThresholdView]:
    """One tenant-scope row on a real catalogue metric, removed afterwards."""
    metric_id = _catalog_metric_id(api)
    _clean_tenant_scope(api, metric_id)

    created = api.post(
        ADMIN_THRESHOLDS,
        json_body={"metric_id": metric_id, "scope": "tenant", "good": 0.0, "warn": 0.0},
    )
    assert created.status_code == 201, f"setup: {created.status_code} {created.text[:300]}"
    row = created.parse(AdminMetricThresholdView)
    yield row
    api.delete(f"{ADMIN_THRESHOLDS}/{row.id}")


def test_admin_threshold_create_400_unknown_metric(api: ApiClient) -> None:
    """`metric_id` is checked against the catalogue before anything is written.

    A well-formed uuid that names no enabled metric is a caller error, not a
    dangling row: without the pre-write check the table accumulates thresholds
    for metrics that do not exist, and nothing later would notice.
    """
    response = api.post(
        ADMIN_THRESHOLDS,
        json_body={
            "metric_id": scratch.UNKNOWN_ID,
            "scope": "tenant",
            "good": 0.0,
            "warn": 0.0,
        },
    )
    assert response.status_code == 400, (
        f"a threshold for a metric that does not exist answered {response.status_code}: "
        f"{response.text[:300]}"
    )


@pytest.mark.xfail(
    reason="#1664: a duplicate (metric, tenant, scope) create 500s instead of 409",
    strict=True,
)
def test_admin_threshold_create_409_duplicate(
    api: ApiClient, admin_threshold_row: AdminMetricThresholdView
) -> None:
    """A second row for the same target is a conflict the caller can act on.

    `uq_metric_threshold_scope_target` is violated, which today falls through to
    the internal-500 schema-drift alarm. Pinned strict so the fix announces
    itself: a routine client conflict reported as a server fault tells whoever
    is holding it that the product broke.
    """
    response = api.post(
        ADMIN_THRESHOLDS,
        json_body={
            "metric_id": str(admin_threshold_row.metric_id),
            "scope": "tenant",
            "good": 0.0,
            "warn": 0.0,
        },
    )
    assert response.status_code == 409, f"status={response.status_code} {response.text[:300]}"


@pytest.mark.parametrize("method", ["PUT", "DELETE"])
def test_admin_threshold_write_to_an_unknown_id_is_404(api: ApiClient, method: str) -> None:
    body: JsonValue = {"good": 1.0, "warn": 1.0} if method == "PUT" else None
    response = api.request(method, f"{ADMIN_THRESHOLDS}/{scratch.UNKNOWN_ID}", json_body=body)
    assert response.status_code == 404, (
        f"{method} on a threshold nobody holds answered {response.status_code}: "
        f"{response.text[:300]}"
    )


def test_a_tenant_id_in_the_query_string_is_refused(api: ApiClient) -> None:
    """The listing will not take a tenant from the caller.

    `ListFilters` denies unknown fields, so `tenant_id` is a 400 rather than a
    filter — the difference between a parameter the service ignores and one it
    honours is every other tenant's thresholds. The tenant comes from the
    session and has no request-side spelling at all.
    """
    response = api.get(ADMIN_THRESHOLDS, params={"tenant_id": scratch.UNKNOWN_ID})
    assert response.status_code == 400, (
        f"a caller-supplied tenant_id was accepted ({response.status_code}) rather than "
        f"refused as an unknown filter: {response.text[:300]}"
    )


def test_a_broader_locked_scope_refuses_a_narrower_create(api: ApiClient) -> None:
    """403 `threshold_locked` — the second of the two 403s analytics can produce.

    A locked tenant-scope row shadows narrower scopes during resolution, so a
    role-scope create for the same metric is refused. Worth having beyond the
    cross-tenant case because it is the only 403 here that is about the STATE of
    a row rather than about who is asking, and the two are answered by different
    code (`lock_enforcer` vs the row-ownership check).
    """
    metric_id = _catalog_metric_id(api)
    _clean_tenant_scope(api, metric_id)

    locked = api.post(
        ADMIN_THRESHOLDS,
        json_body={
            "metric_id": metric_id,
            "scope": "tenant",
            "good": 0.0,
            "warn": 0.0,
            "is_locked": True,
            "lock_reason": "stand lock-enforcer contract",
        },
    )
    assert locked.status_code == 201, f"lock setup: {locked.status_code} {locked.text[:300]}"
    locked_id = locked.parse(AdminMetricThresholdView).id

    try:
        narrower = api.post(
            ADMIN_THRESHOLDS,
            json_body={
                "metric_id": metric_id,
                "scope": "role",
                "role_slug": "stand-analyst",
                "good": 0.0,
                "warn": 0.0,
            },
        )
        assert narrower.status_code == 403, (
            f"a role-scope create under a locked tenant scope answered "
            f"{narrower.status_code}: {narrower.text[:300]}"
        )
    finally:
        # Deleting a locked row is permitted — it is a lock_cleared transition.
        api.delete(f"{ADMIN_THRESHOLDS}/{locked_id}")


@pytest.mark.parametrize(
    ("method", "suffix", "body"),
    [
        ("GET", "", None),
        ("POST", "", {"field_name": "one", "operator": "ge", "value": 1.0, "level": "good"}),
    ],
    ids=["list", "create"],
)
def test_per_metric_thresholds_of_an_unknown_metric_are_404(
    api: ApiClient, method: str, suffix: str, body: JsonValue
) -> None:
    """The METRIC is resolved before anything else on this sub-resource.

    `/v1/metrics/{id}/thresholds` is nested, so a request names two things and
    only one of them exists here. Answering 404 for the parent — before the
    body is validated on the create, per `find_enabled_metric` — is what keeps
    the sub-resource from appearing to exist under a metric that does not.

    A 200 with an empty list would be the plausible wrong answer for the list
    case: indistinguishable from a real metric that has no thresholds yet,
    which `test_metric_thresholds_listing_is_200` shows is a normal state.
    """
    path = analytics_path(f"/v1/metrics/{scratch.UNKNOWN_ID}/thresholds{suffix}")
    response = api.request(method, path, json_body=body)

    assert response.status_code == 404, (
        f"{method} thresholds of a metric that does not exist answered "
        f"{response.status_code}: {response.text[:300]}"
    )
