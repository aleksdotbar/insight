"""Contract: POST /v1/metric-results — the unified-metric compute endpoint.

  POST /v1/metric-results   200 compute · 400 (empty/bad-period/unknown-key) · 403 not-visible · 415 wrong-ct

Added when the `feat/unified-metrics` merge (#1656) introduced this operation to
the committed spec. It computes builtin metrics over unified observation models.

The endpoint validates its request body in `domain::metric_results::validate_request`
BEFORE touching ClickHouse, so the whole 400 family is deterministic in the rig:
an empty `metrics`, a malformed/reversed `period`, and an unknown `metric_key`
(which is a 400 via `unavailable`, NOT a 404) all reject up front. Wrong
Content-Type is a 415 at the `axum::Json` extractor.

The declarative metric suite exercises deterministic 200 responses for every
builtin metric; this module retains the endpoint contract error cases.
"""

from __future__ import annotations

import pytest
from lib.identity_stub import UNKNOWN_EMAIL, VISIBLE_EMAILS, person_id_for

from api.endpoint_helpers import text_body_request

pytestmark = pytest.mark.api

_BUILTIN_METRIC = [{"metric_key": "ai.active_days", "views": [{"view": "period"}]}]


def _request(*, metrics, entity_ids=None, period=("2026-01-01", "2026-01-31")):
    """A well-formed metric-results body with overridable parts.

    entity ids are person UUIDs since the identity cutover; the default is a
    visible persona's UUID with no seeded data, so validation and the
    visibility gate pass and compute paths answer with honest emptiness."""
    if entity_ids is None:
        entity_ids = (person_id_for(VISIBLE_EMAILS[1]),)
    return {
        "entity": {"type": "person", "ids": list(entity_ids)},
        "period": {"from": period[0], "to": period[1]},
        "metrics": metrics,
    }


def test_metric_results_400_non_uuid_person_ids(api) -> None:
    """entity.ids must be person UUIDs since the identity cutover; the
    pre-cutover email shape is a loud 400, never a silent empty result."""
    body = _request(metrics=[{"metric_key": "git.commits", "views": [{"view": "period"}]}],
                    entity_ids=("somebody@example.com",))
    r = api.post("/v1/metric-results", json=body)
    assert r.status_code == 400, f"status={r.status_code} body={r.text}"


def test_metric_results_400_empty_metrics(api) -> None:
    """`metrics` must not be empty — rejected by validate_request_shape before
    any ClickHouse access (deterministic, no seeded data required)."""
    r = api.post("/v1/metric-results", json=_request(metrics=[]))
    assert r.status_code == 400, f"status={r.status_code} body={r.text}"


def test_metric_results_400_bad_period_date(api) -> None:
    """`period.from` is parsed as `%Y-%m-%d`; a non-date is a canonical 400."""
    body = _request(metrics=[{"metric_key": "ai.x", "views": [{"view": "period"}]}])
    body["period"]["from"] = "not-a-date"
    r = api.post("/v1/metric-results", json=body)
    assert r.status_code == 400, f"status={r.status_code} body={r.text}"


def test_metric_results_400_reversed_period(api) -> None:
    """`period.from` after `period.to` is a 400 (before any bucket enumeration)."""
    body = _request(
        metrics=[{"metric_key": "ai.x", "views": [{"view": "period"}]}],
        period=("2026-02-01", "2026-01-01"),
    )
    r = api.post("/v1/metric-results", json=body)
    assert r.status_code == 400, f"status={r.status_code} body={r.text}"


def test_metric_results_400_unknown_metric_key(api) -> None:
    """An unknown `metric_key` is resolved against the catalog and rejected as a
    400 (`unavailable`) — NOT a 404. Pins that the compute endpoint has no
    not-found path (the spec's declared 404 is `.standard_errors` boilerplate)."""
    body = _request(
        metrics=[{"metric_key": "e2e.definitely-not-a-real-metric", "views": [{"view": "period"}]}],
    )
    r = api.post("/v1/metric-results", json=body)
    assert r.status_code == 400, f"status={r.status_code} body={r.text}"


def test_metric_results_403_person_outside_the_callers_visible_set(api) -> None:
    """A person the caller cannot see is refused before any ClickHouse access:
    the gate flattens the caller's subchart and the requested id is not in it."""
    body = _request(metrics=_BUILTIN_METRIC, entity_ids=(person_id_for(UNKNOWN_EMAIL),))
    r = api.post("/v1/metric-results", json=body)
    assert r.status_code == 403, f"status={r.status_code} body={r.text}"


def test_metric_results_403_rejects_the_whole_request_on_one_hidden_person(api) -> None:
    """Mixing a visible person with a hidden one refuses the request as a whole,
    rather than silently dropping the unauthorized entity from the response."""
    body = _request(
        metrics=_BUILTIN_METRIC,
        entity_ids=(person_id_for(VISIBLE_EMAILS[1]), person_id_for(UNKNOWN_EMAIL)),
    )
    r = api.post("/v1/metric-results", json=body)
    assert r.status_code == 403, f"status={r.status_code} body={r.text}"


def test_metric_results_415_wrong_content_type(api) -> None:
    """The body binds `Json<MetricResultsRequest>`; a text/plain body is a 415
    at the extractor, before the handler runs."""
    r = text_body_request(api, "POST", "/v1/metric-results")
    assert r.status_code == 415, f"status={r.status_code} body={r.text}"
