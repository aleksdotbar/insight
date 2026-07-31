"""`POST /v1/metric-results` — the endpoint the dashboard actually calls.

    POST /v1/metric-results   200 · 422 off-schema

The widest single request in the API: an entity, a period and a list of metrics
each with its own views. Worth a deployed-path test more than most, because it
is the one request whose failure a user would see directly, and because its
result depends on the whole chain — the session's tenant reaching ClickHouse,
gold views having been built, and the seeded window overlapping the period asked
for.

Asserted against the metric DEFINITIONS rather than a hardcoded key, so a
catalogue change moves the test without editing it.

The 401 half is in `test_gateway.py`, swept over every operation at once.
"""

from __future__ import annotations

from insight_stand import ApiClient, Manifest, analytics_path

from .schemas import MetricDefinitionListResponse, MetricResultsResponse

METRIC_RESULTS = analytics_path("/v1/metric-results")


def _a_metric_key(api: ApiClient) -> str:
    response = api.get(analytics_path("/v1/metric-definitions"))
    assert response.status_code == 200, f"definitions: {response.status_code}"
    metrics = response.parse(MetricDefinitionListResponse).metrics
    assert metrics, "no metric definitions — did the migrations run?"
    return metrics[0].metric_key


def test_metric_results_200(api: ApiClient, stand_manifest: Manifest) -> None:
    """One person, the seeded data window, one metric, the period view.

    The period comes from the manifest's own `data_window`, so the request asks
    for the range the stand was actually seeded over rather than a guess that
    would return an empty result and still pass.
    """
    person = stand_manifest.fixture("dev_lead")
    start, _, end = stand_manifest.data_window.partition("..")
    metric_key = _a_metric_key(api)

    response = api.post(
        METRIC_RESULTS,
        json_body={
            "entity": {"type": "person", "ids": [person.uuid]},
            "period": {"from": start, "to": end},
            "metrics": [{"metric_key": metric_key, "views": [{"view": "period"}]}],
        },
    )
    assert response.status_code == 200, f"status={response.status_code} {response.text[:300]}"

    # `MetricResultDto` is a RootModel over four view-shaped variants, so the
    # payload has to be unwrapped once. That the union resolved at all is half
    # the assertion: the response matched one of the shapes the contract
    # declares, rather than something the models had to be loosened to accept.
    results = response.parse(MetricResultsResponse)
    answered = [metric.root.metric_key for metric in results.metrics]
    assert answered == [metric_key], (
        f"asked for {metric_key!r} and the response answered for {answered}"
    )


def test_metric_results_422_off_schema(api: ApiClient) -> None:
    """A body that is valid JSON but not the request type.

    Axum's own extractor rejection, so it arrives as `text/plain` rather than a
    canonical problem document — see `schemas/common.EXTRACTOR_REJECTION_*`
    and #1670. Asserted as it behaves.
    """
    response = api.post(METRIC_RESULTS, json_body={"not": "a metric-results request"})
    assert response.status_code == 422, f"status={response.status_code} {response.text[:300]}"
