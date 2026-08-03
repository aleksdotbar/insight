"""The read-only analytics surfaces that describe what the product can measure.

    POST /v1/catalog/get_metrics    200
    GET  /v1/metric-definitions     200 · tenant label wins over the default
    GET  /v1/persons/{email}        200 · 400 undecodable · 404 unknown

Grouped because they answer the same kind of question — "what does this stand
know about?" — and none of them writes anything, so none needs a scratch
fixture.

`/v1/persons/{email}` lives here, on ANALYTICS, which is worth stating plainly:
the identity contract still declares it and `identity-resolution` answers 404.
The capability moved services during the .NET removal and the document did not
follow.

The 401 half is in `test_gateway.py`, swept over every operation at once.
"""

from __future__ import annotations

import pytest
from insight_stand import ApiClient, Manifest, analytics_path

from .. import scratch
from ..schemas import (
    EXTRACTOR_REJECTION_CONTENT_TYPE,
    CatalogResponse,
    MetricDefinitionListResponse,
    Person,
    ProblemDocument,
)

METRIC_DEFINITIONS = analytics_path("/v1/metric-definitions")
CATALOG_GET_METRICS = analytics_path("/v1/catalog/get_metrics")


def _definitions(api: ApiClient) -> MetricDefinitionListResponse:
    response = api.get(METRIC_DEFINITIONS)
    assert response.status_code == 200, f"status={response.status_code} {response.text[:300]}"
    return response.parse(MetricDefinitionListResponse)


def test_catalog_get_metrics_200(api: ApiClient) -> None:
    """The catalogue is the metric-coverage gate's universe, and it is non-empty.

    An empty catalogue on a seeded stand is the signature of a seed that did not
    run, so it is asserted rather than merely parsed — `CatalogResponse`
    validating tells you the shape was right, not that anything is in it.
    """
    response = api.post(CATALOG_GET_METRICS, json_body={})
    assert response.status_code == 200, f"status={response.status_code} {response.text[:300]}"
    catalog = response.parse(CatalogResponse)
    assert catalog.metrics, "the metric catalogue is empty — was this stand seeded?"


def test_metric_definitions_200(api: ApiClient) -> None:
    """Definitions are migration-seeded, so they exist on any stand that migrated."""
    assert _definitions(api).metrics, "no metric definitions — did the migrations run?"


@pytest.mark.requires_catalogue("definition_override")
def test_metric_definitions_resolve_the_tenant_label(
    api: ApiClient, stand_manifest: Manifest
) -> None:
    """A tenant row wins over the product default for the same metric_key.

    The whole point of tenant-scoped definitions, and invisible without one: on
    a stand where every definition is the product's, a listing that ignored
    tenant scoping entirely would look perfectly correct.

    `deploy/seed/analytics.py` writes the row (there is no endpoint for it) and
    records which key it re-labelled, so this reads the answer from the manifest
    rather than assuming the seed picked any particular metric.
    """
    override = stand_manifest.catalogue.definition_override
    assert override is not None, "the requires_catalogue marker should have skipped this"

    response = api.get(analytics_path("/v1/metric-definitions"))
    assert response.status_code == 200, f"status={response.status_code} {response.text[:300]}"

    served = {m.metric_key: m.label for m in response.parse(MetricDefinitionListResponse).metrics}
    assert override.metric_key in served, (
        f"the overridden key {override.metric_key!r} is absent from the listing: "
        f"{sorted(served)[:10]}"
    )
    assert served[override.metric_key] == override.label, (
        f"{override.metric_key} served the label {served[override.metric_key]!r}; the tenant row "
        f"says {override.label!r}. The listing is serving the product default over the tenant's."
    )


def test_person_by_id_200(api: ApiClient, stand_manifest: Manifest) -> None:
    """A seeded person resolves, and resolves to the person the manifest names.

    The path key is the canonical person UUID since the identity cutover
    (#2098), and the answer carries the EMAIL — the reverse of what this test
    asserted before. That direction is the useful one: an id read off a metric
    result resolves to a profile with no second mapping in between.
    """
    expected = stand_manifest.fixture("dev_lead")
    response = api.get(analytics_path(f"/v1/persons/{expected.uuid}"))
    assert response.status_code == 200, f"status={response.status_code} {response.text[:300]}"
    person = response.parse(Person)
    assert person.email == expected.email
    assert person.display_name == expected.display_name


def test_person_by_id_404_unknown(api: ApiClient) -> None:
    """An id nobody holds is a 404 that says so."""
    response = api.get(analytics_path(f"/v1/persons/{scratch.UNKNOWN_ID}"))
    assert response.status_code == 404, f"status={response.status_code} {response.text[:300]}"
    assert response.parse(ProblemDocument).status == 404


@pytest.mark.parametrize(
    ("label", "key"),
    [
        ("pre-cutover email", "somebody@example.com"),
        ("nil uuid", "00000000-0000-0000-0000-000000000000"),
    ],
)
def test_person_by_a_key_that_is_not_a_person_id_is_400(
    api: ApiClient, label: str, key: str
) -> None:
    """Both refused loudly, and 404 would be the wrong kind of quiet.

    An EMAIL is what this route took before the cutover, so a caller that has
    not migrated sends one in earnest; answering 404 would read as "no such
    person" and send them looking for the person rather than the mistake. The
    NIL uuid parses and is never anybody, so it must not reach the identity hop
    either.
    """
    response = api.get(analytics_path(f"/v1/persons/{key}"))
    assert response.status_code == 400, (
        f"a {label} answered {response.status_code} rather than 400: {response.text[:300]}"
    )


def test_person_by_undecodable_key_400(api: ApiClient) -> None:
    """`%FF` is not valid UTF-8, so `{email}` never reaches the handler.

    A different refusal from the 404 above, and the distinction is the point: an
    address nobody holds is a well-formed question with no answer, while this one
    is not a question the route can read. They arrive differently too — the 404 is
    a problem document, this is the extractor's plain text.
    """
    response = api.get(analytics_path("/v1/persons/%FF"))
    assert response.status_code == 400, f"status={response.status_code} {response.text[:300]}"
    assert response.content_type == EXTRACTOR_REJECTION_CONTENT_TYPE, (
        f"expected the extractor's plain-text rejection, got {response.content_type!r}: "
        f"{response.text[:300]}"
    )


def test_the_definitions_listing_is_sorted_and_each_key_appears_once(api: ApiClient) -> None:
    """Ordering and uniqueness together, because the tenant override needs both.

    A tenant row SHADOWS its product default rather than joining it, so a key
    appearing twice would mean the override was added instead of collapsing —
    and a consumer reading the first match would get whichever row the database
    happened to return first.
    """
    metrics = _definitions(api).metrics
    keys = [metric.metric_key for metric in metrics]

    assert keys == sorted(keys), "the definitions listing is not sorted by metric_key"
    assert len(keys) == len(set(keys)), (
        f"a metric_key appears more than once: "
        f"{sorted({k for k in keys if keys.count(k) > 1})}"
    )


def test_the_definitions_listing_carries_no_computation_internals(api: ApiClient) -> None:
    """Consumers get the MEANING of a metric, never its implementation.

    `inputs`, `computation` and `transform` describe how a number is produced
    from which observation sources. Putting them on the wire would make the
    listing a description of the warehouse, and every consumer a party to
    changes in it.
    """
    response = api.get(METRIC_DEFINITIONS)
    body = response.json()
    assert isinstance(body, dict)
    metrics = body.get("metrics")
    assert isinstance(metrics, list) and metrics, "no definitions to inspect"

    leaked = {
        internal
        for metric in metrics
        if isinstance(metric, dict)
        for internal in ("computation", "computation_type", "inputs", "transform", "scale")
        if internal in metric
    }
    assert not leaked, f"the definitions listing exposes computation internals: {sorted(leaked)}"


def test_catalog_get_metrics_400_unknown_field(api: ApiClient) -> None:
    """The request body denies unknown fields, so a typo is refused not ignored.

    The same guard the admin listing applies to query parameters, and for the
    same reason: a field the service silently drops is one a caller believes
    took effect. `tenant_idd` is the shape that matters most — near enough to a
    real filter to be sent in earnest.
    """
    response = api.post(CATALOG_GET_METRICS, json_body={"tenant_idd": "oops"})
    assert response.status_code == 400, (
        f"an unknown request field answered {response.status_code} rather than 400: "
        f"{response.text[:300]}"
    )
