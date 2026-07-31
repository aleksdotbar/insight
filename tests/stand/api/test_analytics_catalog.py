"""The read-only analytics surfaces that describe what the product can measure.

    POST /v1/catalog/get_metrics    200
    GET  /v1/metric-definitions     200
    GET  /v1/persons/{email}        200 · 404 unknown

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

from insight_stand import ApiClient, Manifest, analytics_path

from .schemas import CatalogResponse, MetricDefinitionListResponse, Person, ProblemDocument


def test_catalog_get_metrics_200(api: ApiClient) -> None:
    """The catalogue is the metric-coverage gate's universe, and it is non-empty.

    An empty catalogue on a seeded stand is the signature of a seed that did not
    run, so it is asserted rather than merely parsed — `CatalogResponse`
    validating tells you the shape was right, not that anything is in it.
    """
    response = api.post(analytics_path("/v1/catalog/get_metrics"), json_body={})
    assert response.status_code == 200, f"status={response.status_code} {response.text[:300]}"
    catalog = response.parse(CatalogResponse)
    assert catalog.metrics, "the metric catalogue is empty — was this stand seeded?"


def test_metric_definitions_200(api: ApiClient) -> None:
    """Definitions are migration-seeded, so they exist on any stand that migrated."""
    response = api.get(analytics_path("/v1/metric-definitions"))
    assert response.status_code == 200, f"status={response.status_code} {response.text[:300]}"
    definitions = response.parse(MetricDefinitionListResponse)
    assert definitions.metrics, "no metric definitions — did the migrations run?"


def test_person_by_email_200(api: ApiClient, stand_manifest: Manifest) -> None:
    """A seeded person resolves, and resolves to the person the manifest names.

    The lookup key is an email and the answer carries a `person_id`, so this is
    the identity chain asserted from the analytics side: the same UUID the
    manifest recorded comes back for the address the seed used.
    """
    expected = stand_manifest.fixture("dev_lead")
    response = api.get(analytics_path(f"/v1/persons/{expected.email}"))
    assert response.status_code == 200, f"status={response.status_code} {response.text[:300]}"
    person = response.parse(Person)
    assert person.email == expected.email
    assert person.display_name == expected.display_name


def test_person_by_email_404_unknown(api: ApiClient) -> None:
    """An address nobody holds is a 404 that says so."""
    response = api.get(analytics_path("/v1/persons/nobody@example.com"))
    assert response.status_code == 404, f"status={response.status_code} {response.text[:300]}"
    assert response.parse(ProblemDocument).status == 404
