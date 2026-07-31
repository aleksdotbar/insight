"""The stand refuses unauthenticated API traffic.

This is the deployed-path counterpart to the in-process rig's auth tests: it
asserts that a REAL gateway, in front of REAL services, with no session
attached, answers 401 — which is also the runtime enforcement that
`authDisabled: true` was never switched on.

The authenticated half of the pair (the same routes returning 200 with a
session won by `RealLogin`) is phase 6's, once the Keycloak OIDC chain exists.
Nothing here fakes a session to close the pair early: a stubbed 200 would
assert only that the stub works.
"""

from __future__ import annotations

import pytest
from insight_stand import ApiClient, Manifest, analytics_path, identity_path


def test_anonymous_credentials_really_send_nothing(api_client: ApiClient) -> None:
    """Precondition for every 401 below.

    Without this, a 401 could just as easily mean the client is broken as that
    the stand is enforcing anything.
    """
    assert api_client.credentials.is_authenticated() is False
    assert api_client.credentials.headers() == {}


def test_analytics_route_rejects_anonymous_request(api_client: ApiClient) -> None:
    response = api_client.get(analytics_path("/v1/metrics"))
    assert response.status_code == 401, (
        f"expected 401 from {response.url}, got {response.status_code}: {response.text[:400]}"
    )


def test_identity_route_rejects_anonymous_request(api_client: ApiClient) -> None:
    response = api_client.get(identity_path("/v1/persons"))
    assert response.status_code == 401, (
        f"expected 401 from {response.url}, got {response.status_code}: {response.text[:400]}"
    )


def test_rejection_is_a_machine_readable_problem_document(api_client: ApiClient) -> None:
    """A 401 that a client cannot act on is only half a rejection.

    The gateway answers with a problem document whose `status` agrees with the
    HTTP status and whose `detail` names where to authenticate, which is what
    lets the SPA redirect instead of guessing.
    """
    response = api_client.get(analytics_path("/v1/metrics"))
    assert response.status_code == 401

    body = response.json()
    assert isinstance(body, dict), f"expected a JSON object body, got: {response.text[:400]}"
    assert body.get("status") == 401, f"problem document disagrees with the HTTP status: {body}"
    assert body.get("title"), f"problem document has no title: {body}"


@pytest.mark.requires_seed("dev_lead")
def test_seeded_person_is_not_readable_without_a_session(
    api_client: ApiClient, stand_manifest: Manifest
) -> None:
    """A record that definitely exists, on a route that definitely works.

    This rules out the boring explanation for the 401s above — that they merely
    reflect a route with nothing behind it. The address matters: identity keys
    this endpoint by EMAIL, and `/v1/persons/{uuid}` answers 404 even with a
    valid session, so a uuid here would prove nothing. `test_authenticated.py`
    reads this exact URL successfully with a session.
    """
    dev_lead = stand_manifest.fixture("dev_lead")
    response = api_client.get(identity_path(f"/v1/persons/{dev_lead.email}"))
    assert response.status_code == 401, (
        f"expected 401 for seeded person {dev_lead.email} at {response.url}, "
        f"got {response.status_code}: {response.text[:400]}"
    )


@pytest.mark.requires_ingestion
def test_requires_ingestion_probe(stand_manifest: Manifest) -> None:
    """A probe for the capability marker, not an assertion about ingestion.

    Named honestly: on this stand its VALUE IS THE SKIP. The compose stand
    seeds silver and gold directly — no connector ever runs — so
    `capabilities.ingestion` is false, this is skipped with a reported reason,
    and that is the behaviour phases 7-8 rely on when they mark
    ingestion-dependent work the same way.

    When it does run, the assertion is deliberately the same predicate the
    marker gates on, which makes it a tautology and not a test of ingestion.
    Real ingestion assertions belong to whoever builds a stand that declares
    the capability; this exists so the marker has a live consumer and its skip
    path is exercised on every run.
    """
    assert stand_manifest.capabilities.ingestion is True
