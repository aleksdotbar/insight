"""401 — the stand refuses unauthenticated API traffic.

The deployed-path counterpart to the in-process rig's auth tests: a REAL
gateway, in front of REAL services, with no session attached. This is also the
runtime enforcement that `authDisabled: true` was never switched on.

`test_authenticated.py` asserts the 200 half on the same paths, imported from
`routes` so the two cannot drift apart.
"""

from __future__ import annotations

import pytest
from insight_stand import ApiClient, Manifest

from .routes import METRICS, PERSONS, person


def test_analytics_metrics_is_401_without_a_session(api_client: ApiClient) -> None:
    response = api_client.get(METRICS)
    assert response.status_code == 401, (
        f"expected 401 from {response.url}, got {response.status_code}: {response.text[:400]}"
    )


def test_identity_persons_is_401_without_a_session(api_client: ApiClient) -> None:
    response = api_client.get(PERSONS)
    assert response.status_code == 401, (
        f"expected 401 from {response.url}, got {response.status_code}: {response.text[:400]}"
    )


def test_the_401_is_a_machine_readable_problem_document(api_client: ApiClient) -> None:
    """A 401 a client cannot act on is only half a rejection.

    The body's `status` has to agree with the HTTP status and carry a title —
    that is what lets the SPA redirect to sign-in instead of guessing.
    """
    response = api_client.get(METRICS)
    assert response.status_code == 401

    body = response.json()
    assert isinstance(body, dict), f"expected a JSON object body, got: {response.text[:400]}"
    assert body.get("status") == 401, f"problem document disagrees with the HTTP status: {body}"
    assert body.get("title"), f"problem document has no title: {body}"


@pytest.mark.requires_seed("dev_lead")
def test_seeded_person_is_401_without_a_session(
    api_client: ApiClient, stand_manifest: Manifest
) -> None:
    """A record that definitely exists, on a route that definitely works.

    This rules out the boring explanation for the 401s above — that they merely
    reflect a route with nothing behind it. `test_authenticated.py` reads this
    exact URL successfully with a session.
    """
    dev_lead = stand_manifest.fixture("dev_lead")
    response = api_client.get(person(dev_lead.email))
    assert response.status_code == 401, (
        f"expected 401 for seeded person {dev_lead.email} at {response.url}, "
        f"got {response.status_code}: {response.text[:400]}"
    )
