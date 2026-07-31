"""401 — the stand refuses unauthenticated API traffic.

The deployed-path counterpart to the in-process rig's auth tests: a REAL
gateway, in front of REAL services, with no session attached. This is also the
runtime enforcement that `authDisabled: true` was never switched on.

Both paths asserted here are ones the stand genuinely serves —
`test_authenticated.py` gets real data back from each with a session, from the
same `routes` constants. That pairing is what makes a 401 mean "refused"
rather than "there was nothing there anyway".
"""

from __future__ import annotations

from insight_stand import ApiClient

from .routes import METRICS, SUBCHART


def test_analytics_metrics_is_401_without_a_session(api_client: ApiClient) -> None:
    response = api_client.get(METRICS)
    assert response.status_code == 401, (
        f"expected 401 from {response.url}, got {response.status_code}: {response.text[:400]}"
    )


def test_identity_subchart_is_401_without_a_session(api_client: ApiClient) -> None:
    response = api_client.get(SUBCHART)
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
