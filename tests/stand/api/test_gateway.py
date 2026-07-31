"""The edge: every operation is refused without a session.

This is the assertion the in-process rig cannot make. It runs the services with
auth disabled, so 401 and 403 are permanently unreachable there and sit in its
BLOCKED list. Here they are the point — the runtime proof that a deployed stand
requires a real login, and that `authDisabled: true` was never switched on.

Swept over the whole catalog rather than written per service, because the
property is the gateway's and it is uniform: it terminates the session at the
edge and refuses before it routes anything. One operation added to
`operations.py` is one more url proven closed, with nothing else to remember.

The refusal only MEANS something because the same urls are shown to serve real
data elsewhere in this directory — the gateway answers 401 for paths that do
not exist too.
"""

from __future__ import annotations

import pytest
from insight_stand import ApiClient

from .operations import ALL_OPERATIONS, Operation

#: Bodies are irrelevant: the edge rejects before any handler reads one, and
#: sending one would only obscure which layer answered.
_METHODS_WITHOUT_BODY = frozenset({"GET", "DELETE"})


@pytest.mark.parametrize("operation", ALL_OPERATIONS, ids=lambda op: op.label)
def test_operation_is_refused_without_a_session(
    api_client: ApiClient, operation: Operation
) -> None:
    response = api_client.request(
        operation.method,
        operation.path,
        json_body=None if operation.method in _METHODS_WITHOUT_BODY else {},
    )
    assert response.status_code == 401, (
        f"{operation.label} answered {response.status_code} to an anonymous caller, "
        f"expected 401: {response.text[:300]}"
    )


def test_the_refusal_is_a_machine_readable_problem_document(api_client: ApiClient) -> None:
    """A 401 a client cannot act on is only half a rejection.

    The body's `status` has to agree with the HTTP status and carry a title —
    that is what lets the SPA redirect to sign-in instead of guessing.

    Asserted once rather than per operation: the gateway writes the same
    document for everything it fronts, so 45 copies would restate one fact.
    """
    response = api_client.get(ALL_OPERATIONS[0].path)
    assert response.status_code == 401

    body = response.json()
    assert isinstance(body, dict), f"expected a JSON object body, got: {response.text[:400]}"
    assert body.get("status") == 401, f"problem document disagrees with the HTTP status: {body}"
    assert body.get("title"), f"problem document has no title: {body}"
