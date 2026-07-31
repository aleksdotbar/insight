"""The gateway paths the API tests address, named once.

The unauthenticated and authenticated suites assert opposite outcomes on the
SAME urls. Splitting those assertions into separate tests — one per status
code — is clearer to read and to report on, but it introduces a risk the
combined form did not have: the two halves quietly drifting onto different
routes and no longer proving anything about each other. Naming each path here,
and importing it on both sides, is what removes that risk.

Which service answers, per `deploy/compose/gateway/routes.yaml`:

    /api/analytics  ->  analytics:8081   (Rust)
    /api/identity   ->  identity:8082    (.NET `insight-identity`)

`/v1/subchart` in particular is served by the **.NET** identity service today.
A Rust port of the same surface exists as `identity-resolution` (epic #1602)
and runs side by side, but nothing routes to it — the gateway table has no
entry for it, so it is reachable only on its own host port. These tests assert
through the gateway and never name a service, so they stay correct across that
cutover: the day `/api/identity` is repointed, the same assertions describe the
new implementation.
"""

from __future__ import annotations

from insight_stand import analytics_path, identity_path

#: Analytics metric catalog. 401 anonymous, 200 with a session.
METRICS = analytics_path("/v1/metrics")

#: Identity person collection. 401 anonymous; identity exposes no authenticated
#: collection route, so there is no 200 counterpart.
PERSONS = identity_path("/v1/persons")

#: Caller-derived org subchart — takes no person argument, so what comes back
#: identifies whoever the session belongs to.
SUBCHART = identity_path("/v1/subchart")


def person(email: str) -> str:
    """One person's record. 401 anonymous, 200 with a session.

    Keyed by EMAIL: `/v1/persons/{uuid}` answers 404 even with a valid session.
    """
    return identity_path(f"/v1/persons/{email}")
