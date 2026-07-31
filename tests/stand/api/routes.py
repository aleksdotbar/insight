"""The gateway paths the API tests address, named once.

The unauthenticated and authenticated suites assert opposite outcomes on the
SAME urls. Splitting those assertions into separate tests — one per status
code — is clearer to read and to report on, but it introduces a risk the
combined form did not have: the two halves quietly drifting onto different
routes and no longer proving anything about each other. Naming each path here,
and importing it on both sides, is what removes that risk.

Every path below must be one the stand genuinely serves. A 401 on a route that
does not exist proves nothing — the gateway rejects at the edge, before it
would have discovered there is nothing behind it — so each entry is paired with
an authenticated test that gets real data back.

Which service answers, per `deploy/compose/gateway/routes.yaml`:

    /api/analytics  ->  analytics:8081             (Rust)
    /api/identity   ->  identity-resolution:8082   (Rust)

Both are Rust services. The .NET `insight-identity` that used to serve
`/api/identity` was removed upstream in favour of the `identity-resolution`
port (epic #1602), and the gateway was repointed at it.
"""

from __future__ import annotations

from insight_stand import analytics_path, identity_path

#: Analytics metric catalog. 401 anonymous, 200 with a session.
METRICS = analytics_path("/v1/metrics")

#: Caller-derived org subchart — takes no person argument, so what comes back
#: identifies whoever the session belongs to. 401 anonymous, 200 with a
#: session, and populated from the seeded org chart.
SUBCHART = identity_path("/v1/subchart")

# NOT here, deliberately: `/v1/persons/{email}`. The committed contract at
# docs/components/backend/identity-resolution/openapi.json still declares it,
# but the Rust service does not serve it — it answers 404 even with a valid
# session, and only `/internal/persons/by-email/{email}` (service principals
# only) survives the port. Asserting a 401 on it would be asserting the
# gateway's edge behaviour against a route that is not there.
