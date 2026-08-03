"""Every operation the stand serves behind the gateway, named once.

Read from the route tables in
`src/backend/services/{analytics,identity-resolution}/src/api/`, not from the
committed OpenAPI documents — the identity one is still the .NET contract and
is stale in both directions (it declares `/v1/persons/{email}`, which identity
answers 404 for and analytics actually serves; it omits both persons-sync
operations; and every operation in it lists only `200`).

Two consumers, and the reason this is one list rather than two:

* `test_gateway.py` asserts 401 for EVERY row. That is the deployed-path
  assertion the in-process rig cannot make, since auth is disabled there.
* the per-service modules assert what each operation does WITH a session.

A 401 alone proves nothing — the gateway rejects at the edge before routing, so
a path that does not exist answers 401 too. The refusal only means "refused"
when the same url is shown to serve something. Keeping the catalog here, and
having the service modules build their urls from it, is what stops the two
halves drifting onto different routes.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from insight_stand import analytics_path, identity_path


@dataclass(frozen=True)
class Operation:
    """One (method, path) the gateway routes, with its url already built."""

    method: str
    path: str
    #: `analytics` | `identity` — which service answers, per routes.yaml.
    service: str

    @property
    def label(self) -> str:
        """`GET /api/analytics/v1/metrics`, for a readable parametrize id."""
        return f"{self.method} {self.path}"


def _a(method: str, suffix: str) -> Operation:
    return Operation(method=method, path=analytics_path(suffix), service="analytics")


def _i(method: str, suffix: str) -> Operation:
    return Operation(method=method, path=identity_path(suffix), service="identity")


# Concrete stand-ins for path parameters. The 401 sweep runs before any
# authentication, so the gateway never reaches a handler and these are never
# resolved — they only have to be well-formed enough to route.
SOME_ID: Final[str] = "01900000-0000-7000-8000-000000000000"
SOME_TABLE: Final[str] = "gold_metric_values"
SOME_EMAIL: Final[str] = "nobody@example.com"

#: analytics — 29 operations.
ANALYTICS_OPERATIONS: Final[tuple[Operation, ...]] = (
    _a("GET", "/v1/metrics"),
    _a("POST", "/v1/metrics"),
    _a("GET", f"/v1/metrics/{SOME_ID}"),
    _a("PUT", f"/v1/metrics/{SOME_ID}"),
    _a("DELETE", f"/v1/metrics/{SOME_ID}"),
    _a("POST", f"/v1/metrics/{SOME_ID}/query"),
    _a("POST", "/v1/metrics/queries"),
    _a("GET", f"/v1/metrics/{SOME_ID}/thresholds"),
    _a("POST", f"/v1/metrics/{SOME_ID}/thresholds"),
    _a("PUT", f"/v1/metrics/{SOME_ID}/thresholds/{SOME_ID}"),
    _a("DELETE", f"/v1/metrics/{SOME_ID}/thresholds/{SOME_ID}"),
    _a("GET", "/v1/admin/metric-thresholds"),
    _a("POST", "/v1/admin/metric-thresholds"),
    _a("GET", f"/v1/admin/metric-thresholds/{SOME_ID}"),
    _a("PUT", f"/v1/admin/metric-thresholds/{SOME_ID}"),
    _a("DELETE", f"/v1/admin/metric-thresholds/{SOME_ID}"),
    _a("GET", "/v1/queries"),
    _a("POST", "/v1/queries"),
    _a("GET", f"/v1/queries/{SOME_ID}"),
    _a("PUT", f"/v1/queries/{SOME_ID}"),
    _a("DELETE", f"/v1/queries/{SOME_ID}"),
    _a("POST", f"/v1/queries/{SOME_ID}/run"),
    _a("POST", "/v1/catalog/get_metrics"),
    _a("GET", "/v1/columns"),
    _a("GET", f"/v1/columns/{SOME_TABLE}"),
    _a("GET", "/v1/metric-definitions"),
    _a("POST", "/v1/metric-results"),
    _a("POST", "/v1/metric-drilldown"),
    _a("GET", f"/v1/persons/{SOME_EMAIL}"),
)

#: identity-resolution — 18 operations. `/health` and `/healthz` are the host
#: router's, not the product API, and are deliberately absent: the real probes
#: address the pod directly rather than passing the gateway.
IDENTITY_OPERATIONS: Final[tuple[Operation, ...]] = (
    _i("POST", "/v1/profiles"),
    _i("GET", "/v1/subchart"),
    _i("GET", f"/v1/subchart/{SOME_ID}"),
    _i("GET", "/v1/persons-seed"),
    _i("GET", f"/v1/persons-seed/{SOME_ID}"),
    _i("GET", "/v1/persons-sync"),
    _i("GET", f"/v1/persons-sync/{SOME_ID}"),
    _i("GET", "/v1/roles"),
    _i("POST", "/v1/roles"),
    _i("DELETE", f"/v1/roles/{SOME_ID}"),
    _i("GET", "/v1/person-roles"),
    _i("POST", "/v1/person-roles"),
    _i("DELETE", f"/v1/person-roles/{SOME_ID}"),
    _i("GET", "/v1/visibility"),
    _i("POST", "/v1/visibility"),
    _i("DELETE", f"/v1/visibility/{SOME_ID}"),
    # `.authenticated()`, not admin-gated — and the substring test below does not
    # catch it, which is correct: `/visible-persons` is not `/visibility`.
    _i("POST", "/v1/visible-persons"),
    _i("GET", f"/internal/persons/by-email/{SOME_EMAIL}"),
)

ALL_OPERATIONS: Final[tuple[Operation, ...]] = ANALYTICS_OPERATIONS + IDENTITY_OPERATIONS

#: The 13 identity operations behind `require_admin`, which resolves the caller
#: from the gateway JWT and requires an active `admin` row in `person_roles` —
#: it never reads the `insight-admin` REALM role. The seed grants nobody that
#: row, so every persona is refused; see out/endpoint-coverage-preconditions.md.
ADMIN_GATED: Final[frozenset[str]] = frozenset(
    op.label
    for op in IDENTITY_OPERATIONS
    if any(
        seg in op.path
        for seg in ("/persons-seed", "/persons-sync", "/roles", "/person-roles", "/visibility")
    )
)
