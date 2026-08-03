"""Contract: every documented operation answers 401 to an anonymous call.

The rig runs auth-ENABLED (the gears host's oidc-authn-plugin verifies the
gateway JWT), so authentication is rejected before any handler runs: no body,
no path lookup, no tenant resolution. One case per spec operation keeps the
coverage ledger honest — 401 left UNIVERSAL_BOILERPLATE when the auth-disabled
rig did (lib/api_coverage.py), so each declared 401 must now be observed.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.api

PLACEHOLDER_ID = "00000000-0000-7000-8000-000000000000"

OPERATIONS = [
    ("GET", "/v1/metric-definitions"),
    ("POST", "/v1/metric-drilldown"),
    ("POST", "/v1/metric-results"),
    ("GET", "/v1/queries"),
    ("POST", "/v1/queries"),
    ("GET", f"/v1/queries/{PLACEHOLDER_ID}"),
    ("PUT", f"/v1/queries/{PLACEHOLDER_ID}"),
    ("DELETE", f"/v1/queries/{PLACEHOLDER_ID}"),
    ("POST", f"/v1/queries/{PLACEHOLDER_ID}/run"),
]


@pytest.mark.parametrize(("method", "path"), OPERATIONS, ids=lambda v: v if isinstance(v, str) else None)
def test_anonymous_call_is_rejected_401(anon_api, method: str, path: str) -> None:
    r = anon_api.request(method, path)
    assert r.status_code == 401, f"{method} {path}: status={r.status_code} body={r.text}"


def test_garbage_bearer_is_rejected_401(anon_api) -> None:
    r = anon_api.get("/v1/metric-definitions", headers={"Authorization": "Bearer not-a-jwt"})
    assert r.status_code == 401, f"status={r.status_code} body={r.text}"
