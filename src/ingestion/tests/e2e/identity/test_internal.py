"""Contract: the two SERVICE-ONLY any-tenant internal person-resolve lookups.
Kept as SEPARATE routes/contracts by design (never a shared `value_type`
dispatch) so the login-bootstrap and the admin `__override` view-as feature
can never be confused for one another:

- `GET /internal/persons/by-external-id?source_type=...&external_id=...` —
  the login-bootstrap resolve, scoped to the IdP source type + its
  source-native external id (e.g. the Entra `oid` claim).
- `GET /internal/persons/by-email-override?email=...` — the authenticator's
  admin `__override` (view-as) resolve; never used by login.

Restricted to SERVICE principals (JWT sub_type=service); the tenant is
deliberately ignored (at login/override neither is yet known)."""

from __future__ import annotations

import pytest

from lib import identity_seed as seed

pytestmark = pytest.mark.identity


def test_by_email_override_200_service_token(service_api) -> None:
    """The resolved person comes back as a source-descriptor quadruple
    (insight_source_type='person', insight_source_id=<person uuid>) — the
    shape the authenticator's IdentityPersonResolver consumes."""
    r = service_api.get(
        "/internal/persons/by-email-override", params={"email": seed.ALICE_EMAIL}
    )
    assert r.status_code == 200, f"status={r.status_code} body={r.text}"
    body = r.json()
    assert body["insight_source_type"] == "person", body
    assert body["insight_source_id"] == str(seed.ALICE), body
    assert body["value"] == seed.ALICE_EMAIL, body
    assert body["value_type"] == "email", body


def test_by_email_override_404_unknown(service_api) -> None:
    r = service_api.get(
        "/internal/persons/by-email-override", params={"email": seed.UNKNOWN_EMAIL}
    )
    assert r.status_code == 404, f"status={r.status_code} body={r.text}"


def test_by_email_override_400_missing_email(service_api) -> None:
    r = service_api.get("/internal/persons/by-email-override")
    assert r.status_code == 400, f"status={r.status_code} body={r.text}"


def test_by_email_override_403_user_token(api) -> None:
    """A regular user principal must not reach the S2S surface."""
    r = api.get("/internal/persons/by-email-override", params={"email": seed.ALICE_EMAIL})
    assert r.status_code == 403, f"status={r.status_code} body={r.text}"


def test_by_email_override_401_unauthenticated(anon_api) -> None:
    r = anon_api.get(
        "/internal/persons/by-email-override", params={"email": seed.ALICE_EMAIL}
    )
    assert r.status_code == 401


def test_by_external_id_200_service_token(service_api) -> None:
    """The login-bootstrap mode: resolve by the IdP source type + the fixture's
    seeded `value_type='id'` observation (a source-native account id)."""
    r = service_api.get(
        "/internal/persons/by-external-id",
        params={"source_type": seed.SOURCE_TYPE, "external_id": seed.ALICE_ACCOUNT_ID},
    )
    assert r.status_code == 200, f"status={r.status_code} body={r.text}"
    body = r.json()
    assert body["insight_source_type"] == "person", body
    assert body["insight_source_id"] == str(seed.ALICE), body
    assert body["value"] == seed.ALICE_ACCOUNT_ID, body
    assert body["value_type"] == "id", body


def test_by_external_id_404_unknown(service_api) -> None:
    r = service_api.get(
        "/internal/persons/by-external-id",
        params={"source_type": seed.SOURCE_TYPE, "external_id": "acc-nobody"},
    )
    assert r.status_code == 404, f"status={r.status_code} body={r.text}"


def test_by_external_id_400_missing_source_type(service_api) -> None:
    r = service_api.get(
        "/internal/persons/by-external-id",
        params={"external_id": seed.ALICE_ACCOUNT_ID},
    )
    assert r.status_code == 400, f"status={r.status_code} body={r.text}"


def test_by_external_id_400_missing_external_id(service_api) -> None:
    r = service_api.get(
        "/internal/persons/by-external-id",
        params={"source_type": seed.SOURCE_TYPE},
    )
    assert r.status_code == 400, f"status={r.status_code} body={r.text}"


def test_by_external_id_403_user_token(api) -> None:
    r = api.get(
        "/internal/persons/by-external-id",
        params={"source_type": seed.SOURCE_TYPE, "external_id": seed.ALICE_ACCOUNT_ID},
    )
    assert r.status_code == 403, f"status={r.status_code} body={r.text}"


def test_by_external_id_401_unauthenticated(anon_api) -> None:
    r = anon_api.get(
        "/internal/persons/by-external-id",
        params={"source_type": seed.SOURCE_TYPE, "external_id": seed.ALICE_ACCOUNT_ID},
    )
    assert r.status_code == 401


def test_by_external_id_never_resolves_by_email(service_api) -> None:
    """A login-mode request carrying an email-shaped value in `external_id`
    must NOT resolve via any email fallback — the two contracts are separate
    routes, not a shared dispatch, so this must 404 like any other unknown
    external id."""
    r = service_api.get(
        "/internal/persons/by-external-id",
        params={"source_type": seed.SOURCE_TYPE, "external_id": seed.ALICE_EMAIL},
    )
    assert r.status_code == 404, f"status={r.status_code} body={r.text}"


def test_health_endpoints_public(anon_api) -> None:
    """/health + /healthz answer 200 with no auth (probe surface)."""
    assert anon_api.get("/health").status_code == 200
    assert anon_api.get("/healthz").status_code == 200
