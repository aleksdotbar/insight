"""Contract: POST /v1/profiles — resolve one identity to a person profile.

The successor read endpoint (the deprecated GET /v1/persons/{email} has no
callers and is not part of the contract). Request:
`{value_type, value, insight_source_type?, insight_source_id?}`;
value_type="email" resolves across ALL sources (source fields MUST be null),
value_type="id" resolves a source-native account id within ONE source (both
source fields REQUIRED), value_type="person_id" takes the canonical person
UUID itself (source fields MUST be null) — the key the metrics runtime and the
SPA routes use since the identity cutover. Visibility gates every outcome: a
caller resolves only persons in their org subtree or explicitly granted — a
hidden candidate is indistinguishable from a missing one (404).
"""

from __future__ import annotations

import pytest

from identity.contract import AMBIGUOUS_STATUSES, problem
from lib import identity_seed as seed

pytestmark = pytest.mark.identity


def _resolve_email(client, email):
    return client.post("/v1/profiles", json={"value_type": "email", "value": email})


def _resolve_person_id(client, person_id):
    return client.post(
        "/v1/profiles", json={"value_type": "person_id", "value": str(person_id)}
    )


def test_resolve_by_email_200_full_profile(api) -> None:
    """A visible subordinate resolves to the full profile: identity fields,
    tenant, supervisor projection (from org_chart + the parent's own
    observations), source-native ids, and the recursive subordinates tree."""
    r = _resolve_email(api, seed.BOB_EMAIL)
    assert r.status_code == 200, f"status={r.status_code} body={r.text}"
    p = r.json()
    assert p["person_id"] == str(seed.BOB)
    assert p["email"] == seed.BOB_EMAIL
    assert p["display_name"] == "Bob Builder"
    assert p["department"] == "Engineering"
    assert p["job_title"] == "Team Lead"
    assert p["status"] == "Active"
    assert p["insight_tenant_id"] == str(seed.TEST_TENANT_ID)
    # Supervisor projection: bob's org_chart parent is alice.
    assert p.get("supervisor_email") == seed.ALICE_EMAIL
    assert p.get("supervisor_name") == "Alice Admin"
    # One current value_type='id' observation per source.
    ids = p.get("ids") or []
    assert {
        "insight_source_type": seed.SOURCE_TYPE,
        "insight_source_id": str(seed.SOURCE_ID),
        "value": "acc-bob",
    } in ids, ids
    # Recursive subordinates: carol reports to bob.
    subordinate_ids = [s["person_id"] for s in (p.get("subordinates") or [])]
    assert str(seed.CAROL) in subordinate_ids, p.get("subordinates")


def test_resolve_by_source_id_200(api) -> None:
    """value_type='id' + both source fields resolves the source-native account."""
    r = api.post(
        "/v1/profiles",
        json={
            "value_type": "id",
            "value": "acc-bob",
            "insight_source_type": seed.SOURCE_TYPE,
            "insight_source_id": str(seed.SOURCE_ID),
        },
    )
    assert r.status_code == 200, f"status={r.status_code} body={r.text}"
    assert r.json()["person_id"] == str(seed.BOB)


def test_resolve_unknown_email_404(api) -> None:
    r = _resolve_email(api, seed.UNKNOWN_EMAIL)
    assert r.status_code == 404, f"status={r.status_code} body={r.text}"
    problem(r)


def test_missing_value_type_400(api) -> None:
    r = api.post("/v1/profiles", json={"value": seed.BOB_EMAIL})
    assert r.status_code == 400, f"status={r.status_code} body={r.text}"
    problem(r)


def test_email_with_source_fields_400(api) -> None:
    """value_type='email' forbids the source fields (they select the 'id' mode)."""
    r = api.post(
        "/v1/profiles",
        json={
            "value_type": "email",
            "value": seed.BOB_EMAIL,
            "insight_source_type": seed.SOURCE_TYPE,
            "insight_source_id": str(seed.SOURCE_ID),
        },
    )
    assert r.status_code == 400, f"status={r.status_code} body={r.text}"
    problem(r)


def test_ambiguous_email(api) -> None:
    """Two visible persons share the email → the data-invariant violation is
    surfaced, not silently resolved. KNOWN DIVERGENCE: .NET 422, Rust 409."""
    r = _resolve_email(api, seed.DUP_EMAIL)
    assert r.status_code in AMBIGUOUS_STATUSES, f"status={r.status_code} body={r.text}"
    problem(r)


def test_hidden_person_is_404_without_grant(api) -> None:
    """Roles ≠ visibility: alice is the tenant admin but `hidden` is outside
    her subtree and she holds no grant — indistinguishable from not-found."""
    r = _resolve_email(api, seed.HIDDEN_EMAIL)
    assert r.status_code == 404, f"status={r.status_code} body={r.text}"


def test_hidden_person_resolves_with_explicit_grant(bob_api) -> None:
    """bob holds the seeded visibility grant on `hidden` → 200."""
    r = _resolve_email(bob_api, seed.HIDDEN_EMAIL)
    assert r.status_code == 200, f"status={r.status_code} body={r.text}"
    assert r.json()["person_id"] == str(seed.HIDDEN)


def test_cross_tenant_email_404(api) -> None:
    """eve exists only in OTHER_TENANT — invisible to a TEST_TENANT caller."""
    r = _resolve_email(api, seed.EVE_EMAIL)
    assert r.status_code == 404, f"status={r.status_code} body={r.text}"


def test_unauthenticated_401(anon_api) -> None:
    r = anon_api.post("/v1/profiles", json={"value_type": "email", "value": seed.BOB_EMAIL})
    assert r.status_code == 401, f"status={r.status_code} body={r.text}"


def test_resolve_by_person_id_200_same_profile_as_email(api) -> None:
    """The canonical person id resolves the same profile the email does — the
    two keys are two spellings of one identity, not two views of it."""
    by_id = _resolve_person_id(api, seed.CAROL)
    assert by_id.status_code == 200, f"status={by_id.status_code} body={by_id.text}"
    by_email = _resolve_email(api, seed.CAROL_EMAIL)
    assert by_email.status_code == 200, f"status={by_email.status_code} body={by_email.text}"
    assert by_id.json() == by_email.json()


def test_resolve_by_person_id_of_an_emailless_person(api) -> None:
    """A person id needs no email to resolve: the SPA routes by person id, so a
    person the log knows without a current email must still answer."""
    r = _resolve_person_id(api, seed.NO_EMAIL_PERSON)
    assert r.status_code == 200, f"status={r.status_code} body={r.text}"
    assert r.json()["person_id"] == str(seed.NO_EMAIL_PERSON)
    assert not r.json().get("email"), r.json()


def test_person_id_visibility_gates_the_same_way_as_email(api, bob_api) -> None:
    """Visibility is a property of the person, not of the key used to name
    them: `hidden` is 404 for alice and 200 for bob (seeded grant), exactly as
    with the email key."""
    assert _resolve_person_id(api, seed.HIDDEN).status_code == 404
    granted = _resolve_person_id(bob_api, seed.HIDDEN)
    assert granted.status_code == 200, f"status={granted.status_code} body={granted.text}"
    assert granted.json()["person_id"] == str(seed.HIDDEN)


def test_person_id_cross_tenant_404(api) -> None:
    """eve exists only in OTHER_TENANT — the person-id key cannot cross it."""
    assert _resolve_person_id(api, seed.EVE).status_code == 404


def test_unknown_person_id_404(api) -> None:
    """An id the log never observed is absent, not an error — the same shape an
    unknown email takes, so the endpoint cannot probe which ids exist."""
    assert _resolve_person_id(api, "aaaaaaaa-0000-4000-8000-0000000000ff").status_code == 404


def test_person_id_rejects_a_non_uuid_value(api) -> None:
    """An email under value_type='person_id' is a client error, never a silent
    empty resolution."""
    r = api.post("/v1/profiles", json={"value_type": "person_id", "value": seed.ALICE_EMAIL})
    assert r.status_code == 400, f"status={r.status_code} body={r.text}"
    # The violation must name the offending field: an operator reading the log
    # should not have to guess which of the three keys was malformed.
    assert "value" in r.text, r.text


def test_person_id_rejects_the_nil_uuid(api) -> None:
    """The nil UUID is never a person; it must not read as "not found"."""
    r = _resolve_person_id(api, "00000000-0000-0000-0000-000000000000")
    assert r.status_code == 400, f"status={r.status_code} body={r.text}"


def test_person_id_forbids_the_source_fields(api) -> None:
    """Source scoping selects the 'id' mode; a person id is tenant-wide."""
    r = api.post(
        "/v1/profiles",
        json={
            "value_type": "person_id",
            "value": str(seed.CAROL),
            "insight_source_type": seed.SOURCE_TYPE,
            "insight_source_id": str(seed.SOURCE_ID),
        },
    )
    assert r.status_code == 400, f"status={r.status_code} body={r.text}"
