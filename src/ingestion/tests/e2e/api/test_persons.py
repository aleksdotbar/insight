"""Contract: GET /v1/persons/{person_id} — person lookup (identity-backed).

The rig wires an in-process Identity stub (lib.identity_stub) into the analytics
config so this endpoint resolves against a real backend instead of short-circuiting
to the no-backend 500. That lets the documented outcomes be observed: 200 when the
seeded person resolves, 404 when they don't (#1691), 400 when the path key is not a
person UUID. The path key is the canonical person id since the identity cutover, so
an id read off a metric result resolves to a profile with no second mapping.
"""

from __future__ import annotations

import pytest

from lib.identity_stub import SEEDED_EMAIL, SEEDED_PERSON, SEEDED_PERSON_ID, UNKNOWN_EMAIL, UNKNOWN_PERSON_ID

pytestmark = pytest.mark.api


def test_person_lookup_200_found(api) -> None:
    """A seeded person id resolves: the handler returns the Person body verbatim
    (analytics `get_person` → `Json(serde_json::to_value(p))`)."""
    r = api.get(f"/v1/persons/{SEEDED_PERSON_ID}")
    assert r.status_code == 200, f"status={r.status_code} body={r.text}"
    person = r.json()
    assert person["email"] == SEEDED_EMAIL
    assert person["display_name"] == SEEDED_PERSON["display_name"]
    assert person["department"] == SEEDED_PERSON["department"]
    assert person["job_title"] == SEEDED_PERSON["job_title"]
    # subordinates is a required (non-Option) field on the analytics Person;
    # a missing/renamed field would have failed deserialization → 500, not 200.
    assert person["subordinates"] == []


def test_person_lookup_404_unknown(api) -> None:
    """A person id the backend doesn't know maps to a canonical 404 (the client's
    None → `PersonError::not_found`), not a 500 or an empty 200."""
    r = api.get(f"/v1/persons/{UNKNOWN_PERSON_ID}")
    assert r.status_code == 404, f"status={r.status_code} body={r.text}"
    problem = r.json()
    assert problem.get("status") == 404
    assert problem.get("type", "").endswith("cf.core.err.not_found.v1~"), problem
    # not_found carries the looked-up id as the resource name.
    assert problem.get("context", {}).get("resource_name") == UNKNOWN_PERSON_ID, problem


def test_person_lookup_400_pre_cutover_email_path(api) -> None:
    """A pre-cutover email URL is a loud 400 — not a 404, which would read as
    "no such person" — and it never reaches the identity hop."""
    r = api.get(f"/v1/persons/{UNKNOWN_EMAIL}")
    assert r.status_code == 400, f"status={r.status_code} body={r.text}"


def test_person_lookup_400_nil_person_id(api) -> None:
    """The nil UUID parses but is never a person."""
    r = api.get("/v1/persons/00000000-0000-0000-0000-000000000000")
    assert r.status_code == 400, f"status={r.status_code} body={r.text}"
