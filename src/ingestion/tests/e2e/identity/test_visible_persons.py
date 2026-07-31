"""Contract: POST /v1/visible-persons — the batch visibility filter analytics
gates metric-results on. Authenticated, NOT admin-gated. Speaks canonical
person UUIDs (the identity-cutover contract; the earlier email-based draft
never shipped)."""

from __future__ import annotations

import uuid

import pytest

from lib import identity_seed as seed

pytestmark = pytest.mark.identity


def _check(client, person_ids):
    return client.post("/v1/visible-persons", json={"person_ids": [str(p) for p in person_ids]})


def test_caller_sees_their_own_subtree_and_not_an_unrelated_person(api) -> None:
    """alice roots the seeded subtree, so she sees herself and her reports, but
    `hidden` sits outside it and is simply absent from the answer."""
    r = _check(api, [seed.ALICE, seed.CAROL, seed.HIDDEN])
    assert r.status_code == 200, f"status={r.status_code} body={r.text}"

    visible = set(r.json()["visible"])
    assert str(seed.ALICE) in visible, "self is always visible"
    assert str(seed.CAROL) in visible, "a transitive report is visible"
    assert str(seed.HIDDEN) not in visible, "an unrelated person is refused"


def test_an_explicit_grant_makes_a_person_outside_the_subtree_visible(bob_api) -> None:
    """bob is not admin and `hidden` is not in his line; the seeded grant is the
    only reason he may see them."""
    visible = set(_check(bob_api, [seed.HIDDEN]).json()["visible"])
    assert str(seed.HIDDEN) in visible


def test_an_unknown_person_id_is_absent_rather_than_an_error(api) -> None:
    """Absence carries both "not visible" and "no such person", so the endpoint
    cannot be used to probe which person ids exist."""
    r = _check(api, [uuid.uuid4()])
    assert r.status_code == 200, f"status={r.status_code} body={r.text}"
    assert r.json()["visible"] == []


def test_a_person_in_another_tenant_is_never_visible(api) -> None:
    """eve belongs to a different tenant; tenant scoping precedes visibility."""
    assert _check(api, [seed.EVE]).json()["visible"] == []


def test_a_non_uuid_id_is_a_400(api) -> None:
    """The pre-cutover email shape must be a loud client error, not a silent
    empty answer that reads as `nothing visible`."""
    r = api.post("/v1/visible-persons", json={"person_ids": [seed.ALICE_EMAIL]})
    assert r.status_code == 400, f"status={r.status_code} body={r.text}"
