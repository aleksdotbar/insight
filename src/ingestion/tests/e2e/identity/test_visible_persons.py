"""Contract: POST /v1/visible-persons — the batch visibility filter analytics
gates metric-results on. Authenticated, NOT admin-gated."""

from __future__ import annotations

import pytest
from lib import identity_seed as seed

pytestmark = pytest.mark.identity


def _check(client, emails):
    return client.post("/v1/visible-persons", json={"emails": list(emails)})


def test_caller_sees_their_own_subtree_and_not_an_unrelated_person(api) -> None:
    """alice roots the seeded subtree, so she sees herself and her reports, but
    `hidden` sits outside it and is simply absent from the answer."""
    r = _check(api, [seed.ALICE_EMAIL, seed.CAROL_EMAIL, seed.HIDDEN_EMAIL])
    assert r.status_code == 200, f"status={r.status_code} body={r.text}"

    visible = set(r.json()["visible"])
    assert seed.ALICE_EMAIL in visible, "self is always visible"
    assert seed.CAROL_EMAIL in visible, "a transitive report is visible"
    assert seed.HIDDEN_EMAIL not in visible, "an unrelated person is refused"


def test_an_explicit_grant_makes_a_person_outside_the_subtree_visible(bob_api) -> None:
    """bob is not admin and `hidden` is not in his line; the seeded grant is the
    only reason he may see them."""
    visible = set(_check(bob_api, [seed.HIDDEN_EMAIL]).json()["visible"])
    assert seed.HIDDEN_EMAIL in visible


def test_an_unknown_email_is_absent_rather_than_an_error(api) -> None:
    """Absence carries both "not visible" and "no such person", so the endpoint
    cannot be used to probe which emails exist."""
    r = _check(api, [seed.UNKNOWN_EMAIL])
    assert r.status_code == 200, f"status={r.status_code} body={r.text}"
    assert r.json()["visible"] == []


def test_the_answer_echoes_the_requested_spelling(api) -> None:
    """Roughly half of stored emails differ in case from what a caller sends, so
    the reply must echo the request's spelling for the caller's own comparison
    to work."""
    shouted = seed.ALICE_EMAIL.upper()
    assert _check(api, [shouted]).json()["visible"] == [shouted]


def test_a_person_in_another_tenant_is_never_visible(api) -> None:
    """eve belongs to a different tenant; tenant scoping precedes visibility."""
    assert _check(api, [seed.EVE_EMAIL]).json()["visible"] == []
