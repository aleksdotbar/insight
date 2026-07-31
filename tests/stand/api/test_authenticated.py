"""200 — what a real session buys.

Every session here was won by driving the deployed OIDC chain against
Keycloak: `/auth/login`, the real HTML login form, `/auth/callback`,
`__Host-sid`. No token is minted anywhere in this suite. That distinction is
the point — a minted bearer would prove the analytics service verifies a JWT,
which the in-process rig already proves, and would say nothing about whether a
person can log in to the deployed product.

`test_unauthenticated.py` asserts the 401 half on the same paths, imported
from `routes` so the two cannot drift apart.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest
from insight_stand import ADMIN_ROLE, LEAD_ROLE, MEMBER_ROLE, PersonaSession

from .routes import METRICS, SUBCHART, person


def _people(nodes: list[dict[str, Any]]) -> set[str]:
    """Every email in an org subchart, at any depth."""
    found: set[str] = set()
    for node in nodes:
        email = node.get("email")
        if email:
            found.add(str(email))
        found |= _people(node.get("subordinates") or [])
    return found


def test_analytics_metrics_is_200_with_a_session(lead_session: PersonaSession) -> None:
    """The counterpart to `test_analytics_metrics_is_401_without_a_session`.

    Same URL, same stand; the session is the only difference. A 200 carrying an
    empty body would be a different defect, so the catalog is checked too.
    """
    response = lead_session.client.get(METRICS)
    assert response.status_code == 200, (
        f"expected 200 for {lead_session.email} at {response.url}, "
        f"got {response.status_code}: {response.text[:400]}"
    )
    body = response.json()
    assert isinstance(body, dict) and body.get("items"), (
        f"authenticated metrics response carried no items: {response.text[:400]}"
    )


def test_seeded_person_is_200_with_a_session(lead_session: PersonaSession) -> None:
    """The counterpart to `test_seeded_person_is_401_without_a_session`.

    Also pins the identity mapping: the record identity returns for this email
    must be the person the manifest says it is.
    """
    response = lead_session.client.get(person(lead_session.email))
    assert response.status_code == 200, (
        f"expected 200 reading own person record at {response.url}, "
        f"got {response.status_code}: {response.text[:400]}"
    )
    body = response.json()
    assert isinstance(body, dict), f"unexpected body: {response.text[:300]}"
    assert body.get("person_id") == lead_session.person.uuid, (
        f"identity resolved {lead_session.email} to person_id {body.get('person_id')!r}, "
        f"but the manifest says {lead_session.person.uuid!r}"
    )


def test_the_session_belongs_to_the_persona_who_logged_in(lead_session: PersonaSession) -> None:
    """A session that authenticates as somebody else is worse than none.

    Asserted through a CALLER-DERIVED endpoint on purpose. `/v1/persons/{email}`
    only echoes the address back — it shows the session can *read* that person,
    not that it *is* them. `/v1/subchart` takes no person argument at all: the
    stack resolves the caller from the session, so finding this persona's own
    email in the result is the whole chain confirming it landed on the intended
    human.
    """
    response = lead_session.client.get(SUBCHART)
    assert response.status_code == 200, (
        f"{lead_session.name} could not read {SUBCHART}: {response.status_code} {response.text[:300]}"
    )
    visible = _people(response.json()["roots"])
    assert lead_session.email in visible, (
        f"the caller-derived org chart for {lead_session.name} contains {sorted(visible)}, "
        f"which does not include {lead_session.email} — the session resolved to someone else"
    )


def test_org_visibility_scope_differs_by_persona(
    admin_session: PersonaSession, lead_session: PersonaSession, member_session: PersonaSession
) -> None:
    """One endpoint, three personas, three materially different answers.

    Scope is enforced by the deployed stack, not by the test.

    Note what is NOT being claimed. The scope comes from the seeded org chart,
    not from the caller's realm role — identity never reads the
    `insight-admin` / `insight-lead` / `insight-member` grants for this endpoint
    (its admin gate consults the `person_roles` table instead, which the seed
    leaves empty; see out/persona-matrix.md). The realm-role assertions below
    are a precondition pinning down WHICH three personas are compared, not the
    mechanism under test.

    Relationships are asserted rather than exact counts, so a roster change
    moves the numbers without inventing a failure.
    """
    assert admin_session.has_realm_role(ADMIN_ROLE)
    assert lead_session.has_realm_role(LEAD_ROLE) and not lead_session.has_realm_role(ADMIN_ROLE)
    assert member_session.has_realm_role(MEMBER_ROLE)
    assert admin_session.email != lead_session.email, "admin and lead resolved to the same persona"

    seen = {}
    for session in (admin_session, lead_session, member_session):
        response = session.client.get(SUBCHART)
        assert response.status_code == 200, (
            f"{session.name} could not read {SUBCHART}: "
            f"{response.status_code} {response.text[:300]}"
        )
        body = response.json()
        assert isinstance(body, dict) and "roots" in body, (
            f"unexpected subchart body for {session.name}: {response.text[:300]}"
        )
        seen[session.name] = _people(body["roots"])

    admin_view = seen[admin_session.name]
    lead_view = seen[lead_session.name]
    member_view = seen[member_session.name]
    assert member_view == set(), (
        f"a plain member sees {sorted(member_view)} in the org chart; expected nothing"
    )
    assert lead_view, f"{lead_session.name} is a lead but sees nobody in the org chart"
    assert len(admin_view) > len(lead_view), (
        f"{admin_session.name} (admin) sees {len(admin_view)} people and {lead_session.name} (lead) sees "
        f"{len(lead_view)} — an admin must see strictly more"
    )
    assert lead_view <= admin_view, (
        f"{lead_session.name} sees {sorted(lead_view - admin_view)}, which the admin does not"
    )


@pytest.mark.requires_seed("dev_lead", "sales_lead")
def test_two_leads_of_different_teams_see_different_people(
    session_for: Callable[[str], PersonaSession],
) -> None:
    """Same role, same endpoint, different answers.

    Holding the realm role constant isolates the check to per-person scoping:
    if both leads saw the same set, visibility would be role-shaped only and
    the org chart would be leaking across teams.
    """
    dev, sales = session_for("dev_lead"), session_for("sales_lead")
    assert dev.person.team != sales.person.team

    views = {}
    for session in (dev, sales):
        response = session.client.get(SUBCHART)
        assert response.status_code == 200, (
            f"{session.name} could not read {SUBCHART}: "
            f"{response.status_code} {response.text[:300]}"
        )
        views[session.name] = _people(response.json()["roots"])
    dev_view, sales_view = views[dev.name], views[sales.name]

    assert dev_view and sales_view, "expected both leads to see somebody"
    assert dev_view != sales_view, (
        f"both leads see the same people ({sorted(dev_view)}) — visibility is not per-person"
    )
    assert not (dev_view & sales_view), (
        f"leads of different teams share {sorted(dev_view & sales_view)}"
    )
