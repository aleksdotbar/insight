"""What a real session buys, and what it does not.

Every session here was won by driving the deployed OIDC chain against
Keycloak — `/auth/login`, the real HTML login form, `/auth/callback`,
`__Host-sid`. No token is minted anywhere in this suite. That distinction is
the entire point: a minted bearer would prove the analytics service verifies a
JWT, which the in-process rig already proves, and would say nothing about
whether a person can actually log in to the deployed product.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest
from insight_stand import (
    ADMIN_ROLE,
    LEAD_ROLE,
    MEMBER_ROLE,
    ApiClient,
    PersonaSession,
    analytics_path,
    identity_path,
)

METRICS = analytics_path("/v1/metrics")
SUBCHART = identity_path("/v1/subchart")


def _people(nodes: list[dict[str, Any]]) -> set[str]:
    """Every email in an org subchart, at any depth."""
    found: set[str] = set()
    for node in nodes:
        email = node.get("email")
        if email:
            found.add(str(email))
        found |= _people(node.get("subordinates") or [])
    return found


def test_a_real_session_turns_the_401_into_a_200(
    api_client: ApiClient, lead: PersonaSession
) -> None:
    """The pair, asserted together on one URL.

    Phase 5 could only assert the refusal half. Making both calls here, to the
    same path, means the two can never drift apart into testing different
    routes — the only difference between them is the session.
    """
    assert api_client.get(METRICS).status_code == 401

    assert lead.credentials.is_authenticated(), "login_as returned an unauthenticated session"
    response = lead.client.get(METRICS)
    assert response.status_code == 200, (
        f"expected 200 for {lead.email} at {response.url}, "
        f"got {response.status_code}: {response.text[:400]}"
    )
    body = response.json()
    assert isinstance(body, dict) and body.get("items"), (
        f"authenticated metrics response carried no items: {response.text[:400]}"
    )


def test_the_session_belongs_to_the_persona_who_logged_in(lead: PersonaSession) -> None:
    """A session that authenticates as somebody else is worse than none.

    Asserted through a CALLER-DERIVED endpoint on purpose. `/v1/persons/{email}`
    would only echo the address back — it proves the session can *read* that
    person, not that it *is* them. `/v1/subchart` takes no person argument at
    all: the stack resolves the caller from the session, so finding this
    persona's own email in the result is the whole chain confirming it landed
    on the intended human — Keycloak authenticated them, the authenticator
    mapped the token to a person, identity found that person in the roster.

    The person record is then read as a second, weaker check that the two
    agree on the UUID.
    """
    subchart = lead.client.get(SUBCHART)
    assert subchart.status_code == 200, (
        f"{lead.name} could not read {SUBCHART}: "
        f"{subchart.status_code} {subchart.text[:300]}"
    )
    visible = _people(subchart.json()["roots"])
    assert lead.email in visible, (
        f"the caller-derived org chart for {lead.name} contains {sorted(visible)}, "
        f"which does not include {lead.email} — the session resolved to someone else"
    )

    record = lead.client.get(identity_path(f"/v1/persons/{lead.email}"))
    assert record.status_code == 200, (
        f"could not read own person record: {record.status_code} {record.text[:300]}"
    )
    body = record.json()
    assert isinstance(body, dict), f"unexpected body: {record.text[:300]}"
    assert body.get("person_id") == lead.person.uuid, (
        f"identity resolved {lead.email} to person_id {body.get('person_id')!r}, "
        f"but the manifest says {lead.person.uuid!r}"
    )


def test_org_visibility_scope_differs_by_persona(
    admin: PersonaSession, lead: PersonaSession, member: PersonaSession
) -> None:
    """One endpoint, three personas, three materially different answers.

    This is the assertion the whole phase exists for: scope is enforced by the
    deployed stack, not by the test.

    Note what is NOT being claimed. The scope is derived from the seeded org
    chart, not from the caller's realm role — identity never reads the
    `insight-admin` / `insight-lead` / `insight-member` grants for this
    endpoint (its admin gate consults the `person_roles` table instead, which
    the seed leaves empty; see out/persona-matrix.md). The realm-role
    assertions below are therefore a precondition — they pin down WHICH three
    personas are being compared — and not the mechanism under test.

    Relationships are asserted rather than exact counts, so a roster change
    moves the numbers without inventing a failure.
    """
    assert admin.has_realm_role(ADMIN_ROLE)
    assert lead.has_realm_role(LEAD_ROLE) and not lead.has_realm_role(ADMIN_ROLE)
    assert member.has_realm_role(MEMBER_ROLE)
    assert admin.email != lead.email, "admin and lead resolved to the same persona"

    seen = {}
    for session in (admin, lead, member):
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

    admin_view, lead_view, member_view = (
        seen[admin.name],
        seen[lead.name],
        seen[member.name],
    )
    assert member_view == set(), (
        f"a plain member sees {sorted(member_view)} in the org chart; expected nothing"
    )
    assert lead_view, f"{lead.name} is a lead but sees nobody in the org chart"
    assert len(admin_view) > len(lead_view), (
        f"{admin.name} (admin) sees {len(admin_view)} people and {lead.name} (lead) sees "
        f"{len(lead_view)} — an admin must see strictly more"
    )
    assert lead_view <= admin_view, (
        f"{lead.name} sees {sorted(lead_view - admin_view)}, which the admin does not"
    )


@pytest.mark.requires_seed("dev_lead", "sales_lead")
def test_two_leads_of_different_teams_see_different_people(
    login_as: Callable[[str], PersonaSession],
) -> None:
    """Same role, same endpoint, different answers.

    Holding the realm role constant isolates the check to per-person scoping:
    if both leads saw the same set, visibility would be role-shaped only and
    the org chart would be leaking across teams.
    """
    dev, sales = login_as("dev_lead"), login_as("sales_lead")
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
