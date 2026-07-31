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

from collections.abc import Callable, Mapping, Sequence

import pytest
from insight_stand import ADMIN_ROLE, LEAD_ROLE, MEMBER_ROLE, ApiResponse, JsonValue, PersonaSession

from .routes import METRICS, SUBCHART

type Node = Mapping[str, JsonValue]


def _roots(response: ApiResponse) -> list[Node]:
    """The subchart's root nodes, or a readable failure.

    Narrowing the decoded body once, here, is what lets the tests below index
    into it without casting. A response that is not the documented shape fails
    as a statement about the payload rather than as a `TypeError` from the
    first subscript.
    """
    body = response.json()
    assert isinstance(body, dict), (
        f"expected a JSON object from {response.url}, got: {response.text[:300]}"
    )
    roots = body.get("roots")
    assert isinstance(roots, list), (
        f"subchart from {response.url} has no 'roots' list: {response.text[:300]}"
    )
    nodes: list[Node] = [node for node in roots if isinstance(node, Mapping)]
    assert len(nodes) == len(roots), (
        f"subchart from {response.url} has a non-object root: {response.text[:300]}"
    )
    return nodes


def _nodes(roots: Sequence[Node]) -> list[Node]:
    """Flatten an org subchart to every node, at any depth."""
    out: list[Node] = []
    for node in roots:
        out.append(node)
        subordinates = node.get("subordinates")
        if isinstance(subordinates, list):
            out += _nodes([s for s in subordinates if isinstance(s, Mapping)])
    return out


def _people(roots: Sequence[Node]) -> set[str]:
    """Every email in an org subchart, at any depth."""
    return {str(node["email"]) for node in _nodes(roots) if node.get("email")}


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


def test_identity_subchart_is_200_with_a_session(lead_session: PersonaSession) -> None:
    """The counterpart to `test_identity_subchart_is_401_without_a_session`.

    Same URL, same stand; the session is the only difference. Populated from
    the seeded org chart, so it also rules out the boring explanation for the
    401 — that the route had nothing behind it.
    """
    response = lead_session.client.get(SUBCHART)
    assert response.status_code == 200, (
        f"expected 200 for {lead_session.email} at {response.url}, "
        f"got {response.status_code}: {response.text[:400]}"
    )
    assert _roots(response), f"authenticated subchart carried no roots: {response.text[:400]}"


def test_the_session_belongs_to_the_persona_who_logged_in(lead_session: PersonaSession) -> None:
    """A session that authenticates as somebody else is worse than none.

    Asserted through a CALLER-DERIVED endpoint on purpose: `/v1/subchart` takes
    no person argument, so the stack resolves the caller from the session
    alone. Finding this persona in the result — and finding the manifest's UUID
    on that node — is the whole chain confirming it landed on the intended
    human: Keycloak authenticated them, the authenticator mapped the token to a
    person, and identity found that person in the seeded roster.
    """
    response = lead_session.client.get(SUBCHART)
    assert response.status_code == 200, (
        f"{lead_session.name} could not read {SUBCHART}: "
        f"{response.status_code} {response.text[:300]}"
    )
    nodes = _nodes(_roots(response))
    mine = [n for n in nodes if n.get("email") == lead_session.email]
    assert len(mine) == 1, (
        f"the caller-derived org chart for {lead_session.name} contains "
        f"{sorted(str(n.get('email')) for n in nodes)}, which does not name "
        f"{lead_session.email} exactly once — the session resolved to someone else"
    )
    assert mine[0].get("person_id") == lead_session.person.uuid, (
        f"identity resolved {lead_session.email} to person_id "
        f"{mine[0].get('person_id')!r}, but the manifest says "
        f"{lead_session.person.uuid!r}"
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
        seen[session.name] = _people(_roots(response))

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
        views[session.name] = _people(_roots(response))
    dev_view, sales_view = views[dev.name], views[sales.name]

    assert dev_view and sales_view, "expected both leads to see somebody"
    assert dev_view != sales_view, (
        f"both leads see the same people ({sorted(dev_view)}) — visibility is not per-person"
    )
    assert not (dev_view & sales_view), (
        f"leads of different teams share {sorted(dev_view & sales_view)}"
    )
