"""The admin-gated half of identity-resolution.

Thirteen operations sit behind `require_admin`, which resolves the caller from
the gateway JWT and requires an active `admin` row in `identity.person_roles`.
It never reads the `insight-admin` REALM role — so the CEO, who holds that role,
is refused exactly like everybody else. The seed grants the row to one account:
the admin operator.

That account is deliberately **outside the organisation** — no team, no edge in
the org chart in either direction. It therefore contributes no activity data,
sees nobody in `/v1/subchart`, and cannot move a metric or a visibility
assertion. `test_operator_sees_nobody_in_the_org_chart` below is what keeps that
true: if the operator ever acquires an org edge, it fails here rather than as a
mysterious drift in an unrelated test.

The 401 half is in `test_gateway.py`, swept over every operation at once.

Only the read side is asserted so far. The create/delete round trips — and the
assertion that a visibility grant CHANGES what `/v1/subchart` returns for the
grantee — are the next step; see
`out/endpoint-coverage-implementation.md`.
"""

from __future__ import annotations

import pytest
from insight_stand import ADMIN_ROLE, ApiResponse, JsonValue, PersonaSession, identity_path

#: Read-only admin operations, and what each is a listing of.
ADMIN_LISTINGS = (
    "/v1/roles",
    "/v1/person-roles",
    "/v1/visibility",
    "/v1/persons-seed",
    "/v1/persons-sync",
)


def _items(response: ApiResponse) -> list[JsonValue]:
    body = response.json()
    assert isinstance(body, dict), (
        f"expected a JSON object from {response.url}: {response.text[:300]}"
    )
    items = body.get("items")
    assert isinstance(items, list), f"listing has no 'items' array: {response.text[:300]}"
    return items


@pytest.mark.requires_seed("admin_operator")
@pytest.mark.parametrize("path", ADMIN_LISTINGS)
def test_admin_listing_is_200_for_the_operator(
    admin_operator_session: PersonaSession, path: str
) -> None:
    """The grant works, on every admin route, through the gateway."""
    response = admin_operator_session.client.get(identity_path(path))
    assert response.status_code == 200, (
        f"{path} answered {response.status_code} to the admin operator: {response.text[:300]}"
    )
    _items(response)


@pytest.mark.requires_seed("admin_operator", "ceo")
@pytest.mark.parametrize("path", ADMIN_LISTINGS)
def test_admin_listing_is_403_for_a_realm_admin_without_the_grant(
    realm_admin_session: PersonaSession, path: str
) -> None:
    """Holding `insight-admin` in the realm is NOT administrative authority.

    The sharpest statement of what the gate reads. This persona carries the
    realm's admin role in its token and is still refused, because the gate
    consults `person_roles` and nothing else. A regression that started trusting
    the token's roles would open the admin API to the CEO, and only this test
    would notice.
    """
    assert realm_admin_session.has_realm_role(ADMIN_ROLE)
    response = realm_admin_session.client.get(identity_path(path))
    assert response.status_code == 403, (
        f"{path} answered {response.status_code} to {realm_admin_session.name}, who holds "
        f"{ADMIN_ROLE} in the realm but no person_roles grant: {response.text[:300]}"
    )


@pytest.mark.requires_seed("admin_operator")
def test_the_roles_catalogue_contains_the_admin_role(
    admin_operator_session: PersonaSession,
) -> None:
    """The role the operator holds is the one the catalogue lists.

    Closes the loop between the seed and the API: the grant was written against
    a `roles` row looked up by name, and this is that row coming back out.
    """
    response = admin_operator_session.client.get(identity_path("/v1/roles"))
    assert response.status_code == 200
    names = {str(item["name"]) for item in _items(response) if isinstance(item, dict)}
    assert "admin" in names, f"the roles catalogue does not contain 'admin': {sorted(names)}"


@pytest.mark.requires_seed("admin_operator")
def test_operator_sees_nobody_in_the_org_chart(
    admin_operator_session: PersonaSession,
) -> None:
    """The operator is in the roster but not in the organisation.

    An empty forest is the observable form of that. It is asserted rather than
    assumed because the isolation is what makes the account safe to add: the
    moment the operator gains an org edge it starts appearing in other people's
    views and contributing to metrics, and every scope assertion in the suite
    quietly changes meaning.
    """
    response = admin_operator_session.client.get(identity_path("/v1/subchart"))
    assert response.status_code == 200, f"status={response.status_code} {response.text[:300]}"
    body = response.json()
    assert isinstance(body, dict)
    assert body.get("roots") == [], (
        "the admin operator has an org-chart edge — it is supposed to sit outside "
        f"the organisation entirely: {response.text[:300]}"
    )
