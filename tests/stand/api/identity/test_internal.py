"""`/internal/*` — the routes only a SERVICE may call.

`/internal/persons/by-email/{email}` is how the authenticator resolves a person
during login. It is the one route in this suite reached by something other than
a logged-in human, and the only place a service principal appears.

The credential is obtained, not minted: an RFC 7523 assertion signed with the
stand's `testclient` key is exchanged at the authenticator's own token endpoint
for a gateway JWT whose `sub_type` is `service`. See
`tests/lib/insight_stand/service_token.py` for why that distinction is the whole
point of testing this here rather than in the in-process rig.

Both halves matter and neither means anything alone. A 200 for the service
principal is equally consistent with the route being open to anybody
authenticated; a 403 for a person is equally consistent with the route being
broken. Together they say the gate is on the KIND of principal.

They also use two different ADDRESSES, and that is the product, not a
workaround. The gateway is a browser BFF: it delegates authz to the
authenticator, which looks for a session cookie and answers `401 no_session` to
a bearer-carrying request, so a service principal has no edge address at all.
The service therefore calls identity-resolution directly, exactly as the
authenticator does during login, while the human's refusal is asserted at
`/api/identity/...` where a human's request actually arrives. See
`service_token.default_identity_url`.

Skipped, with a reason, on a stand whose token listener this runner cannot
reach — a k8s stand keeps it in-cluster with no ingress. That is what
`requires_service_principal` reads.
"""

from __future__ import annotations

import pytest
from insight_stand import ApiClient, Manifest, PersonaSession, identity_path

from ..schemas import Profile


@pytest.mark.requires_service_principal
@pytest.mark.requires_seed("dev_lead")
def test_internal_lookup_serves_a_service_principal(
    service_client: ApiClient, stand_manifest: Manifest
) -> None:
    """The S2S route answers a caller the authenticator actually issued a token to.

    `/internal/persons/by-email/{email}` is how the authenticator resolves a
    person during login, and it is the only route in this suite reached by
    something other than a logged-in human. The credential is not minted: an
    RFC 7523 assertion signed with the stand's `testclient` key is exchanged at
    the authenticator's token endpoint for a gateway JWT whose `sub_type` is
    `service`. So a pass means the whole issuance path works, not merely that
    identity compares a claim.
    """
    person = stand_manifest.fixture("dev_lead")
    response = service_client.get(f"/internal/persons/by-email/{person.email}")
    assert response.status_code == 200, (
        f"the service principal was refused {person.email}: "
        f"{response.status_code} {response.text[:300]}"
    )
    assert str(response.parse(Profile).person_id) == person.uuid


@pytest.mark.requires_service_principal
@pytest.mark.requires_seed("dev_lead")
def test_internal_lookup_refuses_a_person(
    lead_session: PersonaSession, stand_manifest: Manifest
) -> None:
    """A logged-in human is refused the same route.

    The half that makes the test above mean something: without it, a 200 for
    the service principal would be equally consistent with the route being open
    to anybody authenticated. Same url, same tenant, same seeded person —
    differing only in what kind of principal is asking.
    """
    person = stand_manifest.fixture("dev_lead")
    response = lead_session.client.get(identity_path(f"/internal/persons/by-email/{person.email}"))
    assert response.status_code == 403, (
        f"a person reached the service-only route (status {response.status_code}) — "
        f"/internal/* is restricted to sub_type=service: {response.text[:300]}"
    )
