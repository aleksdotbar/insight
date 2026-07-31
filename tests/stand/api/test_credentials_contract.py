"""Both credential implementations satisfy one interface.

The interface's whole job is to outlive its implementations: a minted-bearer
credential (the `GatewayAuth.mint` / `auth_header` shape from the in-process
rig) has to be addable later without any signature moving. Checking conformance
here means that promise is enforced rather than merely intended — and it is
what lets phase 6 hand a `RealLogin` to `ApiClient.with_credentials` and change
nothing else.
"""

from __future__ import annotations

import pytest
from insight_stand import AnonymousCredentials, ApiClient, Credentials, Manifest, RealLogin


def test_anonymous_credentials_conform() -> None:
    credentials = AnonymousCredentials()
    assert isinstance(credentials, Credentials)
    assert credentials.headers() == {}
    assert credentials.is_authenticated() is False


@pytest.mark.requires_seed("dev_lead")
def test_real_login_conforms_without_touching_the_network(
    stand_base_url: str, stand_manifest: Manifest
) -> None:
    """Constructing a `RealLogin` performs no I/O.

    It stays inert until `login()` (or `headers()`, which calls it) is asked
    for a session, so building one costs nothing and cannot make an unrelated
    test flaky. The password is irrelevant precisely because nothing here logs
    in — phase 6 supplies the real one when it drives the flow.
    """
    dev_lead = stand_manifest.fixture("dev_lead")
    login = RealLogin(
        base_url=stand_base_url,
        email=dev_lead.email,
        password="unused-no-login-is-performed-here",
    )
    assert isinstance(login, Credentials)
    assert login.is_authenticated() is False


def test_api_client_accepts_any_conforming_credential(stand_base_url: str) -> None:
    """`with_credentials` is the only seam auth arrives through."""
    anonymous = ApiClient(base_url=stand_base_url)
    swapped = anonymous.with_credentials(AnonymousCredentials())
    assert swapped.base_url == anonymous.base_url
    assert swapped is not anonymous
