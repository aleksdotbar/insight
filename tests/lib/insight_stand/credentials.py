"""What a request carries to prove who it is.

The interface exposes only what an HTTP call needs *attached*, never how the
material was obtained. That is what lets two very different implementations
coexist without the interface moving:

* `RealLogin` — a session cookie won by actually logging in at the stand's IdP.
* a future minted-bearer credential — the `GatewayAuth.mint` / `auth_header`
  shape already used by `src/ingestion/tests/e2e/lib/gateway_jwt.py`, which
  returns exactly `{"Authorization": "Bearer <jwt>"}` and therefore drops
  straight into `headers()` with no signature change. (That rig is read-only
  reference material here; nothing in this tree imports from it.)

`AnonymousCredentials` makes "no auth" an explicit, named case rather than an
omitted argument, so a test that means to be unauthenticated says so.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from .errors import LoginNotCompletedError

if TYPE_CHECKING:  # pragma: no cover - typing only
    import httpx

# The authenticator's browser entry point. The gateway's own 401 body names it:
# "No valid session; authenticate at /auth/login."
LOGIN_PATH = "/auth/login"

# Session cookie minted by the authenticator after a successful OIDC callback.
# `__Host-` is not decoration: it pins the cookie to a secure origin with
# Path=/ and no Domain, which is the constraint phase 6 has to satisfy.
SESSION_COOKIE_NAME = "__Host-sid"


@runtime_checkable
class Credentials(Protocol):
    """Auth material to attach to a request."""

    def headers(self) -> dict[str, str]:
        """Headers to merge into the outgoing request.

        Either an `Authorization: Bearer …` header (JWT-style) or a `Cookie:`
        header (session-style) — the caller neither knows nor cares which.
        """
        ...

    def is_authenticated(self) -> bool:
        """False for an explicitly-anonymous credential."""
        ...


@dataclass(frozen=True)
class AnonymousCredentials:
    """No credential at all — the named case the 401 assertion uses."""

    def headers(self) -> dict[str, str]:
        return {}

    def is_authenticated(self) -> bool:
        return False


@dataclass
class RealLogin:
    """A session won by logging in for real at the stand's own IdP.

    No mocking and no token minting: `login()` drives the deployed
    authenticator's `/auth/login` entry point with a cookie jar and keeps
    whatever session cookie the stand actually sets.

    WHAT IS FINISHED HERE (phase 5): the transport and the state machine —
    starting the flow, following the redirect chain, recognising the session
    cookie, and exposing it through `headers()`.

    WHAT IS NOT (phase 6 owns it): completing the IdP's interactive challenge.
    Keycloak answers the authorize request with an HTML login form, and getting
    from that form to a session also means settling the callback origin, the
    `__Host-` cookie's secure-context requirement and the host-IP issuer. Until
    that lands, `login()` against a Keycloak stand stops at the form and raises
    `LoginNotCompletedError` naming the URL it stopped at — an honest failure rather
    than a stubbed success. `complete_challenge()` is the seam phase 6 fills.
    """

    base_url: str
    email: str
    password: str
    session_cookie_name: str = SESSION_COOKIE_NAME
    login_path: str = LOGIN_PATH
    timeout_s: float = 30.0
    _session_cookie: str | None = field(default=None, init=False, repr=False)

    # -- Credentials -------------------------------------------------------

    def headers(self) -> dict[str, str]:
        if self._session_cookie is None:
            self.login()
        return {"Cookie": f"{self.session_cookie_name}={self._session_cookie}"}

    def is_authenticated(self) -> bool:
        return self._session_cookie is not None

    # -- the flow ----------------------------------------------------------

    def login(self) -> None:
        """Drive the real login flow until a session cookie exists.

        Raises `LoginNotCompletedError` if the chain ends anywhere else.
        """
        import httpx

        with httpx.Client(
            base_url=self.base_url, timeout=self.timeout_s, follow_redirects=True
        ) as client:
            response = client.get(self.login_path)
            cookie = client.cookies.get(self.session_cookie_name)
            if cookie is None:
                cookie = self.complete_challenge(client, response)
            if cookie is None:
                raise LoginNotCompletedError(
                    f"login for {self.email!r} did not yield a "
                    f"{self.session_cookie_name!r} cookie; the flow stopped at "
                    f"{response.url}. Completing the IdP challenge is phase 6's "
                    "work (Keycloak login form + callback origin + secure-context "
                    "cookie).",
                    stopped_at=str(response.url),
                )
            self._session_cookie = cookie

    def complete_challenge(self, client: httpx.Client, response: httpx.Response) -> str | None:
        """Answer whatever the IdP put in front of the session. Phase 6.

        Returning `None` means "not completed" and lets `login()` raise with
        the full context. An override submits the Keycloak form and returns the
        resulting session cookie value.
        """
        _ = (client, response)
        return None

    def logout(self) -> None:
        """Forget the session so the next `headers()` logs in again."""
        self._session_cookie = None


__all__: Sequence[str] = (
    "LOGIN_PATH",
    "SESSION_COOKIE_NAME",
    "AnonymousCredentials",
    "Credentials",
    "RealLogin",
)
