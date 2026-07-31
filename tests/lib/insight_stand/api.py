"""HTTP client for the stand's API surface — through the gateway, only.

The gateway is the edge that terminates the session and the sole thing the
product actually exposes. Analytics and identity do publish their own host
ports on compose, but addressing them directly would skip authentication
entirely and prove nothing about the deployed path, so this client refuses to
build such a request: every path must sit under a prefix the gateway routes.

Prefixes come from `deploy/compose/gateway/routes.yaml`:

    /api/analytics  ->  http://analytics:8081   (strip_prefix: true)
    /api/identity   ->  http://identity:8082    (strip_prefix: true)

`strip_prefix` means the service sees its own `/v1/...`, so a caller writes the
full gateway path (`/api/analytics/v1/metrics`) and the rewrite is the
gateway's business.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from .credentials import AnonymousCredentials, Credentials

ANALYTICS_PREFIX = "/api/analytics"
IDENTITY_PREFIX = "/api/identity"
GATEWAY_API_PREFIXES: tuple[str, ...] = (ANALYTICS_PREFIX, IDENTITY_PREFIX)

# Served by the edge itself rather than routed to a backend: the SPA at `/` and
# the authenticator's browser endpoints under `/auth/`.
EDGE_PATH_PREFIXES: tuple[str, ...] = ("/auth/",)


def analytics_path(suffix: str) -> str:
    """`analytics_path("/v1/metrics") -> "/api/analytics/v1/metrics"`."""
    return f"{ANALYTICS_PREFIX}/{suffix.lstrip('/')}"


def identity_path(suffix: str) -> str:
    """`identity_path("/v1/persons") -> "/api/identity/v1/persons"`."""
    return f"{IDENTITY_PREFIX}/{suffix.lstrip('/')}"


@dataclass(frozen=True)
class ApiResponse:
    """A transport-agnostic view of one response.

    Tests assert against this rather than an `httpx.Response`, so swapping the
    HTTP library never touches a test.
    """

    status_code: int
    headers: Mapping[str, str]
    text: str
    url: str

    def json(self) -> Any:
        """Decoded JSON body, or `None` when the body is not JSON.

        Returning `None` rather than raising keeps a status-code assertion
        readable when the stand answers with an HTML error page.
        """
        import json

        try:
            return json.loads(self.text)
        except ValueError:
            return None

    @property
    def content_type(self) -> str:
        return self.headers.get("content-type", "")


@dataclass
class ApiClient:
    """Issues requests at one stand, with one credential.

    Credentials are attached per request via `Credentials.headers()`, so a
    client built with `AnonymousCredentials` genuinely sends nothing — the 401
    assertion is testing the stand, not a missing argument.
    """

    base_url: str
    credentials: Credentials = field(default_factory=AnonymousCredentials)
    timeout_s: float = 30.0

    def __post_init__(self) -> None:
        self.base_url = self.base_url.rstrip("/")

    # -- construction ------------------------------------------------------

    def with_credentials(self, credentials: Credentials) -> ApiClient:
        """A sibling client at the same stand with different auth material."""
        return ApiClient(
            base_url=self.base_url, credentials=credentials, timeout_s=self.timeout_s
        )

    # -- requests ----------------------------------------------------------

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        json_body: Any | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> ApiResponse:
        import httpx

        url = f"{self.base_url}{self._checked_path(path)}"
        merged: dict[str, str] = dict(self.credentials.headers())
        if headers:
            merged.update(headers)

        with httpx.Client(timeout=self.timeout_s, follow_redirects=False) as client:
            response = client.request(
                method, url, params=params, json=json_body, headers=merged
            )
        return ApiResponse(
            status_code=response.status_code,
            headers={k.lower(): v for k, v in response.headers.items()},
            text=response.text,
            url=str(response.url),
        )

    def get(self, path: str, **kwargs: Any) -> ApiResponse:
        return self.request("GET", path, **kwargs)

    def post(self, path: str, **kwargs: Any) -> ApiResponse:
        return self.request("POST", path, **kwargs)

    def put(self, path: str, **kwargs: Any) -> ApiResponse:
        return self.request("PUT", path, **kwargs)

    def delete(self, path: str, **kwargs: Any) -> ApiResponse:
        return self.request("DELETE", path, **kwargs)

    # -- guards ------------------------------------------------------------

    @staticmethod
    def _checked_path(path: str) -> str:
        """Reject anything the gateway does not front.

        A bare service path such as `/v1/metrics` is the classic way to end up
        addressing a backend port directly; catching it here turns that into an
        immediate, explanatory error instead of a puzzling 404.
        """
        if not path.startswith("/"):
            raise ValueError(f"path must be absolute, got {path!r}")
        if path == "/" or path.startswith(EDGE_PATH_PREFIXES):
            return path
        if path.startswith(GATEWAY_API_PREFIXES):
            return path
        allowed = ", ".join((*GATEWAY_API_PREFIXES, *EDGE_PATH_PREFIXES, "/"))
        raise ValueError(
            f"{path!r} is not a gateway-fronted path. Requests must go through "
            f"the gateway, never straight at a backend port; use one of: {allowed} "
            "(see deploy/compose/gateway/routes.yaml)."
        )


__all__: Sequence[str] = (
    "ANALYTICS_PREFIX",
    "EDGE_PATH_PREFIXES",
    "GATEWAY_API_PREFIXES",
    "IDENTITY_PREFIX",
    "ApiClient",
    "ApiResponse",
    "analytics_path",
    "identity_path",
)
