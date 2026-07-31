"""Shared floor for the Insight deployed-stand test suite (`tests/stand/`).

Four things live here, and nothing else:

* `manifest` — the typed model of the stand's self-description
  (`deploy/seed/manifest.json`), the only source of fixture names, capabilities
  and seeded facts.
* `stand` — where the stand is: base-URL resolution for a host-side or
  in-network runner.
* `credentials` — what a request carries: the `Credentials` interface plus
  `AnonymousCredentials` and `RealLogin`.
* `api` — the gateway-fronted HTTP client.
* `wait` — bounded polling for eventually-consistent state.

This package is deliberately test-framework agnostic: no pytest import, no
fixtures, no assertions. `tests/stand/conftest.py` is what turns it into a
suite, and phases 6-8 add person fixtures and browser journeys on top. Nothing
here imports from `src/ingestion/tests/e2e/**` — that rig owns in-process
correctness and feeds four blocking coverage gates; it is read-only reference.
"""

from __future__ import annotations

from collections.abc import Sequence

from .api import (
    ANALYTICS_PREFIX,
    GATEWAY_API_PREFIXES,
    IDENTITY_PREFIX,
    ApiClient,
    ApiResponse,
    analytics_path,
    identity_path,
)
from .credentials import (
    LOGIN_PATH,
    SESSION_COOKIE_NAME,
    AnonymousCredentials,
    Credentials,
    RealLogin,
)
from .errors import LoginNotCompletedError, ManifestError, StandConnectionError, StandError
from .manifest import (
    MANIFEST_PATH,
    SUPPORTED_MANIFEST_VERSION,
    Capabilities,
    GoldenMetric,
    Manifest,
    Person,
    Realm,
    load_manifest,
)
from .stand import BASE_URL_ENV, StandEndpoint, resolve_base_url, resolve_endpoint
from .wait import wait_for, wait_until

__all__: Sequence[str] = (
    "ANALYTICS_PREFIX",
    "BASE_URL_ENV",
    "GATEWAY_API_PREFIXES",
    "IDENTITY_PREFIX",
    "LOGIN_PATH",
    "MANIFEST_PATH",
    "SESSION_COOKIE_NAME",
    "SUPPORTED_MANIFEST_VERSION",
    "AnonymousCredentials",
    "ApiClient",
    "ApiResponse",
    "Capabilities",
    "Credentials",
    "GoldenMetric",
    "LoginNotCompletedError",
    "Manifest",
    "ManifestError",
    "Person",
    "RealLogin",
    "Realm",
    "StandConnectionError",
    "StandEndpoint",
    "StandError",
    "analytics_path",
    "identity_path",
    "load_manifest",
    "resolve_base_url",
    "resolve_endpoint",
    "wait_for",
    "wait_until",
)
