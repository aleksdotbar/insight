"""Browser wiring for the UI journeys.

The browser drives `stand_base_url` — the same URL the API clients use. One
address for the whole suite, resolved once in `tests/stand/conftest.py`.

That URL has to be `localhost`-based, and not for convenience. `__Host-`
prefixed cookies are only stored from a **trustworthy** origin, and over plain
http a browser trusts exactly one host name. Point a runner at
`gateway:8080` and the session cookie is dropped without a word: the SPA sees
`/auth/me` 401, restarts the login, and loops until the gateway's rate limiter
turns it into a 503 that looks like a broken backend. Chromium's
`--unsafely-treat-insecure-origin-as-secure` does not help —
`window.isSecureContext` was measured as `false` with the flag on Chromium 149,
in `launch()` and `launch_persistent_context()`, with and without
`--user-data-dir`.

So a containerised runner joins the gateway's network namespace
(`--network container:insight-gateway`) and uses `localhost:<port>`, which is
genuinely trustworthy and needs no flags. A host-side run uses the same URL
against the published port. Either way the stand's registered `/auth/callback`
redirect URI matches, so one configuration serves both.
"""

from __future__ import annotations

from typing import Any

import pytest
from playwright.sync_api import expect

# Playwright's own defaults are already generous where it matters — 30s for
# actions and navigation — so they are left alone. `expect()` is the exception:
# its 5s default is tight for a cold SPA that renders after an OIDC round trip,
# and raising it is what lets the journeys use web-first assertions instead of
# sleeping or retrying.
expect.set_options(timeout=15_000)


@pytest.fixture(scope="session")
def browser_context_args(
    browser_context_args: dict[str, Any], stand_base_url: str
) -> dict[str, Any]:
    """Give every context the stand as its base URL.

    Journeys then navigate by path (`page.goto("/")`) and
    `expect(page).to_have_url(...)` reads cleanly. Extends pytest-playwright's
    own fixture rather than replacing it, so options contributed by the plugin
    or a CLI flag survive.
    """
    return {**browser_context_args, "base_url": stand_base_url}
