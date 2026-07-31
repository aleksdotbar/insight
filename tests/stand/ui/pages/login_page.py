"""The application entry point. Locators and actions only — no assertions,
no test data, no branching on page state."""

from __future__ import annotations

from playwright.sync_api import Locator, Page


class LoginPage:
    """The app's own origin, before a session exists.

    On this stand there is nothing to click: an unauthenticated visit to `/`
    starts the OIDC chain by itself and the browser lands on the IdP's form.
    `sign_in_control()` is kept for stands whose SPA renders an explicit
    control instead — it is a locator, so defining it costs nothing and
    resolves nothing until used.
    """

    def __init__(self, page: Page) -> None:
        self.page = page

    def go(self) -> None:
        """Open the app root. The context's `base_url` supplies the origin."""
        self.page.goto("/", wait_until="domcontentloaded")

    def sign_in_control(self) -> Locator:
        return self.page.get_by_role("link", name="Log in")
