"""One person's own view — `/ic/$person/personal`. Locators and navigation only.

`$person` is the person's EMAIL, not their UUID, and the SPA builds the link with
`encodeURIComponent`, so `@` arrives as `%40`. Encoding it here rather than in a
test keeps the URL shape a property of the view.

Accessibility-first, like every page object here: the published SPA carries no
`data-testid` attributes at all (re-verified across the whole shipped bundle),
so roles and accessible names are the only stable handles.
"""

from __future__ import annotations

from urllib.parse import quote

from playwright.sync_api import Locator, Page


class PersonView:
    def __init__(self, page: Page) -> None:
        self.page = page

    @staticmethod
    def path(email: str) -> str:
        return f"/ic/{quote(email, safe='')}/personal"

    def go(self, email: str) -> None:
        self.page.goto(self.path(email), wait_until="domcontentloaded")

    def person_heading(self, display_name: str) -> Locator:
        return self.page.get_by_role("heading", name=display_name)

    def metric_tile(self, label: str) -> Locator:
        """The tile for one named metric.

        Addressed by its visible label rather than by position: the set of tiles
        and their order are product decisions, and a test that indexed into them
        would fail on a layout change that broke nothing.
        """
        return self.page.get_by_role("listitem").filter(has_text=label).first
