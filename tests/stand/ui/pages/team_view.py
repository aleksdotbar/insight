"""A lead's team view — `/ic/$person/team`. Locators and navigation only."""

from __future__ import annotations

from urllib.parse import quote

from playwright.sync_api import Locator, Page


class TeamView:
    def __init__(self, page: Page) -> None:
        self.page = page

    @staticmethod
    def path(email: str) -> str:
        return f"/ic/{quote(email, safe='')}/team"

    def go(self, email: str) -> None:
        self.page.goto(self.path(email), wait_until="domcontentloaded")

    def team_heading(self, display_name: str) -> Locator:
        """The heading naming whose team this is.

        The accessible name is composed by the SPA ("Team of <name>"), so it is
        matched as a substring rather than reconstructed here — reconstructing it
        would put the product's copy in the test.
        """
        return self.page.get_by_role("heading").filter(has_text=display_name).first

    def member_row(self, display_name: str) -> Locator:
        """That member's row in the team table.

        A ROW, not a link named after them, and the distinction is the whole
        value of the locator. The sidebar renders every person in the signed-in
        user's org scope on EVERY view, so `get_by_role("link", name=<person>)`
        matches on the team view whether or not the team table rendered at all —
        measured at three matches per name, only one of which is the table.
        An assertion built on that would pass against an empty team view.

        The table row is unique per member (measured: exactly one) and exists
        only if the table rendered, which is what a caller actually means.
        """
        return self.page.get_by_role("row").filter(has_text=display_name)
