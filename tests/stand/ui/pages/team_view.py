"""A lead's team view — `/ic/$person_id/team`. Locators and navigation only.

`$person_id` is the canonical person UUID since the identity cutover (#2098),
the same key `PersonView` uses. Keying it on the email sent the SPA to a
route it could not resolve, and it rendered the PERSONAL view instead — a
redirect, not an error, which is why the failure read as a missing table
rather than as a bad URL.
"""

from __future__ import annotations

from urllib.parse import quote

from playwright.sync_api import Locator, Page


class TeamView:
    def __init__(self, page: Page) -> None:
        self.page = page

    @staticmethod
    def path(person_id: str) -> str:
        return f"/ic/{quote(person_id, safe='')}/team"

    def go(self, person_id: str) -> None:
        self.page.goto(self.path(person_id), wait_until="domcontentloaded")

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
