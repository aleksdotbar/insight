"""`POST /v1/visible-persons` — filtering a list of emails to what the caller may see.

    POST /v1/visible-persons   200 · 415 wrong-ct

The one identity route that answers a question ABOUT visibility rather than
being governed by it, which makes it the sharpest place to state the rule: the
same caller, the same request, and membership decides each email separately.

Two filters run, and the test covers both. Emails resolve to persons within the
caller's TENANT, so somebody in another tenant is not a candidate at all; the
survivors are then narrowed to what the caller can see in the org chart. A
regression in either one leaks a different thing — the first would disclose that
an address exists somewhere in the product, the second who reports to whom — so
the assertion names both an out-of-tenant and an out-of-scope person rather than
treating "not visible" as one bucket.

The visible/out-of-scope pair is the same one `test_subchart.py` establishes
(`development_ic` in, `sales_ic` out), so the two routes are held to one story
about the seeded org rather than each inventing its own.
"""

from __future__ import annotations

import pytest
from insight_stand import ApiClient, Manifest, PersonaSession, identity_path

from ..schemas import VisiblePersons

VISIBLE_PERSONS = identity_path("/v1/visible-persons")

#: Nobody holds this. Present so the answer is shown to drop an address it
#: cannot resolve, rather than echoing back whatever it was handed.
UNKNOWN_EMAIL = "nobody@example.com"


@pytest.mark.requires_seed("dev_lead", "development_ic", "sales_ic", "other_tenant_lead")
def test_only_the_people_the_caller_may_see_come_back(
    lead_session: PersonaSession, stand_manifest: Manifest
) -> None:
    """One request, five emails, and each one decided on its own merits.

    Asserting the whole partition rather than a single membership: a route that
    returned everything it was given, or nothing, would satisfy any one-sided
    check, and both are plausible failures for a filter.
    """
    self_ = lead_session.email
    report = stand_manifest.fixture("development_ic").email
    outsider = stand_manifest.fixture("sales_ic").email
    other_tenant = stand_manifest.fixture("other_tenant_lead").email

    response = lead_session.client.post(
        VISIBLE_PERSONS,
        json_body={"emails": [self_, report, outsider, other_tenant, UNKNOWN_EMAIL]},
    )
    assert response.status_code == 200, f"status={response.status_code} {response.text[:300]}"

    visible = set(response.parse(VisiblePersons).visible)
    assert {self_, report} <= visible, (
        f"a lead cannot see themselves or their own report: {sorted(visible)}"
    )
    assert outsider not in visible, f"{outsider} is outside the lead's org scope"
    assert other_tenant not in visible, (
        f"{other_tenant} belongs to another tenant and is not even a candidate — "
        "returning them would cross the tenant boundary, not merely widen a scope"
    )
    assert UNKNOWN_EMAIL not in visible, "an unresolvable address was echoed back"


def test_visible_persons_415_wrong_content_type(api: ApiClient) -> None:
    """A body refused on its media type, not parsed."""
    response = api.post(
        VISIBLE_PERSONS, content='{"emails":[]}', headers={"Content-Type": "text/plain"}
    )
    assert response.status_code == 415, f"status={response.status_code} {response.text[:300]}"
