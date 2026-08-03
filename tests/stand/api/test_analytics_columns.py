"""`/v1/columns` — the drilldown column catalogue.

    GET /v1/columns           200
    GET /v1/columns/{table}   200 · 400 undecodable

**Asserted against an empty universe on this stand, and that is a real gap.**
`table_columns` has no write endpoint — it is operator- or migration-seeded in a
real deployment, and `deploy/seed` does not populate it — so both routes return
`{"items": []}` here and the per-table filter cannot be exercised against data.

The in-process rig fills the gap by inserting rows straight into MariaDB. This
suite deliberately does not: a database connection would hand every test a back
door around the deployed path, which is the only thing it exists to exercise.
Closing this properly means seeding `table_columns` in `deploy/seed` and naming
it in the manifest — see `out/endpoint-coverage-preconditions.md` (P6).

Until then these two cases prove the routes are reachable, authenticated and
correctly shaped, and nothing about filtering. Stated here rather than left for
a reader to infer from a test that looks thorough.

The 401 half is in `test_gateway.py`, swept over every operation at once.
"""

from __future__ import annotations

from insight_stand import ApiClient, analytics_path

from .schemas import EXTRACTOR_REJECTION_CONTENT_TYPE, ColumnListResponse

COLUMNS = analytics_path("/v1/columns")


def test_columns_listing_is_200(api: ApiClient) -> None:
    response = api.get(COLUMNS)
    assert response.status_code == 200, f"status={response.status_code} {response.text[:300]}"
    response.parse(ColumnListResponse)


def test_columns_for_a_table_is_200(api: ApiClient) -> None:
    """A table nothing has registered columns for still answers, with an empty list.

    Not a 404: the route reports what is registered for a table name, and
    "nothing" is an answer rather than a missing resource.
    """
    response = api.get(f"{COLUMNS}/gold_metric_values")
    assert response.status_code == 200, f"status={response.status_code} {response.text[:300]}"
    assert response.parse(ColumnListResponse).items == []


def test_columns_for_an_undecodable_table_is_400(api: ApiClient) -> None:
    """`%FF` is not valid UTF-8, so `{table}` never deserializes.

    This route's only 400: an unknown table name is an empty 200 (above), so a
    rejection can only come from the parameter failing to decode at all. Worth
    having precisely because the 200 makes every other bad name indistinguishable
    from a good one — this is the single input the route refuses.
    """
    response = api.get(f"{COLUMNS}/%FF")
    assert response.status_code == 400, f"status={response.status_code} {response.text[:300]}"
    assert response.content_type == EXTRACTOR_REJECTION_CONTENT_TYPE, (
        f"expected the extractor's plain-text rejection, got {response.content_type!r}: "
        f"{response.text[:300]}"
    )
