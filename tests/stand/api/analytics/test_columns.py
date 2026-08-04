"""`/v1/columns` — the drilldown column catalogue.

    GET /v1/columns           200 · lists every catalogued column
    GET /v1/columns/{table}   200 · filtered to that table · 400 undecodable

`table_columns` has no write endpoint — a deployment provisions it by operator
or migration — so nothing a test can call creates a row, and this suite holds no
database connection to insert one with (that would be a back door around the
deployed path it exists to exercise). `deploy/seed/analytics.py` seeds the rows
instead and the manifest names them, so the per-table filter is asserted against
data rather than vacuously against an empty universe.

Two tables, not one: a filter is only exercised when asking for A can be shown
NOT to return B's columns. A single catalogued table would pass against a
handler that ignores the parameter entirely.

The 401 half is in `test_gateway.py`, swept over every operation at once.
"""

from __future__ import annotations

import pytest
from insight_stand import ApiClient, Manifest, analytics_path

from ..schemas import EXTRACTOR_REJECTION_CONTENT_TYPE, ColumnListResponse

COLUMNS = analytics_path("/v1/columns")


def test_columns_listing_is_200(api: ApiClient) -> None:
    response = api.get(COLUMNS)
    assert response.status_code == 200, f"status={response.status_code} {response.text[:300]}"
    response.parse(ColumnListResponse)


@pytest.mark.requires_catalogue("table_columns")
def test_columns_listing_carries_every_catalogued_column(
    api: ApiClient, stand_manifest: Manifest
) -> None:
    """The unfiltered listing serves what the seed catalogued.

    Superset, not equality: the rows are seeded tenant-less (platform-visible),
    and a deployment is free to have catalogued more. Asserting the exact set
    would fail on a stand that is merely richer than this one.
    """
    response = api.get(COLUMNS)
    assert response.status_code == 200, f"status={response.status_code} {response.text[:300]}"

    served = {
        (item.clickhouse_table, item.field_name)
        for item in response.parse(ColumnListResponse).items
    }
    expected = {(row.table, row.field) for row in stand_manifest.catalogue.table_columns}
    assert expected <= served, (
        f"the catalogue is missing rows the seed wrote: {sorted(expected - served)} "
        f"(served {sorted(served)})"
    )


@pytest.mark.requires_catalogue("table_columns")
def test_columns_for_a_table_are_filtered_to_it(api: ApiClient, stand_manifest: Manifest) -> None:
    """Asking for one catalogued table does not return the other's columns."""
    catalogued = stand_manifest.catalogue.table_columns
    assert len(catalogued) >= 2, (
        "the filter needs two catalogued tables to be provable; the manifest names "
        f"{[row.table for row in catalogued]}"
    )
    first, second = catalogued[0], catalogued[1]

    response = api.get(f"{COLUMNS}/{first.table}")
    assert response.status_code == 200, f"status={response.status_code} {response.text[:300]}"

    served = {item.field_name for item in response.parse(ColumnListResponse).items}
    assert first.field in served, (
        f"{first.table} did not serve its own column {first.field!r}: {served}"
    )
    assert second.field not in served, (
        f"asking for {first.table} returned {second.table}'s column {second.field!r} — "
        f"the per-table filter is not applied: {served}"
    )


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
