"""Two contracts every analytics route shares, asserted once over all of them.

    {id}-taking routes   400 when the segment is not a UUID
    body-taking routes   415 when the body arrives as text/plain

Both are properties of the ROUTE TABLE rather than of any handler: every `{id}`
binds `Path<Uuid>`, whose deserialization failure is a 400 before handler logic
runs, and every body extractor refuses on media type before it parses. The rig
states each one per endpoint, which is thirty near-identical tests and a list
that silently stops matching the router. Here the list IS the assertion — a
route added to `operations.py` and not to the table below is visible as an
absence in one place.

Ordering is the substance of both. A 400 that arrived as a 404 would mean the
path parsed and the lookup ran, and a 415 that arrived as a 422 would mean the
body was read before its media type was checked. Neither is visible from the
status code alone, which is why the tables pin the code that must come FIRST.

Worth asserting through a real gateway specifically: a proxy that rewrote or
dropped `Content-Type` would turn every 415 below into a 422 or a 2xx, and an
in-process rig cannot see that happen.
"""

from __future__ import annotations

import pytest
from insight_stand import ApiClient, analytics_path

from .. import scratch

#: `{id}` routes, with the offending segment already substituted. Written out
#: rather than generated from `operations.py`: the point is to state which
#: segment is under test, and a route with two ids is two different claims.
NON_UUID_ROUTES: tuple[tuple[str, str], ...] = (
    ("GET", f"/v1/metrics/{scratch.NON_UUID}"),
    ("PUT", f"/v1/metrics/{scratch.NON_UUID}"),
    ("DELETE", f"/v1/metrics/{scratch.NON_UUID}"),
    ("POST", f"/v1/metrics/{scratch.NON_UUID}/query"),
    ("GET", f"/v1/metrics/{scratch.NON_UUID}/thresholds"),
    ("POST", f"/v1/metrics/{scratch.NON_UUID}/thresholds"),
    ("PUT", f"/v1/metrics/{scratch.NON_UUID}/thresholds/{scratch.UNKNOWN_ID}"),
    ("PUT", f"/v1/metrics/{scratch.UNKNOWN_ID}/thresholds/{scratch.NON_UUID}"),
    ("DELETE", f"/v1/metrics/{scratch.NON_UUID}/thresholds/{scratch.UNKNOWN_ID}"),
    ("DELETE", f"/v1/metrics/{scratch.UNKNOWN_ID}/thresholds/{scratch.NON_UUID}"),
    ("GET", f"/v1/admin/metric-thresholds/{scratch.NON_UUID}"),
    ("PUT", f"/v1/admin/metric-thresholds/{scratch.NON_UUID}"),
    ("DELETE", f"/v1/admin/metric-thresholds/{scratch.NON_UUID}"),
    ("GET", f"/v1/queries/{scratch.NON_UUID}"),
    ("PUT", f"/v1/queries/{scratch.NON_UUID}"),
    ("DELETE", f"/v1/queries/{scratch.NON_UUID}"),
    ("POST", f"/v1/queries/{scratch.NON_UUID}/run"),
)

#: Every route that reads a body. Ids are well-formed-but-unknown on purpose:
#: the path must parse, so that what the response reports is the media type and
#: not the segment.
BODY_ROUTES: tuple[tuple[str, str], ...] = (
    ("POST", "/v1/metrics"),
    ("PUT", f"/v1/metrics/{scratch.UNKNOWN_ID}"),
    ("POST", f"/v1/metrics/{scratch.UNKNOWN_ID}/query"),
    ("POST", "/v1/metrics/queries"),
    ("POST", f"/v1/metrics/{scratch.UNKNOWN_ID}/thresholds"),
    ("PUT", f"/v1/metrics/{scratch.UNKNOWN_ID}/thresholds/{scratch.UNKNOWN_ID}"),
    ("POST", "/v1/admin/metric-thresholds"),
    ("PUT", f"/v1/admin/metric-thresholds/{scratch.UNKNOWN_ID}"),
    ("POST", "/v1/queries"),
    ("PUT", f"/v1/queries/{scratch.UNKNOWN_ID}"),
    ("POST", f"/v1/queries/{scratch.UNKNOWN_ID}/run"),
    ("POST", "/v1/catalog/get_metrics"),
    ("POST", "/v1/metric-results"),
    ("POST", "/v1/metric-drilldown"),
    ("POST", "/v1/metric-drilldown/export"),
)


def _id(value: str) -> str:
    return value


@pytest.mark.parametrize(("method", "suffix"), NON_UUID_ROUTES, ids=_id)
def test_a_non_uuid_path_segment_is_400(api: ApiClient, method: str, suffix: str) -> None:
    """Rejected by the path parser, before any lookup.

    404 here would be the wrong answer twice over: it would mean the router
    accepted a segment that cannot be an id, and it would report a miss for a
    row nobody could have named.
    """
    response = api.request(method, analytics_path(suffix))
    assert response.status_code == 400, (
        f"{method} {suffix} answered {response.status_code} for a segment that is not a "
        f"UUID: {response.text[:300]}"
    )


@pytest.mark.parametrize(("method", "suffix"), BODY_ROUTES, ids=_id)
def test_a_body_with_the_wrong_media_type_is_415(
    api: ApiClient, method: str, suffix: str
) -> None:
    """Refused on `Content-Type`, not parsed and then judged.

    The body is valid JSON, so anything but 415 means the extractor read it
    before checking how it was labelled — 422 would say it was parsed against
    the schema, 2xx that it was accepted outright.
    """
    response = api.request(
        method, analytics_path(suffix), content="{}", headers={"Content-Type": "text/plain"}
    )
    assert response.status_code == 415, (
        f"{method} {suffix} answered {response.status_code} to a text/plain body: "
        f"{response.text[:300]}"
    )
