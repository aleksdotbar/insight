"""The `/v1/queries` path group on analytics — saved queries and running them.

    GET    /v1/queries              200 list
    POST   /v1/queries              201 · 415 wrong-ct
    GET    /v1/queries/{id}         200 · 400 non-uuid · 404 unknown
    PUT    /v1/queries/{id}         200 · 404 unknown
    DELETE /v1/queries/{id}         204 · 404 unknown
    POST   /v1/queries/{id}/run     200 · 404 unknown · 415 wrong-ct

`/run` is the one that earns its place here rather than in the rig. It goes
gateway → analytics → ClickHouse in one request, so a green run means the whole
chain is wired: the session survived the edge, the tenant came out of the JWT,
and the query engine answered. The saved SQL returns a single deterministic row,
`{"one": 1}`, so the result can be asserted exactly instead of "something came
back".

The 401 half is in `test_gateway.py`, swept over every operation at once.
"""

from __future__ import annotations

from insight_stand import ApiClient, analytics_path

from ..schemas import RunResponse, SavedQuery, SavedQueryListResponse
from ..scratch import NON_UUID, SCRATCH_QUERY_REF, UNKNOWN_ID, create_saved_query

QUERIES = analytics_path("/v1/queries")


def _query_path(query_id: object, suffix: str = "") -> str:
    return analytics_path(f"/v1/queries/{query_id}{suffix}")


def _saved(api: ApiClient) -> set[str]:
    """Every saved-query name the listing reports, validated on the way through."""
    response = api.get(QUERIES)
    assert response.status_code == 200, f"status={response.status_code} {response.text[:300]}"
    return {item.name for item in response.parse(SavedQueryListResponse).items}


def test_list_queries_200(api: ApiClient, scratch_saved_query: SavedQuery) -> None:
    assert scratch_saved_query.name in _saved(api)


def test_saved_query_create_run_update_delete_round_trip(api: ApiClient) -> None:
    """One cycle: create → read → run → update → delete → gone.

    Asserted as a cycle rather than as six independent cases because that is
    what makes each half honest — a create that leaks its row and a delete that
    runs against a row it did not make are the two ways this coverage rots, and
    a single cycle can do neither.
    """
    created = create_saved_query(api, "roundtrip")
    query_id = created.id

    fetched = api.get(_query_path(query_id))
    assert fetched.status_code == 200, f"read back: {fetched.status_code} {fetched.text[:300]}"
    assert fetched.parse(SavedQuery).sql == SCRATCH_QUERY_REF

    ran = api.post(_query_path(query_id, "/run"), json_body={})
    assert ran.status_code == 200, f"run: {ran.status_code} {ran.text[:300]}"
    assert ran.parse(RunResponse).rows == [{"one": 1}], (
        f"the saved SQL should return exactly one deterministic row: {ran.text[:300]}"
    )

    updated = api.put(
        _query_path(query_id),
        json_body={
            "name": created.name,
            "description": "updated by the stand suite",
            "sql": SCRATCH_QUERY_REF,
        },
    )
    assert updated.status_code == 200, f"update: {updated.status_code} {updated.text[:300]}"

    deleted = api.delete(_query_path(query_id))
    assert deleted.status_code == 204, f"delete: {deleted.status_code} {deleted.text[:300]}"

    assert api.get(_query_path(query_id)).status_code == 404
    assert created.name not in _saved(api), "a hard-deleted saved query is still listed"


def test_create_query_415_wrong_content_type(api: ApiClient) -> None:
    response = api.post(QUERIES, content="{}", headers={"Content-Type": "text/plain"})
    assert response.status_code == 415, f"status={response.status_code} {response.text[:300]}"


def test_get_query_400_non_uuid(api: ApiClient) -> None:
    response = api.get(_query_path(NON_UUID))
    assert response.status_code == 400, f"status={response.status_code} {response.text[:300]}"


def test_get_query_404_unknown(api: ApiClient) -> None:
    response = api.get(_query_path(UNKNOWN_ID))
    assert response.status_code == 404, f"status={response.status_code} {response.text[:300]}"


def test_update_query_404_unknown(api: ApiClient) -> None:
    response = api.put(
        _query_path(UNKNOWN_ID),
        json_body={"name": "absent", "description": "x", "sql": SCRATCH_QUERY_REF},
    )
    assert response.status_code == 404, f"status={response.status_code} {response.text[:300]}"


def test_delete_query_404_unknown(api: ApiClient) -> None:
    response = api.delete(_query_path(UNKNOWN_ID))
    assert response.status_code == 404, f"status={response.status_code} {response.text[:300]}"


def test_run_query_404_unknown(api: ApiClient) -> None:
    response = api.post(_query_path(UNKNOWN_ID, "/run"), json_body={})
    assert response.status_code == 404, f"status={response.status_code} {response.text[:300]}"


def test_run_query_415_wrong_content_type(
    api: ApiClient, scratch_saved_query: SavedQuery
) -> None:
    """`/run` takes an OPTIONAL body, and still refuses one it cannot read.

    Optional is the reason to assert it separately from the create case above:
    a route that may be called with no body at all is the one where "ignore what
    I cannot parse" is a plausible implementation, and ignoring it would mean
    running the query with silently discarded parameters.

    An existing query on purpose — a 404 would satisfy a status-only assertion
    for the wrong reason, since the media type is checked before the lookup.
    """
    response = api.post(
        _query_path(scratch_saved_query.id, "/run"),
        content="{}",
        headers={"Content-Type": "text/plain"},
    )
    assert response.status_code == 415, f"status={response.status_code} {response.text[:300]}"
