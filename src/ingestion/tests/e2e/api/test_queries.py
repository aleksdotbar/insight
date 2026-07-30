"""Contract: /v1/queries path group — saved-query CRUD + run (#1965).

  GET    /v1/queries              200 list · 200 excludes deleted
  POST   /v1/queries              201 · 400 bad-sql · 415 wrong-ct · 400 off-schema (xfail: #1670)
  GET    /v1/queries/{id}         200 · 400 non-uuid · 404 unknown · 404 deleted
  PUT    /v1/queries/{id}         200 · 400 bad-sql · 400 non-uuid · 404 unknown · 415 wrong-ct · 400 off-schema (xfail)
  DELETE /v1/queries/{id}         204 · 400 non-uuid · 404 unknown
  POST   /v1/queries/{id}/run     200 rows · 400 non-uuid · 404 unknown

The scratch query's `sql` runs the REAL read path end-to-end: gated by the
single-SELECT gate on write and run, then executed on ClickHouse as
`presentation_ro` — one deterministic row {one: 1} comes back. CRUD is a hard
delete (no soft-delete flag), so a deleted id is a plain 404.
"""

from __future__ import annotations

import pytest

from api.endpoint_helpers import NON_UUID, SCRATCH_QUERY_REF, UNKNOWN_ID, create_scratch_saved_query, text_body_request

pytestmark = pytest.mark.api


# ── POST /v1/queries ────────────────────────────────────────────────────────


def test_create_saved_query_201(api) -> None:
    """POST /v1/queries → 201 echoing the stored query (helper asserts the body)."""
    created = create_scratch_saved_query(api, "e2e-scratch-create")
    api.delete(f"/v1/queries/{created['id']}")


def test_create_saved_query_400_invalid_sql(api) -> None:
    """POST /v1/queries → 400: the single-SELECT gate rejects a non-read statement."""
    r = api.post("/v1/queries", json={"name": "e2e-bad", "sql": "DROP TABLE metrics"})
    assert r.status_code == 400, f"status={r.status_code} body={r.text}"


def test_create_saved_query_415_wrong_content_type(api) -> None:
    r = text_body_request(api, "POST", "/v1/queries")
    assert r.status_code == 415, f"status={r.status_code} body={r.text}"


@pytest.mark.xfail(reason="#1670: off-schema body should be canonical 400; legacy axum::Json returns 422", strict=True)
def test_create_saved_query_400_schema_mismatch(api) -> None:
    """Intended: `name` is a String, a numeric value is an off-schema body → 400."""
    r = api.post("/v1/queries", json={"name": 123, "sql": SCRATCH_QUERY_REF})
    assert r.status_code == 400, f"status={r.status_code} body={r.text}"


# ── GET /v1/queries ─────────────────────────────────────────────────────────


def test_list_saved_queries_200(api, scratch_saved_query: dict) -> None:
    """GET /v1/queries → 200 {items}: the scratch query is listed."""
    r = api.get("/v1/queries")
    assert r.status_code == 200, f"status={r.status_code} body={r.text}"
    assert scratch_saved_query["id"] in {q["id"] for q in r.json()["items"]}


def test_list_saved_queries_200_excludes_deleted(api, scratch_saved_query: dict) -> None:
    """GET /v1/queries → 200: a deleted query is not listed."""
    api.delete(f"/v1/queries/{scratch_saved_query['id']}")
    r = api.get("/v1/queries")
    assert r.status_code == 200, f"status={r.status_code} body={r.text}"
    assert scratch_saved_query["id"] not in {q["id"] for q in r.json()["items"]}


# ── GET /v1/queries/{id} ────────────────────────────────────────────────────


def test_get_saved_query_200(api, scratch_saved_query: dict) -> None:
    r = api.get(f"/v1/queries/{scratch_saved_query['id']}")
    assert r.status_code == 200, f"status={r.status_code} body={r.text}"
    assert r.json()["name"] == scratch_saved_query["name"]


def test_get_saved_query_400_non_uuid(api) -> None:
    """`{id}` binds `Path<Uuid>`; a non-UUID segment is a 400 before handler logic."""
    r = api.get(f"/v1/queries/{NON_UUID}")
    assert r.status_code == 400, f"status={r.status_code} body={r.text}"


def test_get_saved_query_404_unknown(api) -> None:
    r = api.get(f"/v1/queries/{UNKNOWN_ID}")
    assert r.status_code == 404, f"status={r.status_code} body={r.text}"


def test_get_saved_query_404_deleted(api, scratch_saved_query: dict) -> None:
    """A hard-deleted id is unreadable — same 404 as never-existed."""
    api.delete(f"/v1/queries/{scratch_saved_query['id']}")
    r = api.get(f"/v1/queries/{scratch_saved_query['id']}")
    assert r.status_code == 404, f"status={r.status_code} body={r.text}"


# ── PUT /v1/queries/{id} ────────────────────────────────────────────────────


def test_update_saved_query_200(api, scratch_saved_query: dict) -> None:
    """PUT /v1/queries/{id} → 200; absent fields (here `sql`) stay unchanged."""
    r = api.put(
        f"/v1/queries/{scratch_saved_query['id']}",
        json={"name": scratch_saved_query["name"] + "-renamed", "description": "updated"},
    )
    assert r.status_code == 200, f"status={r.status_code} body={r.text}"
    updated = r.json()
    assert updated["name"] == scratch_saved_query["name"] + "-renamed"
    assert updated["description"] == "updated"
    assert updated["sql"] == SCRATCH_QUERY_REF, "PUT must not reset fields it was not given"


def test_update_saved_query_400_invalid_sql(api, scratch_saved_query: dict) -> None:
    """PUT re-validates `sql` through the gate — a non-read statement is a 400."""
    r = api.put(f"/v1/queries/{scratch_saved_query['id']}", json={"sql": "INSERT INTO metrics VALUES (1)"})
    assert r.status_code == 400, f"status={r.status_code} body={r.text}"


def test_update_saved_query_400_non_uuid(api) -> None:
    r = api.put(f"/v1/queries/{NON_UUID}", json={"name": "x"})
    assert r.status_code == 400, f"status={r.status_code} body={r.text}"


def test_update_saved_query_404_unknown(api) -> None:
    r = api.put(f"/v1/queries/{UNKNOWN_ID}", json={"name": "nope"})
    assert r.status_code == 404, f"status={r.status_code} body={r.text}"


def test_update_saved_query_415_wrong_content_type(api, scratch_saved_query: dict) -> None:
    r = text_body_request(api, "PUT", f"/v1/queries/{scratch_saved_query['id']}")
    assert r.status_code == 415, f"status={r.status_code} body={r.text}"


@pytest.mark.xfail(reason="#1670: off-schema body should be canonical 400; legacy axum::Json returns 422", strict=True)
def test_update_saved_query_400_schema_mismatch(api, scratch_saved_query: dict) -> None:
    """Intended: `name` is `Option<String>`, a numeric value is off-schema → 400."""
    r = api.put(f"/v1/queries/{scratch_saved_query['id']}", json={"name": 123})
    assert r.status_code == 400, f"status={r.status_code} body={r.text}"


# ── DELETE /v1/queries/{id} ─────────────────────────────────────────────────


def test_delete_saved_query_204(api, scratch_saved_query: dict) -> None:
    r = api.delete(f"/v1/queries/{scratch_saved_query['id']}")
    assert r.status_code == 204, f"status={r.status_code} body={r.text}"


def test_delete_saved_query_400_non_uuid(api) -> None:
    r = api.delete(f"/v1/queries/{NON_UUID}")
    assert r.status_code == 400, f"status={r.status_code} body={r.text}"


def test_delete_saved_query_404_unknown(api) -> None:
    """Hard delete is not idempotent: an unknown id is a 404, not a no-op."""
    r = api.delete(f"/v1/queries/{UNKNOWN_ID}")
    assert r.status_code == 404, f"status={r.status_code} body={r.text}"


# ── POST /v1/queries/{id}/run ───────────────────────────────────────────────


def test_run_saved_query_200(api, scratch_saved_query: dict) -> None:
    """POST /v1/queries/{id}/run → 200 with the deterministic system.one row,
    executed read-only as presentation_ro."""
    r = api.post(f"/v1/queries/{scratch_saved_query['id']}/run")
    assert r.status_code == 200, f"status={r.status_code} body={r.text}"
    assert r.json()["rows"] == [{"one": 1}]


def test_run_saved_query_400_non_uuid(api) -> None:
    r = api.post(f"/v1/queries/{NON_UUID}/run")
    assert r.status_code == 400, f"status={r.status_code} body={r.text}"


def test_run_saved_query_404_unknown(api) -> None:
    r = api.post(f"/v1/queries/{UNKNOWN_ID}/run")
    assert r.status_code == 404, f"status={r.status_code} body={r.text}"
