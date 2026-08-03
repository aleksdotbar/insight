"""Shared helpers for the endpoint contract tests (`api/test_*.py`)."""

from __future__ import annotations

import uuid

# A query_ref the validator accepts (SELECT ... FROM db.table, no WHERE) that
# executes deterministically on ANY ClickHouse: system.one has exactly one row.
SCRATCH_QUERY_REF = "SELECT 1 AS one FROM system.one"

# Never-created v7 UUID for the unknown-id 404 cases.
UNKNOWN_ID = "01900000-0000-7000-8000-000000000000"

# A path segment that is not a UUID, for the 400 path-parse cases: every {id}
# route binds `Path<Uuid>`, whose deserialization failure is a 400
# (Axum `FailedToDeserializePathParams`) — before any handler logic runs.
NON_UUID = "not-a-uuid"


def text_body_request(client, method: str, url: str, body: str = "{}"):
    """Issue `method url` with a `text/plain` body so the JSON body extractor
    rejects it on Content-Type — pins the 415 unsupported-media-type contract.

    The body endpoints extract with plain `axum::Json`, so this 415 carries
    Axum's non-canonical plain-text envelope rather than an RFC 9457 Problem."""
    return client.request(method, url, content=body, headers={"Content-Type": "text/plain"})


def create_scratch_saved_query(client, name_prefix: str) -> dict:
    """POST a scratch saved query and return the created body (201 asserted).

    Reuses `SCRATCH_QUERY_REF` so `/run` executes deterministically on any
    ClickHouse. Callers own cleanup: `DELETE /v1/queries/{id}` (a hard delete)
    before the test ends so the row never leaks into `GET /v1/queries`.
    """
    r = client.post(
        "/v1/queries",
        json={
            "name": f"{name_prefix}-{uuid.uuid4().hex[:8]}",
            "description": "e2e endpoint-contract scratch saved query",
            "sql": SCRATCH_QUERY_REF,
        },
    )
    assert r.status_code == 201, f"create saved query: status={r.status_code} body={r.text}"
    body = r.json()
    assert body["sql"] == SCRATCH_QUERY_REF
    return body
