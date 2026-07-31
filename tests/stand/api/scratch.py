"""Scratch resources: create through the API, delete before the test ends.

The stand is seeded once per run and is read-only by contract — reset by volume
teardown, never by TRUNCATE. Covering the write half of the API surface needs an
exception to that, and this is its exact shape:

1. A test may create rows **through the API**, and must delete them. Never a
   database connection: that would hand every test a back door around the
   deployed path, which is the only thing this suite exists to exercise.
2. Every created row carries `SCRATCH_PREFIX` in its name.
3. The metric **catalog** is out of bounds. It is the metric-coverage gate's
   universe, and a stand suite has no business editing it.
4. Teardown deletes are best-effort — a delete-case test has already removed its
   row, so a 404 there is expected rather than a failure.

Rule 2 exists to make rule 1 checkable. Every name is registered here, and
`conftest.py`'s session-scoped detector fails the run if any survives it. The
in-process rig needs no such check because it discards its whole stack; a stand
persists between runs, so a leak silently changes what the NEXT run sees. That
is the one place this suite should be stricter than the rig.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from typing import Final

from insight_stand import ApiClient, ApiResponse, JsonValue, analytics_path

#: Marks every row this suite creates, so a leak is identifiable on sight.
SCRATCH_PREFIX: Final[str] = "stand-scratch"

#: One token per session: a leak becomes attributable to the run that made it.
RUN_TAG: Final[str] = uuid.uuid4().hex[:8]

#: A query_ref the validator accepts (`SELECT … FROM db.table`, no WHERE) that
#: executes deterministically on ANY ClickHouse — `system.one` has exactly one
#: row — so `POST /v1/metrics/{id}/query` drives the real engine end to end.
SCRATCH_QUERY_REF: Final[str] = "SELECT 1 AS one FROM system.one"

#: A well-formed v7 UUID nothing claims, for the unknown-id 404 cases.
UNKNOWN_ID: Final[str] = "01900000-0000-7000-8000-000000000000"

#: Not a UUID, for the path-parse 400 cases: every `{id}` route binds
#: `Path<Uuid>`, whose deserialization failure is a 400 raised before any
#: handler logic runs.
NON_UUID: Final[str] = "not-a-uuid"

#: Names handed out this session, checked for survivors at the end.
_ISSUED: set[str] = set()


def scratch_name(tag: str) -> str:
    """A unique, greppable, attributable name — and register it for the sweep."""
    name = f"{SCRATCH_PREFIX}-{RUN_TAG}-{tag}-{uuid.uuid4().hex[:8]}"
    _ISSUED.add(name)
    return name


def issued_names() -> frozenset[str]:
    return frozenset(_ISSUED)


def _created(response: ApiResponse, what: str) -> dict[str, JsonValue]:
    assert response.status_code == 201, (
        f"create {what}: status={response.status_code} body={response.text[:300]}"
    )
    body = response.json()
    assert isinstance(body, dict), (
        f"create {what}: expected a JSON object, got {response.text[:300]}"
    )
    return body


def create_metric(client: ApiClient, tag: str) -> dict[str, JsonValue]:
    """`POST /v1/metrics` → 201. The caller soft-deletes it."""
    name = scratch_name(tag)
    response = client.post(
        analytics_path("/v1/metrics"),
        json_body={
            "name": name,
            "description": "stand endpoint-contract scratch metric",
            "query_ref": SCRATCH_QUERY_REF,
        },
    )
    body = _created(response, "metric")
    assert body["query_ref"] == SCRATCH_QUERY_REF
    return body


def create_saved_query(client: ApiClient, tag: str) -> dict[str, JsonValue]:
    """`POST /v1/queries` → 201. The caller hard-deletes it."""
    name = scratch_name(tag)
    response = client.post(
        analytics_path("/v1/queries"),
        json_body={
            "name": name,
            "description": "stand endpoint-contract scratch saved query",
            "sql": SCRATCH_QUERY_REF,
        },
    )
    body = _created(response, "saved query")
    assert body["sql"] == SCRATCH_QUERY_REF
    return body


def surviving_scratch_rows(client: ApiClient) -> list[str]:
    """Any scratch row this session created and failed to clean up."""
    if not _ISSUED:
        return []

    leaked: list[str] = []
    for listing in (analytics_path("/v1/metrics"), analytics_path("/v1/queries")):
        response = client.get(listing)
        if response.status_code != 200:
            continue
        body = response.json()
        items = body.get("items") if isinstance(body, dict) else None
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            name = item.get("name")
            if isinstance(name, str) and name in _ISSUED:
                leaked.append(f"{listing} -> {name}")
    return leaked


__all__: Sequence[str] = (
    "NON_UUID",
    "RUN_TAG",
    "SCRATCH_PREFIX",
    "SCRATCH_QUERY_REF",
    "UNKNOWN_ID",
    "create_metric",
    "create_saved_query",
    "issued_names",
    "scratch_name",
    "surviving_scratch_rows",
)
