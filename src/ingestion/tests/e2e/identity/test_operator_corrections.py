"""Operator corrections against the live service and its journal.

The behaviours here are DB-shaped — what `INSERT IGNORE` accepted, what the
journal holds afterwards, what a verb refuses — so they cannot be reached from
the Rust unit tests. Each test states one rule of the correction contract.
"""

from __future__ import annotations

import uuid

import httpx
import pymysql
import pytest
from lib import identity_seed
from lib.config import SessionConfig
from lib.identity import IDENTITY_DATABASE

from .contract import problem

pytestmark = [pytest.mark.identity, pytest.mark.mutating]


def _account(account_id: str) -> dict[str, object]:
    return {"source": identity_seed.SOURCE_TYPE, "source_id": str(identity_seed.SOURCE_ID), "id": account_id}


def _binding_rows(cfg: SessionConfig, account_id: str) -> list[tuple[str, str]]:
    """(person_id, author_person_id) of every binding observation for one
    account, newest first — read straight from the journal, not the API."""
    with (
        pymysql.connect(
            host=cfg.mariadb_host,
            port=cfg.mariadb_port,
            user=cfg.mariadb_user,
            password=cfg.mariadb_password,
            database=IDENTITY_DATABASE,
        ) as conn,
        conn.cursor() as cur,
    ):
        cur.execute(
            """
            SELECT LOWER(HEX(person_id)), LOWER(HEX(author_person_id))
            FROM persons
            WHERE value_type = 'id'
              AND insight_tenant_id = UNHEX(REPLACE(%s, '-', ''))
              AND insight_source_type = %s
              AND value_id = %s
            ORDER BY created_at DESC, id DESC
            """,
            (str(identity_seed.TEST_TENANT_ID), identity_seed.SOURCE_TYPE, account_id),
        )
        return [(row[0], row[1]) for row in cur.fetchall()]


def test_confirming_an_automatic_binding_records_the_operator(
    identity_svc, api: httpx.Client, compose_stack: SessionConfig
) -> None:
    """Binding an account to the person automation already gave it is the
    confirm act: the same person, now authored by a human. It must append a
    row — the operator's decision is what takes the account out of review —
    and only a REPEAT of that decision is a no-op."""
    account_id = identity_seed.ALICE_ACCOUNT_ID
    before = _binding_rows(compose_stack, account_id)
    assert before, "the fixture seeds an automatic binding for this account"

    confirm = api.post(
        "/v1/resolution/bind",
        json={
            "bindings": [{"account": _account(account_id), "person_id": str(identity_seed.ALICE)}],
            "comment": "e2e: confirm the automatic binding",
        },
    )
    assert confirm.status_code == 200, confirm.text
    assert confirm.json()["applied"] == 1, confirm.json()

    after = _binding_rows(compose_stack, account_id)
    assert len(after) == len(before) + 1, "the confirmation is a new observation"
    assert after[0][1] == identity_seed.ALICE.hex, "authored by the calling operator"

    repeat = api.post(
        "/v1/resolution/bind",
        json={
            "bindings": [{"account": _account(account_id), "person_id": str(identity_seed.ALICE)}],
            "comment": "e2e: repeat the same decision",
        },
    )
    assert repeat.status_code == 200, repeat.text
    assert repeat.json()["applied"] == 0, "repeating an operator decision writes nothing"
    assert repeat.json()["already_decided"] == 1, repeat.json()
    assert _binding_rows(compose_stack, account_id) == after, "no duplicate history"


@pytest.mark.parametrize("verb", ["detach", "exclude"])
def test_verbs_refuse_an_account_nothing_has_observed(identity_svc, api: httpx.Client, verb: str) -> None:
    """Pre-registration is a bind-only affordance: minting a person, or an
    excluded binding, for an account nobody has ever seen is a typo."""
    response = api.post(
        f"/v1/resolution/{verb}", json={"account": _account(f"ghost-{uuid.uuid4().hex[:8]}"), "comment": "e2e"}
    )

    assert response.status_code == 404, response.text
    problem(response)


def test_a_bulk_call_naming_one_account_twice_is_rejected(identity_svc, api: httpx.Client) -> None:
    """Which person wins is the caller's contradiction to resolve."""
    account = _account(identity_seed.ALICE_ACCOUNT_ID)
    response = api.post(
        "/v1/resolution/bind",
        json={
            "bindings": [
                {"account": account, "person_id": str(identity_seed.ALICE)},
                {"account": account, "person_id": str(identity_seed.BOB)},
            ],
            "comment": "e2e: contradictory bulk",
        },
    )

    assert response.status_code == 400, response.text
    problem(response)


def test_detach_moves_the_account_and_names_the_person_it_reached(
    identity_svc, api: httpx.Client, compose_stack: SessionConfig
) -> None:
    """Detach works on an account regardless of how its current grouping arose,
    and reports the person the account actually reached."""
    account_id = "acc-carol"  # a leaf of the fixture tree, safe to move
    before = _binding_rows(compose_stack, account_id)

    response = api.post("/v1/resolution/detach", json={"account": _account(account_id), "comment": "e2e: detach"})
    assert response.status_code == 200, response.text

    body = response.json()
    assert body["applied"] == 1, body
    new_person = body["new_person_id"]
    assert new_person, "a successful detach names the person it minted"

    after = _binding_rows(compose_stack, account_id)
    assert len(after) == len(before) + 1
    assert after[0][0] == uuid.UUID(new_person).hex, "the account reached the reported person"


def test_an_excluded_account_no_longer_resolves_at_login(
    identity_svc, api: httpx.Client, service_api: httpx.Client
) -> None:
    """Excluding an account makes it nobody everywhere: the login bootstrap
    must answer not-found rather than hand every excluded account the same
    shared sentinel identity."""
    account_id = f"acc-bot-{uuid.uuid4().hex[:8]}"

    bound = api.post(
        "/v1/resolution/bind",
        json={
            "bindings": [{"account": _account(account_id), "person_id": str(identity_seed.BOB)}],
            "comment": "e2e: pre-register the bot before excluding it",
        },
    )
    assert bound.status_code == 200, bound.text
    assert bound.json()["applied"] == 1, bound.json()

    resolves = service_api.get(
        "/internal/persons/by-external-id", params={"source_type": identity_seed.SOURCE_TYPE, "external_id": account_id}
    )
    assert resolves.status_code == 200, "a bound account resolves before the exclusion"

    excluded = api.post(
        "/v1/resolution/exclude", json={"account": _account(account_id), "comment": "e2e: not a person"}
    )
    assert excluded.status_code == 200, excluded.text
    assert excluded.json()["applied"] == 1, excluded.json()

    gone = service_api.get(
        "/internal/persons/by-external-id", params={"source_type": identity_seed.SOURCE_TYPE, "external_id": account_id}
    )
    assert gone.status_code == 404, gone.text
    problem(gone)


def test_the_queue_reports_items_and_rates_over_observed_accounts(identity_svc, api: httpx.Client) -> None:
    """The queue answers with the two things an operator needs: what to decide,
    and how much is already resolved. Every item names one of the three
    conditions, and the rates cover the accounts the evidence knows."""
    queue = api.get("/v1/resolution/attention")
    assert queue.status_code == 200, queue.text

    body = queue.json()
    assert "items" in body and "rates" in body, body
    rates = body["rates"]
    assert rates["observed"] >= rates["bound"] + rates["excluded"], rates
    for item in body["items"]:
        assert item["kind"] in {"contested", "binding_conflict", "no_evidence"}, item


def test_the_queue_honours_the_limit_without_narrowing_the_rates(identity_svc, api: httpx.Client) -> None:
    """`limit` truncates the items an operator is handed; the rates describe
    every observed account either way, so they must not move with it."""
    full = api.get("/v1/resolution/attention")
    assert full.status_code == 200, full.text

    capped = api.get("/v1/resolution/attention", params={"limit": 1})
    assert capped.status_code == 200, capped.text
    assert len(capped.json()["items"]) <= 1, capped.json()
    assert capped.json()["rates"] == full.json()["rates"], "the rates are not a page of the queue"


def _mint_person(api: httpx.Client, account_id: str) -> str:
    """A person nothing else in the suite owns: pre-register an account, then
    detach it into a person of its own. Returns the new `person_id`."""
    bound = api.post(
        "/v1/resolution/bind",
        json={
            "bindings": [{"account": _account(account_id), "person_id": str(identity_seed.BOB)}],
            "comment": "e2e: pre-register before minting",
        },
    )
    assert bound.status_code == 200, bound.text

    detached = api.post("/v1/resolution/detach", json={"account": _account(account_id), "comment": "e2e: mint"})
    assert detached.status_code == 200, detached.text
    return detached.json()["new_person_id"]


def test_merge_moves_every_account_of_the_absorbed_person(
    identity_svc, api: httpx.Client, compose_stack: SessionConfig
) -> None:
    """Merge is the whole-person verb: every account bound to the source ends
    up on the survivor, and the accounts keep their history rather than being
    rewritten."""
    absorbed_first = f"acc-merge-a-{uuid.uuid4().hex[:8]}"
    absorbed_second = f"acc-merge-b-{uuid.uuid4().hex[:8]}"
    source = _mint_person(api, absorbed_first)
    survivor = _mint_person(api, f"acc-merge-keep-{uuid.uuid4().hex[:8]}")

    # A second account on the source person, so the merge has to move more
    # than one and cannot pass by moving only the first.
    second = api.post(
        "/v1/resolution/bind",
        json={
            "bindings": [{"account": _account(absorbed_second), "person_id": source}],
            "comment": "e2e: second account of the absorbed person",
        },
    )
    assert second.status_code == 200, second.text

    history_before = _binding_rows(compose_stack, absorbed_first)

    merged = api.post(
        "/v1/resolution/merge",
        json={"source_person_id": source, "target_person_id": survivor, "comment": "e2e: one human"},
    )
    assert merged.status_code == 200, merged.text
    assert merged.json()["applied"] == 2, merged.json()

    for account_id in (absorbed_first, absorbed_second):
        rows = _binding_rows(compose_stack, account_id)
        assert rows[0][0] == uuid.UUID(survivor).hex, f"{account_id} did not reach the survivor"

    after = _binding_rows(compose_stack, absorbed_first)
    assert after[1:] == history_before, "the merge appended; it must not rewrite what was there"


def test_merge_refuses_a_person_that_is_not_two_people(identity_svc, api: httpx.Client) -> None:
    """Merging a person into themselves has no meaning, and neither side may
    be a person the tenant's journal has never heard of."""
    known = str(identity_seed.BOB)
    stranger = str(uuid.uuid4())

    same = api.post(
        "/v1/resolution/merge",
        json={"source_person_id": known, "target_person_id": known, "comment": "e2e: self-merge"},
    )
    assert same.status_code == 400, same.text
    problem(same)

    for label, body in (
        ("unknown source", {"source_person_id": stranger, "target_person_id": known}),
        ("unknown target", {"source_person_id": known, "target_person_id": stranger}),
    ):
        response = api.post("/v1/resolution/merge", json={**body, "comment": "e2e"})
        assert response.status_code == 404, f"{label}: {response.text}"
        problem(response)


def test_bind_refuses_a_person_the_tenant_never_had(identity_svc, api: httpx.Client) -> None:
    """A correction may not invent its target: binding to an unknown person is
    a typo, not a decision to record."""
    response = api.post(
        "/v1/resolution/bind",
        json={
            "bindings": [{"account": _account(identity_seed.ALICE_ACCOUNT_ID), "person_id": str(uuid.uuid4())}],
            "comment": "e2e: unknown person",
        },
    )

    assert response.status_code == 404, response.text
    problem(response)


def test_bind_refuses_an_empty_and_an_oversized_call(identity_svc, api: httpx.Client) -> None:
    """A bulk call carries a prepared matching table pasted by a human — no
    rows is a mistake, and past the cap it is not a paste any more."""
    empty = api.post("/v1/resolution/bind", json={"bindings": [], "comment": "e2e: nothing to do"})
    assert empty.status_code == 400, empty.text
    problem(empty)

    one_too_many = [
        {"account": _account(f"acc-bulk-{index}"), "person_id": str(identity_seed.BOB)} for index in range(1001)
    ]
    oversized = api.post("/v1/resolution/bind", json={"bindings": one_too_many, "comment": "e2e: too many"})
    assert oversized.status_code == 400, oversized.text
    problem(oversized)


def test_an_accounts_history_names_every_decision_and_its_author(
    identity_svc, api: httpx.Client, compose_stack: SessionConfig
) -> None:
    """The explain surface answers why an account belongs to whom: the binding
    in force plus each decision behind it, marked human or automatic."""
    account_id = f"acc-history-{uuid.uuid4().hex[:8]}"
    person = _mint_person(api, account_id)

    response = api.get(f"/v1/resolution/accounts/{identity_seed.SOURCE_TYPE}/{identity_seed.SOURCE_ID}/{account_id}")
    assert response.status_code == 200, response.text

    body = response.json()
    assert body["account_id"] == account_id, body
    assert body["person_id"] == person, "the binding in force is the newest decision"

    reasons = [entry["reason"] for entry in body["history"]]
    assert reasons == ["operator-detach", "operator-bind"], f"newest first, both verbs recorded: {reasons}"
    assert all(entry["by_operator"] for entry in body["history"]), body["history"]
    assert all(entry["author_person_id"] == str(identity_seed.ALICE) for entry in body["history"]), body["history"]


def test_an_unknown_accounts_history_is_empty_rather_than_missing(identity_svc, api: httpx.Client) -> None:
    """Asking about an account nobody has bound is a legitimate question with
    an empty answer — the read surface reports no binding, not not-found."""
    response = api.get(f"/v1/resolution/accounts/{identity_seed.SOURCE_TYPE}/{identity_seed.SOURCE_ID}/acc-never-seen")

    assert response.status_code == 200, response.text
    assert response.json()["person_id"] is None, response.json()
    assert response.json()["history"] == [], response.json()


def test_a_persons_accounts_are_listed_with_who_bound_them(identity_svc, api: httpx.Client) -> None:
    """The matching table for one person: every account bound to them, and
    whether a human or automation made each link."""
    account_id = f"acc-listed-{uuid.uuid4().hex[:8]}"
    person = _mint_person(api, account_id)

    response = api.get(f"/v1/resolution/persons/{person}/accounts")
    assert response.status_code == 200, response.text

    body = response.json()
    assert body["person_id"] == person, body
    listed = {entry["account_id"]: entry for entry in body["accounts"]}
    assert account_id in listed, body
    assert listed[account_id]["bound_by_operator"] is True, listed[account_id]


def test_a_person_with_no_accounts_lists_none(identity_svc, api: httpx.Client) -> None:
    """A person id the journal never bound an account to answers with an empty
    table, not an error — the question is well-formed."""
    response = api.get(f"/v1/resolution/persons/{uuid.uuid4()}/accounts")

    assert response.status_code == 200, response.text
    assert response.json()["accounts"] == [], response.json()


@pytest.mark.parametrize(
    "path",
    [
        "/v1/resolution/persons/not-a-uuid/accounts",
        f"/v1/resolution/accounts/{identity_seed.SOURCE_TYPE}/not-a-uuid/acc-alice",
    ],
    ids=["person_id", "source_id"],
)
def test_a_malformed_uuid_in_the_path_is_rejected(identity_svc, api: httpx.Client, path: str) -> None:
    """Both read paths carry a UUID segment; anything else never reaches the
    handler, so no query is built from an unparsed value."""
    response = api.get(path)

    assert response.status_code == 400, response.text


# Every route of the correction surface, with a body that parses — the
# authorization gate runs inside the handler, so a body the extractor would
# reject would prove nothing about the gate.
_ROUTES: list[tuple[str, str, dict[str, object] | None]] = [
    ("POST", "/v1/resolution/bind", {"bindings": [{"account": _account("acc-authz"), "person_id": str(uuid.uuid4())}]}),
    ("POST", "/v1/resolution/merge", {"source_person_id": str(uuid.uuid4()), "target_person_id": str(uuid.uuid4())}),
    ("POST", "/v1/resolution/detach", {"account": _account("acc-authz")}),
    ("POST", "/v1/resolution/exclude", {"account": _account("acc-authz")}),
    ("GET", "/v1/resolution/attention", None),
    ("GET", f"/v1/resolution/accounts/{identity_seed.SOURCE_TYPE}/{identity_seed.SOURCE_ID}/acc-authz", None),
    ("GET", f"/v1/resolution/persons/{identity_seed.BOB}/accounts", None),
]


@pytest.mark.parametrize(("method", "path", "body"), _ROUTES, ids=[f"{m} {p}" for m, p, _ in _ROUTES])
def test_every_route_refuses_a_caller_without_the_operator_grant(
    identity_svc, bob_api: httpx.Client, method: str, path: str, body: dict[str, object] | None
) -> None:
    """The whole surface is operator-only — reads included: the queue and the
    history describe who someone is, which is not for every authenticated
    caller to see."""
    response = bob_api.request(method, path, json=body)

    assert response.status_code == 403, response.text
    problem(response)


@pytest.mark.parametrize(("method", "path", "body"), _ROUTES, ids=[f"{m} {p}" for m, p, _ in _ROUTES])
def test_every_route_refuses_an_unauthenticated_caller(
    identity_svc, anon_api: httpx.Client, method: str, path: str, body: dict[str, object] | None
) -> None:
    """No token, no answer — before any authorization question is asked."""
    response = anon_api.request(method, path, json=body)

    assert response.status_code == 401, response.text
