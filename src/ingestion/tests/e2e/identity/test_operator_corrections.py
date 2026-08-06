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
