"""Idempotency + roster-scope tests for `identity.seed_login_ids`.

A stdlib `unittest` test against a minimal fake cursor, locking two
regressions:

1. Idempotency: migration 004 (`004_persons_relax_constraints.sql`) put
   `created_at` in `persons`' unique key, so `INSERT IGNORE` alone no longer
   dedupes a re-run (each insert gets a fresh `created_at`, so the unique key
   never collides). Every writer must check for an existing row explicitly.
2. Roster scope: fakeidp only defines a fixed dev-lead identity, but a
   Keycloak realm seeds the WHOLE roster (`keycloak_realm` pins every realm
   user's id to their own roster uuid) — `seed_login_ids` must seed a row per
   roster member under `AUTH_MODE=keycloak`, not just the dev lead.

Run against the installed package (see the README's develop section):

    uv run --extra dev python -m unittest discover -s tests -t .
"""

from __future__ import annotations

import os
import unittest
from typing import Any

from insight_seed import identity, profiles

_TENANT = "00000000-df51-5b42-9538-d2b56b7ee953"


def _roster() -> list[profiles.Person]:
    return [
        profiles.Person(
            uuid=profiles.DEV_LEAD_UUID,
            email="dev@company.nonpresent",
            team="development",
            role="lead",
            parent_uuid=profiles.CEO_UUID,
            first_name="Dev",
            last_name="Lead",
        ),
        profiles.Person(
            uuid=profiles.SALES_LEAD_UUID,
            email="sales-lead@company.nonpresent",
            team="sales",
            role="lead",
            parent_uuid=profiles.CEO_UUID,
            first_name="Sales",
            last_name="Lead",
        ),
    ]


class _FakeCursor:
    """Tracks which (person_id, external_id) pairs have been inserted.

    `seed_login_ids` runs one SELECT-then-maybe-INSERT per roster pair; this
    fake extracts the identifying `(person, external_id)` elements from each
    statement's params (they sit at different positions in the SELECT vs.
    INSERT param tuples — see `seed_login_ids`' `exists_sql`/`insert_sql`) so
    it can answer `fetchone()` per pair, not just a single global flag.
    """

    def __init__(self) -> None:
        self.insert_count = 0
        self.rowcount = 0
        self._existing: set[tuple[Any, Any]] = set()
        self._pending_result: tuple[int] | None = None

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        statement = sql.strip().upper()
        if statement.startswith("SELECT"):
            # exists_sql params: (tenant, person, source_type, source_id, external_id)
            key = (params[1], params[4])
            self._pending_result = (1,) if key in self._existing else None
        elif statement.startswith("INSERT"):
            # insert_sql params: (source_type, source_id, tenant, external_id, person, author, reason)
            key = (params[4], params[3])
            self._existing.add(key)
            self.insert_count += 1
            self.rowcount = 1
        else:
            raise AssertionError(f"unexpected SQL in seed_login_ids: {sql}")

    def fetchone(self) -> tuple[int] | None:
        return self._pending_result


class SeedLoginIdsTests(unittest.TestCase):
    """Both variables are SET, not defaulted: `profiles` reads them at call time,
    so a value left over from the developer's shell would otherwise decide which
    personas these tests expect."""

    _ENV = ("AUTH_MODE", "IDP_SOURCE_TYPE")

    def setUp(self) -> None:
        self._previous = {name: os.environ.get(name) for name in self._ENV}
        os.environ["IDP_SOURCE_TYPE"] = "fakeidp"

    def tearDown(self) -> None:
        for name, value in self._previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    def test_second_run_does_not_insert_a_duplicate(self) -> None:
        os.environ["AUTH_MODE"] = "fakeidp"
        cur = _FakeCursor()
        roster = _roster()

        # The fake cursor implements only what these writers call.
        first_run_count = identity.seed_login_ids(cur, _TENANT, roster)  # type: ignore[arg-type]
        second_run_count = identity.seed_login_ids(cur, _TENANT, roster)  # type: ignore[arg-type]

        self.assertEqual(first_run_count, 1, "fakeidp seeds only the dev lead")
        self.assertEqual(second_run_count, 0, "re-run must be a no-op, not a duplicate insert")
        self.assertEqual(cur.insert_count, 1, "only one INSERT should ever have executed")

    def test_keycloak_seeds_the_whole_roster(self) -> None:
        os.environ["AUTH_MODE"] = "keycloak"
        cur = _FakeCursor()
        roster = _roster()

        # The fake cursor implements only what these writers call.
        first_run_count = identity.seed_login_ids(cur, _TENANT, roster)  # type: ignore[arg-type]
        second_run_count = identity.seed_login_ids(cur, _TENANT, roster)  # type: ignore[arg-type]

        self.assertEqual(
            first_run_count,
            len(roster),
            "keycloak seeds every roster persona (keycloak_realm registers all of them)",
        )
        self.assertEqual(second_run_count, 0, "re-run must be a no-op for every pair")
        self.assertEqual(cur.insert_count, len(roster))


if __name__ == "__main__":
    unittest.main()
