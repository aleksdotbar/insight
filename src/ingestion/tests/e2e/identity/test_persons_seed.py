"""Contract: the persons-seed write path + the operation-tracking reads.

The seed streams ClickHouse `identity.identity_inputs` and rebuilds the
caller-tenant's persons / account_person_map / org_chart. It runs here under
its own SEED_TENANT (see lib/identity_seed.py) so the rebuild never touches
the fixture tree the read tests depend on. The module fixture provisions the
`identity.identity_inputs` table with a deterministic three-account roster:
two accounts sharing an email (one person, two bindings) + one solo account.

TRIGGER DIVERGENCE (#1690, accepted): the .NET service triggers the seed via
`POST /v1/persons-seed` (async queue + poll); the Rust successor REMOVED the
POST — the seed is CLI-only there (`identity-resolution seed`, run by the
Helm CronJob / a manual Job; synchronous) and only the GET journal routes
remain. Tests select the trigger through `_trigger_seed` and gate the
POST-specific cases on `supports_seed_http_trigger`; the CLI-specific cases
(input guards, advisory lock, exit codes) gate on `supports_seed_cli`. The
POST cases die with the .NET service.

The end-to-end case runs only where the implementation's ClickHouse reader
works against the harness's containerized ClickHouse — see
`lib.identity.supports_containerized_clickhouse`: the frozen .NET service's
Octonica native-protocol handshake deadlocks against every containerized CH
tried, so on `dotnet` that ONE case skips; the Rust implementation (HTTP
ClickHouse client) runs it — and that is the run that matters as cutover
acceptance.
"""

from __future__ import annotations

import time
import uuid

import pytest

from identity.contract import items_of
from lib import clickhouse
from lib import identity_seed as seed
from lib.config import SessionConfig

pytestmark = [pytest.mark.identity, pytest.mark.mutating]

SEED_SOURCE_ID = uuid.UUID("55555555-5555-5555-5555-555555555555")
SHARED_EMAIL = "seeded.person@e2e.test"
SOLO_EMAIL = "solo.person@e2e.test"

# A tenant nothing ever seeds successfully: `persons` never has rows under it,
# so the wrong-tenant guard must always refuse an unforced run for it.
GUARD_TENANT = uuid.UUID("66666666-6666-6666-6666-666666666666")

# Author the CLI stamps on its journal rows (no JWT on that path) — the
# SYSTEM_AUTHOR nil-UUID convention shared with the legacy Python seed.
SYSTEM_AUTHOR = "00000000-0000-0000-0000-000000000000"

_OPERATION_TIMEOUT_S = 120.0

_ROSTER: list[tuple[str, str, str]] = [
    # (account, value_type, value) — two accounts share SHARED_EMAIL.
    # Connectors emit a source-native `id` observation per account; the
    # profile's `ids[]` is built from exactly those.
    ("seed-acc-1", "email", SHARED_EMAIL),
    ("seed-acc-1", "id", "seed-acc-1"),
    ("seed-acc-1", "display_name", "Seeded Person"),
    ("seed-acc-2", "email", SHARED_EMAIL),
    ("seed-acc-2", "id", "seed-acc-2"),
    ("seed-acc-3", "email", SOLO_EMAIL),
    ("seed-acc-3", "id", "seed-acc-3"),
    ("seed-acc-3", "display_name", "Solo Person"),
]


def _fill_roster(cfg: SessionConfig) -> None:
    """TRUNCATE + INSERT the deterministic roster into identity_inputs."""
    clickhouse.execute(cfg, "TRUNCATE TABLE identity.identity_inputs")
    values = []
    for i, (account, value_type, value) in enumerate(_ROSTER):
        # Distinct _synced_at per row (production reality): the seed derives
        # observation created_at from it, and the persons UNIQUE key
        # (…, value_type, created_at) silently drops same-instant collisions —
        # e.g. two accounts' `id` observations for the same person.
        values.append(
            "("
            f"'{account}:{value_type}', "
            f"'{seed.SEED_TENANT}', 'e2e-source', '{SEED_SOURCE_ID}', "
            f"'{account}', '{value_type}', '{value}', "
            f"'UPSERT', now64(3) - INTERVAL {len(_ROSTER) - i} SECOND, {i + 1}"
            ")"
        )
    clickhouse.execute(
        cfg,
        "INSERT INTO identity.identity_inputs "  # noqa: S608 — every value is a fixed test literal above, no untrusted input
        "(unique_key, insight_tenant_id, insight_source_type, insight_source_id,"
        " source_account_id, value_type, value, operation_type, _synced_at, _version) VALUES "
        + ", ".join(values),
    )


@pytest.fixture(scope="module")
def identity_inputs(compose_stack: SessionConfig):
    """Create + fill `identity.identity_inputs` (schema mirrors the dbt model's
    reader-relevant columns; extra dbt bookkeeping columns included so the
    service's `SELECT` never meets a missing column)."""
    clickhouse.ensure_database(compose_stack, "identity")
    clickhouse.execute(
        compose_stack,
        """
        CREATE TABLE IF NOT EXISTS identity.identity_inputs (
            unique_key          String,
            insight_tenant_id   Nullable(String),
            insight_source_type String,
            insight_source_id   Nullable(String),
            source_account_id   Nullable(String),
            value_type          Nullable(String),
            value               Nullable(String),
            operation_type      String,
            _synced_at          DateTime64(3, 'UTC'),
            _version            UInt64
        ) ENGINE = ReplacingMergeTree(_version) ORDER BY unique_key
        """,
    )
    _fill_roster(compose_stack)
    return compose_stack


@pytest.fixture
def seed_api(identity_svc):
    """Client authenticated as the SEED_TENANT admin (see identity_seed)."""
    with identity_svc.client(sub=str(seed.SEED_ADMIN), tenant=str(seed.SEED_TENANT)) as c:
        yield c


def _trigger_seed(identity_svc, seed_api) -> str:
    """Trigger one seed run through the implementation's trigger and return
    its operation id. POST (async) on .NET; the `seed` CLI on Rust —
    synchronous, so the returned operation is already terminal there.

    The CLI run is `--force`: the fixture dataset lives under TEST_TENANT_ID
    while the seed runs under SEED_TENANT, which is exactly the wrong-tenant
    shape the guard exists for — the guard's own contract is proven by the
    unforced tests below.
    """
    if identity_svc.supports_seed_http_trigger:
        r = seed_api.post("/v1/persons-seed", json={"mode": "link-by-email"})
        assert r.status_code == 202, f"status={r.status_code} body={r.text}"
        return r.json()["operation_id"]
    res = identity_svc.run_seed_cli(tenant=str(seed.SEED_TENANT), force=True)
    assert res.returncode == 0, f"rc={res.returncode}\n{res.stdout}\n{res.stderr}"
    r = seed_api.get("/v1/persons-seed?limit=1")
    assert r.status_code == 200, f"status={r.status_code} body={r.text}"
    rows = items_of(r.json())
    assert rows, "a completed CLI run must be visible in the journal"
    return rows[0]["operation_id"]


@pytest.fixture
def seed_operation(identity_inputs, seed_api, identity_svc) -> str:
    """A freshly created seed operation's id — each dependent test owns its
    own operation instead of leaning on another test having run first."""
    return _trigger_seed(identity_svc, seed_api)


def _wait_completed(client, operation_id: str) -> dict:
    deadline = time.monotonic() + _OPERATION_TIMEOUT_S
    last: dict = {}
    while time.monotonic() < deadline:
        r = client.get(f"/v1/persons-seed/{operation_id}")
        assert r.status_code == 200, f"status={r.status_code} body={r.text}"
        last = r.json()
        if last.get("status") in {"completed", "failed"}:
            return last
        time.sleep(0.5)
    raise AssertionError(f"seed operation did not finish in {_OPERATION_TIMEOUT_S:.0f}s: {last}")


def test_persons_seed_end_to_end(identity_inputs, seed_api, identity_svc) -> None:
    """Seed run → operation completes → the seeded person resolves,
    with BOTH same-email accounts bound to one person."""
    if not identity_svc.supports_containerized_clickhouse:
        pytest.skip(
            "the .NET Octonica reader deadlocks against the harness's "
            "containerized ClickHouse (see module docstring); the Rust "
            "implementation runs this case"
        )
    operation_id = _trigger_seed(identity_svc, seed_api)

    op = _wait_completed(seed_api, operation_id)
    assert op["status"] == "completed", op
    summary = op.get("summary") or {}
    assert summary, op
    if identity_svc.supports_seed_cli:
        # CLI journal contract: system author (no JWT on that path) and the
        # request records the trigger.
        assert op["author_person_id"] == SYSTEM_AUTHOR, op
        request = op.get("request") or {}
        assert request.get("trigger") == "cli", op

    # Freshly minted person_ids come from the tenant-agnostic internal lookup
    # (no visibility gate — at this point NOBODY is in the seed admin's
    # subtree, so /v1/profiles would correctly answer 404 for the admin).
    with identity_svc.client(
        sub=str(seed.SEED_ADMIN), tenant=str(seed.SEED_TENANT), sub_type="service", roles="service"
    ) as svc:
        shared = svc.get(f"/internal/persons/by-email/{SHARED_EMAIL}")
        assert shared.status_code == 200, f"status={shared.status_code} body={shared.text}"
        shared_id = shared.json()["insight_source_id"]
        solo = svc.get(f"/internal/persons/by-email/{SOLO_EMAIL}")
        assert solo.status_code == 200, f"status={solo.status_code} body={solo.text}"
        # The solo account minted its own person.
        assert solo.json()["insight_source_id"] != shared_id

    # The two same-email accounts collapsed into ONE person — proven through
    # the read contract, resolved AS that person (a seeded top-of-tree sees
    # itself): the shared email resolves to a single profile (NOT ambiguous),
    # and the by-id/`ids[]` surface carries the CURRENT id observation per
    # source instance (the rn=1 reduction both implementations share) — for
    # two accounts in one source that is the newest one, seed-acc-2.
    with identity_svc.client(sub=shared_id, tenant=str(seed.SEED_TENANT)) as pc:
        by_email = pc.post("/v1/profiles", json={"value_type": "email", "value": SHARED_EMAIL})
        assert by_email.status_code == 200, f"status={by_email.status_code} body={by_email.text}"
        person = by_email.json()
        assert person["person_id"] == shared_id, person
        accounts = {entry["value"] for entry in person.get("ids") or []}
        assert accounts == {"seed-acc-2"}, person.get("ids")

        by_id = pc.post(
            "/v1/profiles",
            json={
                "value_type": "id",
                "value": "seed-acc-2",
                "insight_source_type": "e2e-source",
                "insight_source_id": str(SEED_SOURCE_ID),
            },
        )
        assert by_id.status_code == 200, f"status={by_id.status_code} body={by_id.text}"
        assert by_id.json()["person_id"] == shared_id, by_id.json()


def test_persons_seed_operations_listed(seed_operation, seed_api) -> None:
    """The list carries the operation THIS test created — order-independent,
    green on a fresh database, no reliance on the end-to-end test."""
    r = seed_api.get("/v1/persons-seed")
    assert r.status_code == 200, f"status={r.status_code} body={r.text}"
    ops = items_of(r.json())
    matching = [op for op in ops if op["operation_id"] == seed_operation]
    assert len(matching) == 1, ops
    assert matching[0]["operation_type"] == "persons-seed", matching[0]
    assert matching[0]["insight_tenant_id"] == str(seed.SEED_TENANT), matching[0]


def test_persons_seed_list_limit(seed_operation, seed_api, identity_svc) -> None:
    """With at least two operations present (the fixture's + one more),
    limit=1 returns exactly one — an empty list would mean the filter is
    vacuously 'passing'."""
    _trigger_seed(identity_svc, seed_api)
    r = seed_api.get("/v1/persons-seed?limit=1")
    assert r.status_code == 200, f"status={r.status_code} body={r.text}"
    rows = items_of(r.json())
    assert len(rows) == 1, rows
    assert rows[0]["insight_tenant_id"] == str(seed.SEED_TENANT), rows[0]


def test_persons_seed_list_status_filter(seed_operation, seed_api) -> None:
    """The status filter includes the created operation under its current
    status and excludes it under a status it can no longer hold.

    The lifecycle is one-way (queued → running → completed|failed), so the
    inclusion check retries until a status read and the filtered list agree
    (the operation may transition between the two GETs — on a fast CI worker
    it can cross two states in milliseconds; a CLI-triggered run is terminal
    already), and the exclusion check uses `queued`, which the operation can
    never re-enter once it was observed past it. No terminal state is
    required — the .NET worker may legitimately still be running (or, on
    macOS Docker Desktop, stuck — see the module docstring)."""
    deadline = time.monotonic() + 30.0
    while True:
        r = seed_api.get(f"/v1/persons-seed/{seed_operation}")
        assert r.status_code == 200, f"status={r.status_code} body={r.text}"
        current = r.json()["status"]
        included = items_of(seed_api.get(f"/v1/persons-seed?status={current}").json())
        if seed_operation in {op["operation_id"] for op in included}:
            break
        assert time.monotonic() < deadline, (
            f"status read and ?status= filter never agreed within 30s "
            f"(last read: {current}; filtered: {included})"
        )
        time.sleep(0.2)
    assert all(op["status"] == current for op in included), included

    if current != "queued":
        # One-way lifecycle: once past `queued` it can never be queued again,
        # so this exclusion cannot race with a transition.
        excluded = items_of(seed_api.get("/v1/persons-seed?status=queued").json())
        assert seed_operation not in {op["operation_id"] for op in excluded}, excluded


# ── POST trigger (dotnet-only; dies with the .NET service) ────────────────


def test_persons_seed_403_non_admin(bob_api, identity_svc) -> None:
    """bob is not an admin anywhere — the seed trigger is refused."""
    if not identity_svc.supports_seed_http_trigger:
        pytest.skip("POST /v1/persons-seed removed in the Rust successor (#1690)")
    r = bob_api.post("/v1/persons-seed", json={"mode": "link-by-email"})
    assert r.status_code == 403, f"status={r.status_code} body={r.text}"


def test_persons_seed_401_unauthenticated(anon_api, identity_svc) -> None:
    if not identity_svc.supports_seed_http_trigger:
        pytest.skip("POST /v1/persons-seed removed in the Rust successor (#1690)")
    assert anon_api.post("/v1/persons-seed", json={"mode": "link-by-email"}).status_code == 401


# ── CLI trigger (rust-only): guards, lock, exit codes (#1690) ─────────────


def _operation_row(cfg: SessionConfig, tenant: uuid.UUID) -> dict | None:
    """Newest `operations` row for a tenant, read straight from MariaDB —
    guard-refused tenants have no admin, so the HTTP journal is unreadable
    for them by design."""
    with seed._connection(cfg) as conn, conn.cursor() as cur:  # noqa: SLF001 — harness-internal helper
        cur.execute(
            "SELECT status, error_message, HEX(author_person_id) AS author"
            " FROM operations WHERE insight_tenant_id = %s"
            " ORDER BY started_at DESC LIMIT 1",
            (tenant.bytes,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    if isinstance(row, dict):
        return row
    status, error_message, author = row
    return {"status": status, "error_message": error_message, "author": author}


def test_seed_cli_wrong_tenant_guard(identity_inputs, identity_svc, compose_stack) -> None:
    """An unforced run for a tenant `persons` has never seen — while other
    tenants' rows exist — must refuse (exit 3) and journal the refusal:
    seeding would mint a parallel person set under a wrong tenant (#1550)."""
    if not identity_svc.supports_seed_cli:
        pytest.skip("the seed CLI exists only on the Rust implementation (#1690)")
    res = identity_svc.run_seed_cli(tenant=str(GUARD_TENANT), force=False)
    assert res.returncode == 3, f"rc={res.returncode}\n{res.stdout}\n{res.stderr}"

    op = _operation_row(compose_stack, GUARD_TENANT)
    assert op is not None, "the guard refusal must still write a journal row"
    assert op["status"] == "failed", op
    assert "tenant" in (op["error_message"] or ""), op


def test_seed_cli_empty_input_guard(identity_inputs, identity_svc, compose_stack) -> None:
    """An unforced run over an EMPTY identity_inputs must refuse (exit 3) —
    an empty read means a broken/misconfigured pipeline, not 'no people'."""
    if not identity_svc.supports_seed_cli:
        pytest.skip("the seed CLI exists only on the Rust implementation (#1690)")
    clickhouse.execute(compose_stack, "TRUNCATE TABLE identity.identity_inputs")
    try:
        res = identity_svc.run_seed_cli(tenant=str(seed.SEED_TENANT), force=False)
        assert res.returncode == 3, f"rc={res.returncode}\n{res.stdout}\n{res.stderr}"
        op = _operation_row(compose_stack, seed.SEED_TENANT)
        assert op is not None and op["status"] == "failed", op
        assert "identity_inputs" in (op["error_message"] or ""), op
    finally:
        # The module fixture fills once (module scope) — restore for the
        # tests that run after this one.
        _fill_roster(compose_stack)


def test_seed_cli_lock_busy(identity_inputs, identity_svc, compose_stack) -> None:
    """A run against a held per-tenant advisory lock fails fast with exit 2 —
    the serialization that replaced the in-process queue (cron-vs-manual and
    multi-instance overlap)."""
    if not identity_svc.supports_seed_cli:
        pytest.skip("the seed CLI exists only on the Rust implementation (#1690)")
    with seed._connection(compose_stack) as conn, conn.cursor() as cur:  # noqa: SLF001 — harness-internal helper
        cur.execute("SELECT GET_LOCK(%s, 0)", (f"persons-seed:{seed.SEED_TENANT}",))
        got = cur.fetchone()
        assert got and next(iter(got if isinstance(got, tuple) else got.values())) == 1, got
        try:
            res = identity_svc.run_seed_cli(tenant=str(seed.SEED_TENANT), force=True)
            assert res.returncode == 2, f"rc={res.returncode}\n{res.stdout}\n{res.stderr}"
        finally:
            cur.execute("SELECT RELEASE_LOCK(%s)", (f"persons-seed:{seed.SEED_TENANT}",))
