# The compose-stand suite

Deployed-stand tests for Insight: a real Keycloak login, four browser
journeys against the SPA, an API-contract suite, and a metrics harness — all
run against a local `docker-compose` stand seeded deterministically for
tests (`deploy/seed`).

This suite assumes an **already-running, already-seeded** stand. It never
starts compose, applies migrations, or spawns service processes itself —
see `conftest.py`'s module docstring for why that split matters. The shared
library it imports (`insight_stand`, session handling, the manifest reader,
persona resolution) lives in `../lib`; both are one uv project
(`tests/pyproject.toml`).

## Layout

| Path | Contents |
|---|---|
| `conftest.py` | Session wiring: manifest loading, `--stand-manifest`, the `session_for` persona factory, `requires_seed` / capability-marker enforcement. |
| `api/` | HTTP contract tests — no browser. |
| `ui/` | The four browser journeys, plus `ui/pages/` (page objects). |
| `metrics/` | The golden-metrics harness. |

## Running it locally

The stand and the suite are both driven through `./dev-compose.sh
test-stand`, from the repository root:

```bash
./dev-compose.sh test-stand up              # bring the stand up, seed it, wait for dbt
./dev-compose.sh test-stand test             # run the whole suite (uv, on the host)
./dev-compose.sh test-stand test tests/stand/api/    # a subset — args pass through to pytest
./dev-compose.sh test-stand down             # stop the stand and drop its volumes
```

`test-stand test` requires [`uv`](https://docs.astral.sh/uv/) on the host
and runs `uv run --project tests --frozen pytest tests/stand <args>`. Run
pytest directly the same way if you prefer:

```bash
uv sync --project tests
uv run --project tests playwright install chromium   # first time only, for ui/
uv run --project tests pytest tests/stand
```

To run the suite the way CI does — inside the published `ui-tests` image,
against the gateway's own network namespace — pass `--image`:

```bash
./dev-compose.sh test-stand test --image ghcr.io/constructorfabric/insight-ui-tests:latest
```

That mode never builds the image; pull it first. See `dev-compose.sh`'s
`cmd_test_stand_help` for the full verb reference, including
`--auth`/`--base-url`/`--stand-manifest` overrides for pointing the suite at
a stand other than the one it just brought up.

## Reading PROFILE.md before writing a test

[`deploy/seed/PROFILE.md`](../../deploy/seed/PROFILE.md) is generated from
the same builder that writes the stand's `manifest.json`, so the two cannot
disagree. Before adding a test, read it for:

- **the roster and fixtures** — the `fixtures{}` table is the set of stable,
  role-shaped names (`dev_lead`, `admin_operator`, …) a test may declare
  against; a raw email or UUID is never a stable target.
- **populated / golden metrics** — as of this suite, that table is empty by
  design (see `deploy/seed/golden_metrics.py`'s admission criteria). A test
  asserting a metric's exact value has nothing to read yet; check this table
  before writing one so you don't duplicate that gap or assert a value the
  seed cannot yet promise.
- **capabilities** — e.g. `ingestion`, which this stand does not have
  (compose seeds silver/gold directly). A test that needs a capability the
  stand may lack should carry the matching marker (below), not assume it.

Regenerate it with `python3 deploy/seed/render_profile.py` after changing
the roster or the manifest builder; `--check` verifies it without a
database.

## Declaring required seed data

A test that depends on a specific person or role being present declares it
with `@pytest.mark.requires_seed(*fixture_names)`:

```python
@pytest.mark.requires_seed("dev_lead", "development_ic")
def test_something(session_for):
    lead = session_for("dev_lead")
    ...
```

Every name is checked against the manifest's `fixtures{}` catalog at
**collection time** — before any test runs. A stand seeded without a name a
collected test needs aborts the whole run once, listing every missing name
and every test that needed it, rather than failing tests one at a time as
they happen to run. `requires_ingestion` works the same way for a
capability rather than a person: a test carrying it is **skipped** (not
failed) with a reason, on a stand whose manifest does not declare that
capability.

Both markers are registered in `tests/pyproject.toml`
(`[tool.pytest.ini_options] markers`, with `--strict-markers`) — that is the
one place to look if a new capability marker needs adding.

## UI vs API: the governing rule

**A new UI test must state, in writing, why it cannot instead be an API
test.** A browser is slow, flaky relative to an HTTP call, and exercises
more surface than most assertions need — so the suite would rather have one
more API test than one more browser test whenever the two would prove the
same thing.

State the reason as a paragraph in the test module's docstring, in the
shape the four shipped journeys already use — for example
`ui/test_logged_out_access_refused.py`:

> Why this is a browser test and not an API test, measured rather than
> asserted: every SPA route answers 200 text/html to an anonymous HTTP
> client... Refusal exists only inside the browser: the SPA boots, asks
> `/auth/me` (401), and the root route's `beforeLoad` sends the window to
> `/auth/login`...

A justification backed by something you measured (a cookie only a real
browser can set, a client-side route change no HTTP call performs, a
redirect chain that only exists post-render) is what this rule is asking
for — not an assertion that the UI "should" be covered too.

## What this suite does not cover

As of this writing: no metric value is asserted (the golden set is empty by
design, above); no accessibility or contrast checking; cross-tenant
isolation and the service-principal route are left to the in-process
`bronze-to-api` rig; `/v1/columns` is asserted only against an empty
universe (the seed does not populate `table_columns`).
