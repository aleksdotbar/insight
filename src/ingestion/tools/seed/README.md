# Insight sample-data seeder

Populates a stand with a 25-person demo organisation (4 teams + CEO) and
per-team activity in ClickHouse silver tables. `profiles.py` documents the
roster and the per-team source-type weights; the per-domain generators under
`generators/` document the row shapes they emit. See
[PROFILE.md](PROFILE.md) for what a freshly seeded stand actually contains
— roster, fixtures, populated metrics and capabilities.

It runs against two kinds of stand, from the same sources: the local
docker-compose stack, and a chart-deployed Kubernetes stand (the package ships
inside the toolbox image, so no separate image or build is involved).

This package lives inside `src/ingestion` deliberately: the silver step runs the
ingestion tree's own DDL and gold-build scripts, and being in the same tree means
the published toolbox image carries both, at one version, with no chance of the
seeder and the migration SQL drifting apart.

## Run it on compose

The stack must be up first (`./dev-compose.sh up`). Then:

```bash
./dev-compose.sh seed                       # everything
./dev-compose.sh seed identity              # just identity
./dev-compose.sh seed silver                # just silver
```

A successful run writes `manifest.json` next to this README, describing the
stand it just produced (roster, fixtures, data window, capabilities).

## Run it on a Kubernetes stand

```bash
export KUBECONFIG=<your stand's kubeconfig>
./src/ingestion/tools/seed/seed-stand.sh -n <namespace> --email you@example.com
```

That renders [`seed-job.yaml.tpl`](seed-job.yaml.tpl) into a one-shot Job, applies
it, and follows the logs. Every coordinate comes from the stand itself, so there
is no manifest to hand-edit and no tenant UUID to copy:

| Value | Read from |
|-------|-----------|
| MariaDB + ClickHouse host, port, user | ConfigMap `<release>-platform` |
| database holding the analytics catalogue | ConfigMap `<release>-platform`, `MARIADB_DATABASE` |
| database holding `persons` | Secret `insight-identity-resolution-config`, `…database_url` |
| the stand's tenant | Secret `insight-identity-resolution-config`, `…tenant_default_id` |
| the image to run | `helm get values <release>`, `ingestion.toolboxImage` |
| passwords | never read — the Job references Secret `insight-db-creds` by key |

Credentials never pass through the script, and the Job runs as the application
MariaDB user rather than root: the umbrella already grants that user everything
the seed writes.

Useful flags — `--dry-run` prints the rendered Job instead of applying it,
`--step identity|silver|analytics` runs one step (identity alone needs no
ClickHouse and finishes in seconds), `--tenant` seeds a tenant of your choosing,
and `--days` / `--anchor` pin the activity window. `--help` lists the rest.

Anything the script cannot discover is a hard error naming the flag that supplies
it — it never falls back to a guess.

### Two prerequisites it cannot satisfy for you

1. **A user with `--email` must already exist in the stand's IdP.** The
   authenticator resolves people by the email claim, so the seeded dev-lead
   persona is only reachable by a login that already authenticates. Create the
   user in the realm first, or point `--email` at one that exists.
2. **The stand's ClickHouse schema must exist** before `--step silver`, i.e. the
   chart's `clickhouse-migrate` hook has run at least once. The step re-applies
   the placeholder DDL and rebuilds gold, but it does not stand in for the
   release's own migration path.

### It refuses rather than making a mess

Preflight runs before anything is written and reports every problem at once:

- `TENANT_DEFAULT_ID` missing or not a UUID — rows under the wrong tenant are
  invisible to every login while the run still reports success;
- the named analytics database does not hold `metric_definitions` — the error
  names the database it looked in;
- MariaDB or ClickHouse unreachable, or the ingestion scripts missing;
- the target tenant already holds `persons` rows this seeder did not write
  (every row it writes carries a `reason` starting `seed.py `);
- any table the silver step clears holds rows for another tenant — that step
  **TRUNCATEs every table it writes**, across all tenants, so those rows would be
  destroyed. This is the one genuinely destructive thing the seeder does, and it
  is why an occupied stand is refused rather than merged into. The surface it
  checks is `generators.base.RESET_TARGETS`, the same list `truncate` itself
  enforces — including two inputs outside the silver database (an
  identity-projection table and a bronze HR table). Targets carrying no tenant
  column at all cannot be attributed to anyone; the run logs them by name
  instead of pretending to have judged them.

Either refusal is overridable with `--force`, which is how you say "yes, clear
it" out loud.

The Job carries `backoffLimit: 0`: a failed seed is kept for reading rather than
retried, and because it is a plain Job rather than a chart hook, a failure never
touches the release or triggers a rollback.

## Reproducing a dataset

`SEED_ANCHOR_DATE` fixes the last day carrying activity; `SEED_DAYS` sets the
window length. Pin both to reproduce a dataset exactly:

```bash
SEED_ANCHOR_DATE=2026-06-30 SEED_DAYS=60 ./dev-compose.sh seed
```

Unset (or the literal `today`), the anchor is yesterday UTC, so the developer
loop stays populated as the calendar moves. Whichever applied is recorded in
`manifest.json`, so a stand always reports how to recreate it.

## [PROFILE.md](PROFILE.md)

[`PROFILE.md`](PROFILE.md) is generated and committed. Regenerate it after any change to the
roster or the manifest builder:

```bash
cd src/ingestion/tools/seed
python3 -m insight_seed.render_profile            # regenerate
python3 -m insight_seed.render_profile --check    # verify (no database needed)
```

## Develop on it

```bash
cd src/ingestion/tools/seed
python3 -m venv .venv                              # one-time
.venv/bin/pip install -e '.[dev]'

.venv/bin/ruff check .                             # package + tests
.venv/bin/mypy .
python3 -m unittest discover -s tests -t .         # stdlib only, no database
```

The tests need nothing installed: they stub the database drivers and exercise
the pure half — the environment contract, the SQL a guard issues, and the
messages a refusal carries.

Deps live in `pyproject.toml`: `[project.dependencies]` for runtime,
`[project.optional-dependencies].dev` for the tooling (ruff, mypy, stubs).

## Layout

Code and tests are separate trees, and the artifacts the package produces sit
at the root beside this README — where their readers (the stand suite, the
compose bind mount) name them.

```text
src/ingestion/tools/seed/
├── insight_seed/            the package — everything importable
│   ├── __main__.py          `python3 -m insight_seed <step>`: the entry point
│   ├── config.py            environment contract: required, defaulted, and why
│   ├── preflight.py         refuses a stand that cannot take the seed
│   ├── identity.py          MariaDB: persons, org_chart, account_person_map
│   ├── silver.py            ClickHouse: placeholders → generators → gold build
│   ├── analytics.py         the catalogue rows no endpoint can create
│   ├── profiles.py          demo roster + per-team activity weights
│   ├── manifest.py          builds `manifest.json`, the stand's description
│   ├── golden_metrics.py    the only source for the manifest's golden set
│   ├── profile_md.py        renders `PROFILE.md` from a manifest
│   ├── render_profile.py    regenerates / verifies `PROFILE.md`; no database
│   └── generators/          one module per activity domain, `base.py` shared
├── tests/                   stdlib unittest; drivers stubbed in `conftest.py`
├── seed-stand.sh            seeds a Kubernetes stand (discover → render → apply)
├── seed-job.yaml.tpl        the Job it renders — and the reference manifest
├── Dockerfile               the compose `seed-sample` image
├── pyproject.toml           package metadata, deps, ruff + mypy config
├── PROFILE.md               GENERATED, committed — do not hand-edit
└── manifest.json            GENERATED per stand at seed time (gitignored)
```

On a cluster the runtime is the toolbox image (`../toolbox/Dockerfile`), which
carries this tree at `/ingestion/tools/seed` together with the migration
scripts the silver step runs — so the Job's command is the same
`python -m insight_seed` you would run locally.
