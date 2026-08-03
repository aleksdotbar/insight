"""Meta-tests for the coverage gate — no stand, no network.

A gate that prints a blocking violation while exiting 0 is worse than no gate:
it converts a real gap into a green run plus a wall of text nobody reads. These
pin the mechanics that could do that — the verdict, the exit code and the
rendered report all deriving from one predicate — and the two failure modes this
gate exists for.

Ported from the rig's `identity/test_meta_gate.py`, which pinned the same
property for `lib/api_coverage.py`. The tables differ on 403 (see `coverage.py`'s
docstring for why it is required here rather than excluded); the reason for
testing the gate does not.

Deliberately under `stand/` but needing nothing from it: no fixture here touches
the manifest, a session or a URL, so these run in a plain checkout with no stand
up. That is the same reasoning that keeps the gate itself stdlib-only.
"""

from __future__ import annotations

from typing import Any

from insight_stand import coverage

# Two catalogued operations, standing in for the real 45.
METRICS = coverage.Operation(method="GET", path="/api/analytics/v1/metrics")
SUBCHART = coverage.Operation(method="GET", path="/api/identity/v1/subchart")
CATALOGUE = [METRICS, SUBCHART]


def _ledger(rows: dict[tuple[str, str], list[int]]) -> list[dict[str, Any]]:
    return [
        {"method": method, "path": path, "statuses": statuses}
        for (method, path), statuses in rows.items()
    ]


def _spec(paths: dict[str, dict[str, list[int]]]) -> dict[str, Any]:
    return {
        "paths": {
            path: {
                method: {"responses": {str(code): {} for code in codes}}
                for method, codes in methods.items()
            }
            for path, methods in paths.items()
        }
    }


def _catalogue_report(rows: dict[tuple[str, str], list[int]]) -> coverage.CatalogueReport:
    return coverage.CatalogueReport(
        catalogue=CATALOGUE, observed=coverage.by_label(_ledger(rows))
    )


def test_an_operation_only_the_sweep_touched_is_not_covered() -> None:
    """The failure this gate exists for.

    `api/test_gateway.py` calls every catalogued operation anonymously, so every
    one appears in the ledger whether or not a test ever used it. Treating
    presence as coverage would report 100% for a suite that asserts nothing but
    the edge's refusal — which is exactly the shape a naive port of the rig's
    gate would have had.
    """
    report = _catalogue_report(
        {
            (METRICS.method, METRICS.path): [200, 401],
            (SUBCHART.method, SUBCHART.path): [401],
        }
    )

    assert report.exercised == [METRICS.label]
    assert report.swept_only == [SUBCHART.label]
    assert not report.passed

    violations = coverage.violations(report, None)
    assert any("SWEPT ONLY" in v and SUBCHART.path in v for v in violations), violations


def test_a_real_id_counts_against_the_operation_it_belongs_to() -> None:
    """A concrete url folds onto its catalogued template.

    The catalogue names one stand-in id; a test that updates a real row records
    a different one. Comparing literal paths puts them in separate buckets, so
    the only call left against the catalogued url is the anonymous sweep's and
    the gate reports SWEPT ONLY for an operation a passing test just exercised.
    That is what it said about both admin-threshold writes on the first run
    that got this far.
    """
    catalogued = coverage.Operation(
        method="PUT",
        path="/api/analytics/v1/admin/metric-thresholds/01900000-0000-7000-8000-000000000000",
        template="/api/analytics/v1/admin/metric-thresholds/{id}",
    )
    report = coverage.CatalogueReport(
        catalogue=[catalogued],
        observed=coverage.by_label(
            _ledger(
                {
                    ("PUT", catalogued.path): [401],  # the sweep
                    ("PUT", "/api/analytics/v1/admin/metric-thresholds/019fc6c8-020f"): [200],
                }
            )
        ),
    )

    assert report.exercised == [catalogued.key]
    assert not report.swept_only
    assert report.passed


def test_a_catalogue_entry_without_a_template_still_matches_itself() -> None:
    """A ledger from a suite that predates templates must not crash the gate."""
    report = _catalogue_report({(METRICS.method, METRICS.path): [200]})
    assert METRICS.key == METRICS.label
    assert report.exercised == [METRICS.label]


def test_a_catalogued_operation_nobody_called_fails() -> None:
    """`operations.py` naming a route no test reaches is a gate failure.

    Distinct from swept-only: this one is absent from the ledger entirely, which
    means even the 401 sweep did not reach it — usually a typo'd path, which
    would otherwise sit in the catalogue looking like coverage forever.
    """
    report = _catalogue_report({(METRICS.method, METRICS.path): [200]})

    assert report.unobserved == [SUBCHART.label]
    assert not report.passed
    assert any("NEVER CALLED" in v for v in coverage.violations(report, None))


def test_a_fully_exercised_catalogue_passes() -> None:
    report = _catalogue_report(
        {
            (METRICS.method, METRICS.path): [200, 401],
            (SUBCHART.method, SUBCHART.path): [200, 401],
        }
    )
    assert report.passed
    assert not coverage.violations(report, None)


def test_the_verdict_and_the_violations_cannot_disagree() -> None:
    """PASS is `no violations`, not a second opinion about them.

    The one invariant that makes every other test here worth having: a report
    that renders ✅ while listing a blocking finding is the failure mode a gate
    must never have.
    """
    failing = _catalogue_report({(SUBCHART.method, SUBCHART.path): [401]})
    rendered = coverage.render(failing, None)
    assert "❌ FAIL" in rendered and "✅ PASS" not in rendered
    assert coverage.violations(failing, None)

    passing = _catalogue_report(
        {
            (METRICS.method, METRICS.path): [200],
            (SUBCHART.method, SUBCHART.path): [200],
        }
    )
    assert "✅ PASS" in coverage.render(passing, None)


def test_gateway_prefixes_fold_onto_the_service_contract() -> None:
    """`/api/analytics/v1/metrics` is the document's `/v1/metrics`.

    The gateway strips the prefix before the service sees the request
    (`routes.yaml`, `strip_prefix: true`), so the ledger's gateway paths and the
    spec's service paths describe the same call. Getting this wrong would report
    every operation as unmatched — a 0% that looks like a broken suite rather
    than a broken matcher.
    """
    spec_ops = coverage.spec_operations(_spec({"/v1/metrics": {"get": [200, 404]}}))
    validated, unmatched = coverage.match_against_spec(
        _ledger({("GET", "/api/analytics/v1/metrics"): [200]}),
        "/api/analytics",
        spec_ops,
    )

    assert validated == {"GET /v1/metrics": {200}}
    assert not unmatched


def test_a_path_parameter_matches_its_template() -> None:
    spec_ops = coverage.spec_operations(_spec({"/v1/metrics/{id}": {"get": [200]}}))
    validated, _ = coverage.match_against_spec(
        _ledger({("GET", "/api/analytics/v1/metrics/abc-123"): [200]}),
        "/api/analytics",
        spec_ops,
    )
    assert validated == {"GET /v1/metrics/{id}": {200}}


def test_a_literal_path_wins_over_a_same_arity_template() -> None:
    """`/v1/metrics/queries` is not a metric whose id is "queries".

    Both templates have two segments, so ordering decides. Sorting by
    `{param}` count is what makes the answer independent of the order the
    document happened to list them in.
    """
    spec_ops = coverage.spec_operations(
        _spec({"/v1/metrics/{id}": {"get": [200]}, "/v1/metrics/queries": {"get": [200]}})
    )
    validated, _ = coverage.match_against_spec(
        _ledger({("GET", "/api/analytics/v1/metrics/queries"): [200]}),
        "/api/analytics",
        spec_ops,
    )
    assert validated == {"GET /v1/metrics/queries": {200}}


def test_401_and_403_are_required_where_a_handler_can_answer_them() -> None:
    """The authorization codes stay required, pinned — on a route that has them.

    `POST /v1/metric-results` is the one analytics operation whose 403 comes
    from a visibility check rather than a role stub, so it is deliberately NOT
    in BLOCKED. This suite crosses a real gateway with a real session, which
    makes it the only one able to prove either code; dropping them into
    UNIVERSAL_BOILERPLATE would stop requiring the authorization behaviour the
    whole suite exists for.
    """
    op = "POST /v1/metric-results"
    spec_ops = coverage.spec_operations(
        _spec({"/v1/metric-results": {"post": [200, 401, 403, 429, 500]}})
    )
    report = coverage.SpecReport(spec_ops=spec_ops, validated={op: {200}}, unmatched=[])

    required = report.required[op]
    assert 401 in required and 403 in required
    assert 429 not in required, "no rate limiter fronts this stand"
    assert 500 not in required, "a server fault is not deterministically inducible"
    assert report.uncovered[op] == {401, 403}


def test_403_is_subtracted_only_where_no_handler_can_produce_it() -> None:
    """The other half, and the one that makes the table above honest.

    25 of analytics' 30 operations reach no authorization check at all — the
    admin surface's `is_tenant_admin` is a stub returning true, and nothing in
    the metrics, saved-query, catalogue or drilldown handlers gates on anything.
    The five that can refuse are the three admin-threshold WRITES (a broader
    scope's lock, plus a cross-tenant row on the two taking an id) and
    `POST /v1/metric-results` (person visibility).
    The spec declares 403 on all 29 regardless (`.standard_errors`, #1669), so
    requiring it everywhere demands a response the service has no code to send.

    Per-route and sourced, never universal: `GET /v1/metrics` cannot refuse,
    `POST /v1/metric-results` can, and the difference is which handlers exist.
    """
    assert 403 not in coverage.UNIVERSAL_BOILERPLATE, "must stay a per-route judgement"
    assert coverage.BLOCKED["GET /v1/metrics"] == frozenset({403})
    assert "POST /v1/metric-results" not in coverage.BLOCKED
    for write in ("POST /v1/admin/metric-thresholds",
                  "PUT /v1/admin/metric-thresholds/{id}",
                  "DELETE /v1/admin/metric-thresholds/{id}"):
        assert write not in coverage.BLOCKED, f"{write} reaches the lock enforcer"
    assert "GET /v1/admin/metric-thresholds" in coverage.BLOCKED, "reads cannot refuse"

    spec_ops = coverage.spec_operations(_spec({"/v1/metrics": {"get": [200, 401, 403]}}))
    report = coverage.SpecReport(
        spec_ops=spec_ops, validated={"GET /v1/metrics": {200}}, unmatched=[]
    )
    assert report.required["GET /v1/metrics"] == {200, 401}, "403 subtracted, nothing else"


def test_an_undeclared_code_the_suite_proved_is_reported() -> None:
    """The under-declaration half of #1669.

    A code the route answers but the document omits has no column in the
    matrix, so without this it would be invisible — the suite covers it, the
    contract does not describe it, and only one of those is a problem.
    """
    spec_ops = coverage.spec_operations(_spec({"/v1/metrics": {"get": [200]}}))
    report = coverage.SpecReport(
        spec_ops=spec_ops, validated={"GET /v1/metrics": {200, 415}}, unmatched=[]
    )

    assert report.undeclared == {"GET /v1/metrics": {415}}
    assert any("observed but undeclared" in note for note in coverage.advisories(report))


def test_the_session_ledger_survives_these_tests() -> None:
    """The hazard this directory's conftest exists for, pinned from inside it.

    Everything below calls `reset()`, which clears the ledger the whole suite is
    recording into. Under the autouse isolation the caller's entries come back;
    without it they are gone, the gate grades the run on whatever happened after
    this module, and reports a suite-wide catastrophe that never occurred.
    """
    coverage.record("GET", "/api/analytics/v1/from-an-earlier-test", 200)

    with coverage.isolated():
        coverage.reset()
        coverage.record("GET", "/api/analytics/v1/only-inside", 500)
        assert coverage.by_label(coverage.observed_rows()) == {
            "GET /api/analytics/v1/only-inside": {500}
        }

    surviving = coverage.by_label(coverage.observed_rows())
    assert surviving["GET /api/analytics/v1/from-an-earlier-test"] == {200}
    assert "GET /api/analytics/v1/only-inside" not in surviving


def test_the_ledger_merges_rather_than_overwrites(tmp_path: Any) -> None:
    """Two partial runs against one stand add up.

    A developer runs `-k` slices; a plain overwrite would report the last one as
    if it were the whole run, which is the difference between "we cover this"
    and "the last thing I ran covered this".
    """
    target = tmp_path / "ledger.json"

    coverage.reset()
    coverage.record("GET", "/api/analytics/v1/metrics", 200)
    coverage.dump(target)

    coverage.reset()
    coverage.record("GET", "/api/analytics/v1/metrics", 404)
    coverage.record("GET", "/api/identity/v1/subchart", 200)
    coverage.dump(target)
    coverage.reset()

    merged = coverage.by_label(coverage.load_ledger(target))
    assert merged["GET /api/analytics/v1/metrics"] == {200, 404}
    assert merged["GET /api/identity/v1/subchart"] == {200}
