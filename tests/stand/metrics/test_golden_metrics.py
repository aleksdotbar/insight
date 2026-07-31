"""One assertion per `golden_metrics[]` entry the stand declares.

**This suite currently asserts nothing, and that is the accepted design rather
than an oversight.** `deploy/seed/manifest.json` ships `golden_metrics: []`, so
the parametrised case below collects zero times and the suite reports a skip.

Why it is empty is worth reading before anyone "fixes" it. A golden metric is an
exact expected value, and `deploy/seed/golden_metrics.py` will only accept one
that satisfies two constraints: it must not depend on wall-clock time, and its
expectation must be COMPUTABLE FROM THE SEED INPUTS rather than read back out of
the gold layer. Reading a value off a running stand and asserting it back is not
a test — it asserts that the code that produced the number produced the number.
No metric in the current inventory has cleared that bar.

So the honest shape is a suite that is visibly empty. The alternative — inventing
plausible expectations, or capturing observed output — would be green and wrong,
and would take the pressure off ever measuring properly. A visible gap beats a
silent wrong answer.

`test_the_empty_golden_set_explains_itself` exists so the gap has a voice in the
test report instead of being a zero nobody notices.

The parametrisation lives in `conftest.py`, and that file explains why.
"""

from __future__ import annotations

import pytest
from insight_stand import GoldenMetric, Manifest, PersonaSession, analytics_path

#: A `window` naming the manifest's own seeded range rather than spelling dates.
WINDOW_DATA_WINDOW = "data_window"

#: The `scope` grammar this suite can honour: one seeded person, by FIXTURE NAME
#: — `person:dev_lead`. A fixture name rather than an email or a UUID for the
#: same reason every other test uses one: a roster reshuffle moves the person
#: without editing an expectation.
SCOPE_PERSON_PREFIX = "person:"


def _entity_email(golden: GoldenMetric, manifest: Manifest) -> str:
    """The email the declared scope names, or a refusal naming what is supported.

    Refusing is the point. This test previously ignored `scope` entirely and
    always asked for the lead's own figures, so an entry scoped to somebody else
    — or to a team — was asserted against the wrong subject and passed or failed
    for reasons unrelated to what it declared. An expectation the suite cannot
    honour must stop the run, not quietly become a different expectation.
    """
    if not golden.scope.startswith(SCOPE_PERSON_PREFIX):
        raise AssertionError(
            f"{golden.metric_key} declares scope {golden.scope!r}, which this suite "
            f"cannot honour. Supported: {SCOPE_PERSON_PREFIX}<fixture name>, e.g. "
            f"{SCOPE_PERSON_PREFIX}dev_lead. Implement the new scope in "
            "_entity_email before adding an entry that uses it — do NOT let it "
            "fall through to a different subject."
        )
    fixture_name = golden.scope[len(SCOPE_PERSON_PREFIX) :]
    # `Manifest.fixture` raises naming the catalog if the name is unknown, which
    # is the right failure for a typo in the seed's own golden list.
    return manifest.fixture(fixture_name).email


def _period(golden: GoldenMetric, manifest: Manifest) -> tuple[str, str]:
    """The `from`/`to` the declared window names, or a refusal.

    Two forms, both anchored: the literal `data_window`, meaning the range the
    manifest says this stand was seeded over, and an explicit `<from>..<to>`.
    `deploy/seed/golden_metrics.py` requires an expectation to be reproducible
    against a pinned anchor, so a window that is neither is a mistake worth
    stopping for.
    """
    if golden.window == WINDOW_DATA_WINDOW:
        start, _, end = manifest.data_window.partition("..")
        return start, end
    if ".." in golden.window:
        start, _, end = golden.window.partition("..")
        if start and end:
            return start, end
    raise AssertionError(
        f"{golden.metric_key} declares window {golden.window!r}, which this suite "
        f"cannot honour. Supported: {WINDOW_DATA_WINDOW!r}, or an explicit "
        "'<from>..<to>'. Implement the new form in _period before adding an entry "
        "that uses it — do NOT let it fall through to a different range."
    )


def _served_value(body: object, golden: GoldenMetric, entity_email: str) -> float | int | str | None:
    """The single value the response carries for this metric and this person.

    Walked rather than searched. This test previously asserted
    `str(expected) in response.text`, which matches the expectation anywhere in
    the envelope — the echoed period dates, the `format`/`unit` metadata, the
    entity id, or a longer number that merely contains it. Measured against this
    stand, that accepted `1`, `0`, `44` and `2026` for a metric whose real value
    was `3044.0`, and accepted any digit at all for a metric whose value was
    `null`.
    """
    assert isinstance(body, dict), f"{golden.metric_key}: response is not a JSON object"

    metrics = body.get("metrics")
    assert isinstance(metrics, list) and len(metrics) == 1, (
        f"{golden.metric_key}: expected exactly one metric in the response, got {metrics!r}"
    )
    metric = metrics[0]
    assert isinstance(metric, dict) and metric.get("metric_key") == golden.metric_key, (
        f"asked for {golden.metric_key!r}, response answered for {metric!r}"
    )

    views = metric.get("views")
    assert isinstance(views, list) and len(views) == 1, (
        f"{golden.metric_key}: expected exactly one view, got {views!r}"
    )
    view = views[0]
    assert isinstance(view, dict) and view.get("view") == "period", (
        f"{golden.metric_key}: asked for the period view, got {view!r}"
    )

    values = view.get("values")
    assert isinstance(values, list) and len(values) == 1, (
        f"{golden.metric_key}: expected one value for {entity_email}, got {values!r}"
    )
    value = values[0]
    assert isinstance(value, dict) and value.get("entity_id") == entity_email, (
        f"{golden.metric_key}: asked about {entity_email}, response answered about {value!r}"
    )

    served = value.get("value")
    assert served is None or isinstance(served, (int, float, str)), (
        f"{golden.metric_key}: value is {served!r}, which is not a number, a string or null"
    )
    return served


def test_the_empty_golden_set_explains_itself(stand_manifest: Manifest) -> None:
    """When there are no golden metrics, the manifest must say why.

    The one thing this suite can always assert. An empty `golden_metrics` with a
    populated `golden_metrics_note` is a deliberate, documented gap; an empty one
    with no note is a seed that failed to write the section, and the two are
    indistinguishable from the array alone.
    """
    if stand_manifest.golden_metrics:
        pytest.skip(
            f"this stand declares {len(stand_manifest.golden_metrics)} golden metrics, "
            "so the parametrised assertions below carry the coverage"
        )

    assert stand_manifest.golden_metrics_note.strip(), (
        f"{stand_manifest.source_path} declares no golden metrics AND no "
        "`golden_metrics_note` — so nothing distinguishes 'deliberately none yet' "
        "from 'the seed failed to write them'"
    )


@pytest.mark.requires_seed("dev_lead")
def test_golden_metric_matches_the_manifest(
    golden: GoldenMetric, lead_session: PersonaSession, stand_manifest: Manifest
) -> None:
    """The stand serves the value its own manifest says it was seeded to serve.

    Every expectation is read from the manifest at runtime. No number appears in
    this file, which is the rule that keeps the suite from asserting a value
    somebody once observed.

    The comparison is EQUALITY on the one served value, which is what the seed
    side already promises: "The consuming test suite asserts every entry as an
    exact match and has no other source of expectations"
    (`deploy/seed/golden_metrics.py`).

    `expected` is deliberately not narrowed to a number — the manifest schema
    declares it `number | string`. Numbers are compared numerically because the
    API serialises an integer-format metric as `12.0`, and `12 != "12.0"` as
    strings while the metric is plainly correct.
    """
    entity_email = _entity_email(golden, stand_manifest)
    start, end = _period(golden, stand_manifest)

    response = lead_session.client.post(
        analytics_path("/v1/metric-results"),
        json_body={
            # Email, not UUID: this endpoint keys entities by email and answers a
            # well-formed 200 of nulls for a UUID. See
            # tests/stand/api/test_analytics_results.py.
            "entity": {"type": "person", "ids": [entity_email]},
            "period": {"from": start, "to": end},
            "metrics": [{"metric_key": golden.metric_key, "views": [{"view": "period"}]}],
        },
    )
    assert response.status_code == 200, (
        f"{golden.metric_key}: {response.status_code} {response.text[:300]}"
    )

    served = _served_value(response.json(), golden, entity_email)
    assert served is not None, (
        f"{golden.metric_key} came back null for {entity_email} over {start}..{end}; "
        f"{stand_manifest.source_path} says it should be {golden.expected!r} "
        f"({golden.derivation}). The request reached the service and no gold data "
        "answered it."
    )

    expected = golden.expected
    both_numeric = (
        isinstance(expected, (int, float))
        and not isinstance(expected, bool)
        and isinstance(served, (int, float))
    )
    matches = float(served) == float(expected) if both_numeric else str(served) == str(expected)
    assert matches, (
        f"{golden.metric_key} at scope {golden.scope!r} over {golden.window!r}: the "
        f"stand served {served!r}, {stand_manifest.source_path} says {expected!r} "
        f"({golden.derivation})"
    )
