"""Parametrisation for the golden-metric suite.

The golden set is turned into test cases by `pytest_generate_tests` rather than
by a module-level `@parametrize`, and the reason is not style.

A module-level parametrize has to read the manifest while the module is being
imported. At that moment the only path available is the default one, so two
things go wrong that are invisible until someone hits them:

* `--stand-manifest <path>` is silently ignored. The fixtures honour it, the
  parametrised cases do not, and the suite reports zero golden metrics while
  claiming to describe the stand that flag names.
* If `deploy/seed/manifest.json` does not exist — a checkout that has never
  seeded locally, pointed at a remote stand — the import raises and the WHOLE
  suite fails to collect, `--stand-manifest` notwithstanding.

`pytest_generate_tests` runs during collection, after `pytest_configure`, and is
handed `metafunc.config`. So it can resolve the path exactly as
`tests/stand/conftest.py` does, and an unusable manifest degrades to zero cases
here and is reported once, properly, by that conftest's collection hook.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from insight_stand import GoldenMetric, Manifest, ManifestError, default_manifest_path


def _declared_golden_metrics(config: pytest.Config) -> tuple[GoldenMetric, ...]:
    """The golden set from the manifest THIS RUN was aimed at.

    Resolution mirrors `tests/stand/conftest.py::pytest_configure`: the
    `--stand-manifest` option first, then `$INSIGHT_STAND_MANIFEST`, then the
    path the seed writes.

    A manifest that cannot be read yields no cases rather than an error. The
    suite still refuses to run — `pytest_collection_modifyitems` in the parent
    conftest raises a `UsageError` naming the file — and one clear message from
    the place that owns that check beats a collection traceback from here.
    """
    chosen = config.getoption("--stand-manifest", default=None)
    path = Path(str(chosen)) if chosen else default_manifest_path()
    try:
        return Manifest.load(path).golden_metrics
    except ManifestError:
        return ()


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if "golden" not in metafunc.fixturenames:
        return
    metafunc.parametrize(
        "golden",
        _declared_golden_metrics(metafunc.config),
        ids=lambda g: f"{g.metric_key}[{g.scope}/{g.window}]",
    )
