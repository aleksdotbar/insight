"""Every stand api/ui test carries exactly one quality-vector marker.

Exactly one is what makes `-m <vector>` selection sound: pytest markers are
additive, so the earlier module-default-plus-override scheme left BOTH vectors
on an overridden test and `-m reliability` / `-m security` selections
overlapped (PR #2414 review). `tests/stand/conftest.py` rejects that shape at
collection; this meta test holds the same invariant from inside a running
session, so the additive form cannot quietly return through a conftest
refactor.

The vector names are derived from the marker declarations in
`tests/pyproject.toml` — the single source the conftest comment promises —
rather than re-declared here.
"""

from __future__ import annotations

import pytest


def _quality_vectors(config: pytest.Config) -> frozenset[str]:
    return frozenset(
        declaration.split(":", 1)[0].strip()
        for declaration in config.getini("markers")
        if "quality vector" in declaration
    )


def test_every_api_and_ui_item_carries_exactly_one_vector(
    request: pytest.FixtureRequest,
) -> None:
    vectors = _quality_vectors(request.config)
    assert len(vectors) == 5, f"marker declarations moved: {sorted(vectors)}"

    offenders = {
        item.nodeid: sorted(m.name for m in item.iter_markers() if m.name in vectors)
        for item in request.session.items
        if "/stand/api/" in str(item.path) or "/stand/ui/" in str(item.path)
    }
    offenders = {nodeid: names for nodeid, names in offenders.items() if len(names) != 1}
    assert not offenders, (
        f"items without exactly one vector marker (so -m selections overlap or miss): {offenders}"
    )
