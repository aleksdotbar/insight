"""Keep the meta-tests from destroying the thing they are about.

`coverage.py` records into a process global that the whole suite shares, and
`tests/stand/conftest.py` dumps it once at session end. These tests exercise
that module's own state — including `reset()` — from inside that same session,
so without this every request made before `tests/stand/meta/` ran would be
discarded and the gate would grade the run on the handful of calls that came
after.

Autouse rather than opt-in: a future meta-test that touches the ledger should
be safe by default, not safe only if its author remembered the hazard. The one
that did not remember was the first one written.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from insight_stand import coverage


@pytest.fixture(autouse=True)
def _private_ledger() -> Iterator[None]:
    with coverage.isolated():
        yield
