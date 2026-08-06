"""Shared test bootstrap: put the tool directory on `sys.path` and stub the
database drivers.

Named `conftest.py` for pytest's benefit, but it is plain module-level code and
works the same under `unittest`, which is what this package actually uses (see
`tests/README` in the seeder README). Both entry points import it first:

    python3 -m unittest discover -s tests -t .      # from the tool directory

`pymysql` and `clickhouse_connect` are runtime-only dependencies of the seed
image. Every test here exercises the pure half — env parsing, SQL shapes,
refusal messages — so stand-ins keep the suite runnable with nothing installed.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path

#: The tool directory (parent of `tests/`), so `import insight_seed` resolves
#: whichever checkout this file lives in.
TOOL_ROOT = Path(__file__).resolve().parents[1]
if str(TOOL_ROOT) not in sys.path:
    sys.path.insert(0, str(TOOL_ROOT))


def _stub_pymysql() -> None:
    if "pymysql" in sys.modules:
        return
    stub = types.ModuleType("pymysql")
    stub.cursors = types.SimpleNamespace(Cursor=object)  # type: ignore[attr-defined]
    stub.connections = types.SimpleNamespace(Connection=object)  # type: ignore[attr-defined]
    stub.connect = lambda **_kwargs: None  # type: ignore[attr-defined]
    stub.MySQLError = Exception  # type: ignore[attr-defined]
    sys.modules["pymysql"] = stub


_stub_pymysql()
