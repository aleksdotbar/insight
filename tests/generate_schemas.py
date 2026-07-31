"""Generate (or verify) `stand/api/schemas/analytics.py` from the committed spec.

    uv run --project tests --frozen python tests/generate_schemas.py
    uv run --project tests --frozen python tests/generate_schemas.py --check

`docs/components/backend/analytics/openapi.json` is itself generated from the
handlers' own types (`cargo run -p analytics -- openapi`) and drift-gated in CI
by `.github/workflows/openapi-specs.yml`. Generating pydantic models from it
therefore introduces no second source of truth — the models describe the very
structs that serialize the wire.

The output is COMMITTED. A test run must never need the generator, which is a
dev-only dependency and absent from the ui-tests image; `--check` is what keeps
the committed copy honest, the same arrangement `deploy/seed/render_profile.py`
uses for PROFILE.md.

Deliberately NOT a pytest case. `tests/stand/` exists to assert things about a
deployed stand, and whether a generated file in this repository is current is a
statement about the repository. The same reasoning retired
`test_credentials_contract.py`.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
from pathlib import Path

_TESTS = Path(__file__).resolve().parent
_REPO_ROOT = _TESTS.parent

SPEC = _REPO_ROOT / "docs" / "components" / "backend" / "analytics" / "openapi.json"
OUTPUT = _TESTS / "stand" / "api" / "schemas" / "analytics.py"

# `--disable-timestamp` matters: without it every run writes a new header and
# `--check` can never pass. `--extra-fields forbid` is the generated half's
# strictness — these models are regenerated in the same change that adds a
# field, so an undeclared one is real drift rather than a benign addition.
CODEGEN_ARGS = (
    "--input-file-type", "openapi",
    "--output-model-type", "pydantic_v2.BaseModel",
    "--target-python-version", "3.13",
    "--use-standard-collections",
    "--use-union-operator",
    "--use-schema-description",
    "--field-constraints",
    "--extra-fields", "forbid",
    "--disable-timestamp",
    "--formatters", "ruff-format",
    "--formatters", "ruff-check",
)

HEADER = '''"""Analytics response shapes — GENERATED, do not edit.

Regenerate with:

    uv run --project tests --frozen python tests/generate_schemas.py

Source: `docs/components/backend/analytics/openapi.json`, which is itself
generated from the analytics handlers' own types and drift-gated in CI. These
models therefore describe the structs that serialize the wire, and a validation
failure means the service and its published contract disagree — a contract test,
unlike the hand-written models in `identity.py`.

`extra="forbid"` throughout: an undeclared field is drift, and the models are
regenerated in the same change that would add one.

BODIES ONLY. This document's per-operation status-code lists are stamped
uniformly by `.standard_errors` and describe nothing (#1669), so no test takes a
status code from here — every one is asserted from observed behaviour.

ONE substitution is applied after generation: every `AwareDatetime` becomes
`UnzonedDatetime`, because the contract declares `format: date-time` while the
service serialises timestamps with no offset. See `common.UnzonedDatetime`.
"""

'''


def generate() -> str:
    """Run the generator and return the module source, header included."""
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "analytics.py"
        subprocess.run(
            ["datamodel-codegen", "--input", str(SPEC), *CODEGEN_ARGS, "--output", str(out)],
            check=True,
            capture_output=True,
            text=True,
        )
        body = out.read_text(encoding="utf-8")

    # The generator's own two-line provenance comment is replaced by HEADER,
    # which says the same thing plus what a reader needs to know about trusting
    # these models.
    lines = body.splitlines(keepends=True)
    while lines and lines[0].startswith("#"):
        lines.pop(0)
    source = "".join(lines).lstrip("\n")

    # The single pinned deviation — see `common.UnzonedDatetime`. Applied here
    # rather than by hand because the file is regenerated: an edit would be lost,
    # and this way `--check` still passes on a clean tree.
    source = source.replace("AwareDatetime", "UnzonedDatetime")
    source = source.replace("from pydantic import UnzonedDatetime, ", "from pydantic import ")
    source = source.replace("from pydantic import UnzonedDatetime\n", "")
    # After `from __future__`, which must stay the first statement in the module.
    source = source.replace(
        "from __future__ import annotations\n",
        "from __future__ import annotations\n\nfrom .common import UnzonedDatetime\n",
        1,
    )

    return HEADER + source


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="exit non-zero if the committed module differs from a fresh generation",
    )
    args = parser.parse_args()

    fresh = generate()
    if not args.check:
        OUTPUT.write_text(fresh, encoding="utf-8")
        print(f"wrote {OUTPUT}")
        return 0

    current = OUTPUT.read_text(encoding="utf-8") if OUTPUT.exists() else ""
    if current == fresh:
        print(f"{OUTPUT.name} is up to date")
        return 0

    print(
        f"{OUTPUT} is STALE — the committed models no longer match {SPEC.name}.\n"
        "Regenerate:  uv run --project tests --frozen python tests/generate_schemas.py",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
