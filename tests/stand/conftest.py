"""Suite-wide wiring for the deployed-stand tests.

This conftest assumes an ALREADY-RUNNING stand. It does not start compose, does
not spawn service processes and does not apply migrations — deliberately unlike
`src/ingestion/tests/e2e/conftest.py`, whose `compose_stack` /
`ch_migrations_applied` / `analytics` fixtures own that lifecycle because that
rig builds its own private stack per session. Here the stand is brought up and
seeded by `./dev-compose.sh test-stand up`, and the suite only reads it. A test
run that could bring its own stand up would hide exactly the deployment failures
this suite exists to catch.

Two rules follow from that split:

* The stand must describe itself. Every fixture name, capability and seeded
  fact comes from `deploy/seed/manifest.json`. If it is missing or unparseable
  the session aborts; nothing here has a default to fall back to.
* Unsatisfiable data requirements are a COLLECTION-time abort, not a run-time
  failure. Finding out on test #47 that the stand was never seeded wastes the
  run and buries the cause.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

# `tests/lib` on sys.path so a checkout runs without an install step.
# `pip install -e tests/lib` also works and takes precedence naturally.
_REPO_ROOT = Path(__file__).resolve().parents[2]
_LIB_PATH = _REPO_ROOT / "tests" / "lib"
if str(_LIB_PATH) not in sys.path:
    sys.path.insert(0, str(_LIB_PATH))

from insight_stand import (  # noqa: E402  (import follows the sys.path bootstrap)
    AnonymousCredentials,
    ApiClient,
    Manifest,
    ManifestError,
    StandConnectionError,
    StandEndpoint,
    resolve_endpoint,
)

# Marker -> the manifest capability it requires. A test carrying one of these
# is SKIPPED (with a reason) on a stand that lacks the capability, never failed
# and never silently dropped. Add a row to extend; nothing else changes.
CAPABILITY_MARKERS: dict[str, str] = {
    "requires_ingestion": "ingestion",
}

_MANIFEST: Manifest | None = None


def _manifest() -> Manifest:
    """Load the manifest once per session from its frozen path.

    The path is fixed at `deploy/seed/manifest.json` and has no env knob: it is
    where the seed writes, and letting a run point somewhere else would let a
    green suite describe a stand it never touched.
    """
    global _MANIFEST
    if _MANIFEST is None:
        _MANIFEST = Manifest.load()
    return _MANIFEST


# ---------------------------------------------------------------------------
# Hooks
# ---------------------------------------------------------------------------


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "requires_seed(*names): stand fixtures this test needs, validated "
        "against the manifest's fixture catalog at COLLECTION time",
    )
    config.addinivalue_line(
        "markers",
        "requires_ingestion: needs a stand whose manifest declares the "
        "'ingestion' capability; skipped with a reason when it does not",
    )
    # Make skips visible. Under the wrapper's `pytest -q` the default report
    # chars ("fE") show a bare `s` and no reason, which turns a skipped test
    # into an invisible one. Appending "s" prints every skip with its reason in
    # the short summary; an explicit `-r` from the caller is preserved.
    reportchars = getattr(config.option, "reportchars", "") or ""
    if "s" not in reportchars and "a" not in reportchars:
        config.option.reportchars = reportchars + "s"


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    """Validate data requirements before a single test runs.

    Two different resolutions, on purpose:

    * `requires_seed` — a missing fixture means the stand was seeded wrong.
      Every collected test is inspected, every missing name is gathered, and
      the session aborts ONCE with all of them listed. Failing per-test would
      report the same root cause dozens of times; failing on the first miss
      would hide the rest and force a fix-rerun-discover loop.
    * capability markers — a missing capability is a legitimate property of
      this stand, not a defect, so it skips that item alone.
    """
    del config

    try:
        manifest = _manifest()
    except ManifestError as exc:
        # UsageError aborts the session with a non-zero exit before any test
        # runs — the "refuse to start" contract.
        raise pytest.UsageError(f"stand manifest unusable: {exc}") from exc

    missing: dict[str, list[str]] = {}
    for item in items:
        for marker in item.iter_markers(name="requires_seed"):
            for name in marker.args:
                if name not in manifest.seeded_names:
                    missing.setdefault(str(name), []).append(item.nodeid)

    if missing:
        lines = [
            f"  - {name!r} required by: {', '.join(nodeids)}"
            for name, nodeids in sorted(missing.items())
        ]
        available = ", ".join(sorted(manifest.seeded_names)) or "<none>"
        raise pytest.UsageError(
            "requires_seed: manifest is missing fixtures needed by collected tests:\n"
            + "\n".join(lines)
            + f"\n  manifest: {manifest.source_path} (seeded steps: "
            + f"{', '.join(manifest.seeded) or 'none'})"
            + f"\n  available fixtures: {available}"
            + "\n  Re-seed the stand:  ./dev-compose.sh test-stand seed"
        )

    for item in items:
        for marker_name, capability in CAPABILITY_MARKERS.items():
            if item.get_closest_marker(marker_name) is None:
                continue
            try:
                satisfied = manifest.has_capability(capability)
            except ValueError as exc:
                # A typo in CAPABILITY_MARKERS above, not a property of the
                # stand. Left unchecked it would skip every test carrying the
                # marker with a reason that reads perfectly plausibly.
                raise pytest.UsageError(
                    f"CAPABILITY_MARKERS maps {marker_name!r} to an unknown "
                    f"capability: {exc}"
                ) from exc
            if satisfied:
                continue
            item.add_marker(
                pytest.mark.skip(
                    reason=(
                        f"{marker_name}: manifest capability {capability!r} not "
                        f"present on this stand ({manifest.source_path})"
                    )
                )
            )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def stand_manifest() -> Manifest:
    """The stand's self-description. Raises rather than defaulting."""
    return _manifest()


@pytest.fixture(scope="session")
def stand_endpoint() -> StandEndpoint:
    """Where the stand is, plus which file or variable said so."""
    try:
        return resolve_endpoint()
    except StandConnectionError as exc:
        pytest.fail(str(exc), pytrace=False)


@pytest.fixture(scope="session")
def stand_base_url(stand_endpoint: StandEndpoint) -> str:
    return stand_endpoint.base_url


@pytest.fixture
def anonymous_credentials() -> AnonymousCredentials:
    """No auth material at all — the named case, not an omitted argument."""
    return AnonymousCredentials()


@pytest.fixture
def api_client(
    stand_base_url: str, anonymous_credentials: AnonymousCredentials
) -> Iterator[ApiClient]:
    """Gateway-fronted client with NO credentials.

    Authenticated clients arrive in phase 6, built by handing a `RealLogin` to
    `ApiClient.with_credentials`.
    """
    yield ApiClient(base_url=stand_base_url, credentials=anonymous_credentials)
