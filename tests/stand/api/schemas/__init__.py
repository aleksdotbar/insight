"""Pydantic models for the stand's response shapes.

Two halves, and a reader should be able to tell which one they are in:

* `common.py` — the error envelope and the listing wrapper, hand-written from
  the bodies the stand returns.
* `identity.py` — hand-written from the Rust DTOs in
  `src/backend/services/identity-resolution/src/api/`. **Not** generated,
  because the committed contract for that service is still the .NET document:
  it declares `/v1/persons/{email}` (which identity answers 404 for — the path
  moved to analytics), declares `POST /v1/persons-seed` (405), spells the
  subchart parameter `{personId}` where the service serves `{person_id}`, omits
  both persons-sync operations, and lists only `200` for all 18 operations.
  Generating from it would record every one of those errors as fact.
* `analytics.py` — GENERATED from `docs/components/backend/analytics/openapi.json`.
  That document is itself generated from the handlers' own types
  (`cargo run -p analytics -- openapi`) and drift-gated in CI by
  `.github/workflows/openapi-specs.yml`, which fails the build when the
  committed copy goes stale. So its response schemas describe the very structs
  that serialize the wire — generating models from it introduces no second
  source of truth, and its 70 schemas are not a hand-copying job.

* `authenticator.py` — GENERATED the same way, from
  `docs/components/backend/authenticator/openapi.json` (same provenance: emitted
  offline by the service, drift-gated in the same workflow). Currently just the
  error envelope, because that document declares every `/auth/*` success body as
  a bare `type: object` — those handlers answer untyped JSON. It exists so a
  handler that gains a typed response lands here on the next regeneration rather
  than being noticed by someone.

  **Bodies from the spec; status codes never.** The same document's per-operation
  status-code lists are stamped uniformly by `.standard_errors` and do not
  describe what any route actually returns (#1669) — the identity contract shows
  the same failure in its own way, listing only `200` everywhere. So the models
  come from the spec and every status code stays asserted per test, from
  observed behaviour.

That asymmetry is a real difference in what the models mean. The generated ones
are a **contract test** — a mismatch says the service and its published contract
disagree. The hand-written ones are a **description of observed behaviour**, and
they should be deleted in favour of generated ones once the identity contract is
regenerated from the service.

The strictness follows from that. Generated models set `extra="forbid"`: they are
regenerated in the same change that adds a field, so strictness costs nothing and
an undeclared field is exactly the drift worth catching. Hand-written models
leave `extra` at its default, because there they would only tax every benign
upstream addition.
"""

from __future__ import annotations

from collections.abc import Sequence

# `ListResponse` is aliased on import: the analytics document names the
# admin-threshold envelope generically, which would shadow the generic
# `common.ListResponse[T]` that every other listing uses.
from .analytics import (
    AdminMetricThresholdView,
    CatalogResponse,
    ColumnListResponse,
    Metric,
    MetricDefinitionListResponse,
    MetricListResponse,
    MetricResultsResponse,
    MetricSummary,
    Person,
    QueryResponse,
    RunResponse,
    SavedQuery,
    SavedQueryListResponse,
    Threshold,
    ThresholdListResponse,
)
from .analytics import (
    ListResponse as AdminMetricThresholdList,
)
from .common import (
    EXTRACTOR_REJECTION_CONTENT_TYPE,
    PROBLEM_CONTENT_TYPE,
    ListResponse,
    ProblemDocument,
)
from .identity import (
    Operation,
    OperationList,
    PersonRole,
    PersonRoleList,
    Profile,
    Role,
    RoleList,
    Subchart,
    SubchartForest,
    SubchartNode,
    Visibility,
    VisibilityList,
)

__all__: Sequence[str] = (
    "EXTRACTOR_REJECTION_CONTENT_TYPE",
    "PROBLEM_CONTENT_TYPE",
    "AdminMetricThresholdList",
    "AdminMetricThresholdView",
    "CatalogResponse",
    "ColumnListResponse",
    "ListResponse",
    "Metric",
    "MetricDefinitionListResponse",
    "MetricListResponse",
    "MetricResultsResponse",
    "MetricSummary",
    "Operation",
    "OperationList",
    "Person",
    "PersonRole",
    "PersonRoleList",
    "ProblemDocument",
    "Profile",
    "QueryResponse",
    "Role",
    "RoleList",
    "RunResponse",
    "SavedQuery",
    "SavedQueryListResponse",
    "Subchart",
    "SubchartForest",
    "SubchartNode",
    "Threshold",
    "ThresholdListResponse",
    "Visibility",
    "VisibilityList",
)
