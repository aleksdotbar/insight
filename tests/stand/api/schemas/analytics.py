"""Analytics response shapes — GENERATED, do not edit.

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

from __future__ import annotations

from .common import UnzonedDatetime
from enum import StrEnum
from pydantic import BaseModel, ConfigDict, Field, RootModel
from uuid import UUID
from typing import Any
from datetime import date as date_aliased


class Status(StrEnum):
    ok = 'ok'


class Status1(StrEnum):
    error = 'error'


class Bucket(StrEnum):
    day = 'day'
    week = 'week'
    month = 'month'


class CatalogThresholdView(BaseModel):
    """
    Resolved threshold for one metric.

    `good` / `warn` are `f64` on the wire — DECIMAL(20,6) in the DB rounds-trips
    through DOUBLE for every seed value (integers and one-decimal floats). If
    future seed entries need full-precision decimals, this is the place to switch
    to a string serializer; the FE byte-for-byte comparison gate (PRD §12) is
    the regression detector.

    The OpenAPI component is named `CatalogThresholdView` (via `#[schema(as)]`)
    to disambiguate from the admin-CRUD `ThresholdView`
    (`domain::admin_threshold::dto::ThresholdView`), which is a different wire
    shape registered under `AdminMetricThresholdView`. `#[schema(as)]` renames
    only the OpenAPI component — it does NOT affect serde / the wire format.
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    alert_bad: float | None = None
    alert_trigger: float | None = None
    bounded_by_lock: bool = Field(..., description='`true` iff the walk halted on a locked broader-scope row before reaching\nthe most-specific candidate. Separate signal from `resolved_from`, which\nalways names the row that won.')
    good: float
    resolved_from: str = Field(..., description='One of `"team+role" | "team" | "role" | "tenant" | "product-default"`.\nNames the row that won the walk.')
    warn: float


class Computation(StrEnum):
    sum = 'sum'


class ComputationDto1(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    computation: Computation


class Computation1(StrEnum):
    ratio = 'ratio'


class ComputationDto2(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    computation: Computation1
    scale: float


class Computation2(StrEnum):
    median = 'median'


class ComputationDto3(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    computation: Computation2


class Computation3(StrEnum):
    distinct_count = 'distinct_count'


class ComputationDto4(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    computation: Computation3


class ComputationDto(RootModel[ComputationDto1 | ComputationDto2 | ComputationDto3 | ComputationDto4]):
    root: ComputationDto1 | ComputationDto2 | ComputationDto3 | ComputationDto4


class CreateMetricRequest(BaseModel):
    """
    Request to create a new metric.
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    description: str | None = None
    name: str
    query_ref: str


class CreateSavedQueryRequest(BaseModel):
    """
    Request to create a saved query.
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    description: str | None = None
    name: str
    sql: str


class CreateThresholdRequest(BaseModel):
    """
    Request to create a threshold.
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    field_name: str
    level: str = Field(..., description='Result level: `good`, `warning`, `critical`.')
    operator: str = Field(..., description='Comparison operator: `gt`, `ge`, `lt`, `le`, `eq`.')
    value: float


class EvidenceGranularity(StrEnum):
    event = 'event'
    source_summary = 'source_summary'
    derived_population = 'derived_population'


class GetMetricsRequest(BaseModel):
    """
    Request body for `POST /v1/catalog/get_metrics`.

    `tenant_id` is intentionally NOT accepted here — it is resolved server-side
    from the session by `tenant_middleware` (Refs #522 auth-trait). Allowing a
    body-supplied `tenant_id` would open a cross-tenant disclosure surface.
    `deny_unknown_fields` enforces that defensively at the parser layer: a
    caller that smuggles `"tenant_id": "..."` into the body gets a 400 instead
    of a silent ignore.
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    role_slug: str | None = Field(None, description='Role slug for `role` / `team+role` resolution chains. `None` and `Some("")`\nare semantically identical and produce the same cache key (canonical\nempty-string sentinel — see `cache_key` in the cache layer).')
    team_id: str | None = Field(None, description='Team id for `team` / `team+role` resolution chains. Same `None` vs `Some("")`\nequivalence as `role_slug`.')


class HistogramBinDto(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    count: int = Field(..., ge=0)
    hi: float
    lo: float


class HistogramValueDto(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    bins: list[HistogramBinDto] = Field(..., description="Empty when the entity has no events in the period — the entity is\nstill listed, mirroring the period view's every-requested-entity rule.")
    entity_id: str


class Metric(BaseModel):
    """
    A metric definition — an admin-configured SQL query against `ClickHouse`.

    The `query_ref` field holds raw `ClickHouse` SQL. The query engine wraps it
    as a subquery, appending security filters + `OData` filters as parameterized
    WHERE clauses.
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    created_at: UnzonedDatetime
    description: str | None = None
    id: UUID
    insight_tenant_id: UUID
    is_enabled: bool
    name: str
    query_ref: str
    updated_at: UnzonedDatetime


class MetricDimensionDto(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    key: str
    label: str | None = None
    value: str


class MetricDimensionFilterDto(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    dimension: str
    values: list[str]


class MetricDimensionFilterRequest(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    dimension: str
    values: list[str]


class MetricDirection(StrEnum):
    higher_is_better = 'higher_is_better'
    lower_is_better = 'lower_is_better'
    neutral = 'neutral'


class MetricDrilldownCapability(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    granularity: list[EvidenceGranularity]


class MetricDrilldownColumnType(StrEnum):
    string = 'string'
    date = 'date'
    number = 'number'


class MetricDrilldownEntity(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    id: str
    type: str


class MetricDrilldownExportFormat(StrEnum):
    csv = 'csv'
    xlsx = 'xlsx'


class MetricDrilldownFilter(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    dimension: str
    values: list[str]


class MetricDrilldownPeriod(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    from_: str = Field(..., alias='from')
    to: str


class MetricDrilldownRequest(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    cursor: str | None = None
    display_dimensions: list[str] | None = None
    entity: MetricDrilldownEntity
    filters: list[MetricDrilldownFilter] | None = None
    limit: int | None = Field(None, ge=0)
    metric_key: str
    period: MetricDrilldownPeriod


class MetricDrilldownRow(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    values: dict[str, Any]


class MetricDrilldownSelection(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    display_dimensions: list[str]
    entity: MetricDrilldownEntity
    filters: list[MetricDrilldownFilter]
    metric_key: str
    period: MetricDrilldownPeriod


class MetricFormat(StrEnum):
    integer = 'integer'
    decimal = 'decimal'
    currency = 'currency'
    percent = 'percent'


class MetricGroupLimitRequest(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    count: int = Field(..., ge=0)
    include_remainder: bool
    rank_by_metric: str | None = None


class MetricQueryLink(BaseModel):
    """
    One link row from `metric_query_catalog`. Tells a consumer which catalog
    rows a `metrics.query_ref` emits when executed — the M:N answer ADR-001
    added at the DB layer, surfaced here so consumers don't have to derive it
    by joining on backend-internal `metric_key` strings.

    `catalog_metric_ids` is the set of `metric_catalog.id` UUIDs the query
    produces. The set is empty only when the linked catalog rows are all
    `is_enabled = false` (filtered out of the `metrics` array) — consumers
    degrade gracefully on empty.
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    catalog_metric_ids: list[UUID] = Field(..., description='`metric_catalog.id` UUIDs this query emits. Sorted ascending so the\nwire payload is byte-stable for cache + diff tooling.')
    query_id: UUID = Field(..., description='`metrics.id` — the ClickHouse `query_ref` row this link is FROM.')


class Computation4(StrEnum):
    sum = 'sum'


class MetricResultDto1(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    computation: Computation4


class Computation5(StrEnum):
    ratio = 'ratio'


class MetricResultDto2(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    computation: Computation5
    scale: float


class Computation6(StrEnum):
    median = 'median'


class MetricResultDto3(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    computation: Computation6


class Computation7(StrEnum):
    distinct_count = 'distinct_count'


class MetricResultDto4(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    computation: Computation7


class View(StrEnum):
    period = 'period'


class View1(StrEnum):
    timeseries = 'timeseries'


class View2(StrEnum):
    peer = 'peer'


class View3(StrEnum):
    breakdown = 'breakdown'


class View4(StrEnum):
    histogram = 'histogram'


class MetricResultViewDto5(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    values: list[HistogramValueDto]
    view: View4


class MetricResultsEntity(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    ids: list[str]
    type: str


class MetricResultsEntityDto(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    ids: list[str]
    type: str


class MetricResultsPeriod(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    from_: str = Field(..., alias='from')
    to: str


class MetricResultsPeriodDto(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    from_: str = Field(..., alias='from')
    to: str


class MetricSchemaErrorCode(StrEnum):
    table_not_found = 'table_not_found'
    column_not_found = 'column_not_found'
    dimension_not_covered = 'dimension_not_covered'
    unknown = 'unknown'


class MetricSummary(BaseModel):
    """
    Summary returned in list endpoints (no `query_ref`).
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    description: str | None = None
    id: UUID
    name: str


class MetricView(BaseModel):
    """
    One catalog metric on the wire. `metric_key` is surfaced per ADR-002 as the
    transitional FE-bridge identifier; consumers MUST still key lookups by `id`.
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    description: str | None = None
    format: str | None = None
    higher_is_better: bool
    id: UUID
    is_member_scale: bool
    label: str
    metric_key: str = Field(..., description="Backend's `<table_name>.<column_name>` identifier. Surfaced per ADR-002\nso the FE can align compiled-in `BULLET_DEFS` constants to wire rows\nduring the catalog-hydration transitional release; the stable lookup\nkey remains `id`.")
    schema_error_code: str | None = Field(None, description='Canonical code from `{ table_not_found, column_not_found,\nclickhouse_unreachable, unknown }`, only present when `schema_status = "error"`.\nRaw ClickHouse error text NEVER reaches consumers per DESIGN §3.3.')
    schema_status: str = Field(..., description='`"ok" | "error" | "unchecked"` — sourced from `metric_catalog.schema_status`.\nConsumers render `"unchecked"` the same as `"ok"` (validator hasn\'t run\nyet); only `"error"` triggers the broken-metric indicator.')
    source_tags: list[str]
    sublabel: str | None = None
    thresholds: CatalogThresholdView
    unit: str | None = None


class View5(StrEnum):
    period = 'period'


class MetricViewRequest1(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    view: View5


class View6(StrEnum):
    peer = 'peer'


class MetricViewRequest2(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    cohort_key: str | None = None
    view: View6


class View7(StrEnum):
    timeseries = 'timeseries'


class MetricViewRequest3(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    bucket: Bucket | None = None
    dimensions: list[str] | None = None
    group_limit: MetricGroupLimitRequest | None = None
    view: View7


class View8(StrEnum):
    breakdown = 'breakdown'


class MetricViewRequest4(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    dimensions: list[str]
    view: View8


class View9(StrEnum):
    histogram = 'histogram'


class MetricViewRequest5(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    view: View9


class MetricViewRequest(RootModel[MetricViewRequest1 | MetricViewRequest2 | MetricViewRequest3 | MetricViewRequest4 | MetricViewRequest5]):
    root: MetricViewRequest1 | MetricViewRequest2 | MetricViewRequest3 | MetricViewRequest4 | MetricViewRequest5


class PageInfo(BaseModel):
    """
    Pagination info.
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    cursor: str | None = None
    has_next: bool


class PeerValueDto(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    entity_id: str
    max: float | None = None
    median: float | None = None
    min: float | None = None
    n: int = Field(..., ge=0)
    p25: float | None = None
    p75: float | None = None
    target_value: float | None = None


class PeriodValueDto(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    entity_id: str
    value: float | None = None


class Problem(BaseModel):
    """
    RFC 9457 problem+json. `context` varies by error category.
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    context: dict[str, Any]
    detail: str
    instance: str | None = None
    status: int
    title: str
    trace_id: str | None = None
    type: str


class QueryRequest(BaseModel):
    """
    Query request body for `POST /v1/metrics/{id}/query`.

    Uses `OData`-style parameters: `$filter`, `$orderby`, `$select`, `$top`, `$skip`.
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    field_filter: str | None = Field(None, alias='$filter', description='`OData` filter expression.\ne.g. `"metric_date ge \'2026-03-01\' and metric_date lt \'2026-04-01\'"`.')
    field_orderby: str | None = Field(None, alias='$orderby', description='`OData` ordering expression.\ne.g. `"metric_date desc"`.')
    field_select: str | None = Field(None, alias='$select', description='Comma-separated list of columns to return.\ne.g. `"person_id, avg_hours, metric_date"`.')
    field_skip: str | None = Field(None, alias='$skip', description='Opaque cursor for keyset pagination (from previous `page_info.cursor`).')
    field_top: int | None = Field(None, alias='$top', description='Maximum number of rows (default 25, max 200).', ge=0)


class QueryResponse(BaseModel):
    """
    Query response with cursor-based pagination.

    `items` rows carry a per-metric dynamic schema (the `SELECT` columns vary by
    metric), so each row is an untyped JSON object.
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    items: list[Any]
    page_info: PageInfo


class RunResponse(BaseModel):
    """
    Result of `POST /v1/queries/{id}/run`.

    `rows` carry a per-query dynamic schema (the `SELECT` columns vary), so each
    row is an untyped JSON object — the same shape as the metric query path.
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    rows: list[Any]


class RunSavedQueryRequest(BaseModel):
    """
    Optional parameters for `POST /v1/queries/{id}/run` (#1966).

    The `{tenant}` parameter is always injected from the session context and is
    never client-settable, so it is absent here. `period` is the first optional
    named parameter an author can reference as `{period:<Type>}`; it is bound as
    a ClickHouse server-side parameter, never interpolated into the SQL text.
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    period: str | None = None


class SavedQuery(BaseModel):
    """
    A saved query row.
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    created_at: UnzonedDatetime
    description: str | None = None
    id: UUID
    insight_tenant_id: UUID
    name: str
    sql: str
    updated_at: UnzonedDatetime


class SavedQuerySummary(BaseModel):
    """
    Summary returned by the list endpoint (no `sql` body).
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    description: str | None = None
    id: UUID
    name: str


class SchemaStatus(StrEnum):
    ok = 'ok'
    error = 'error'
    unchecked = 'unchecked'


class Scope(StrEnum):
    """
    Canonical scope values for `metric_threshold.scope`. Mirrors the DB-side
    ENUM declared in `migration/m20260522_000002_metric_threshold.rs` line
    102–106 and the resolver's `Scope` (kept as a separate type because the
    resolver's enum is private to that module).

    Wire form is the dash-keyed string the DB stores — deserializing via
    `serde(rename_all = "kebab-case")` would NOT produce the right value for
    `team+role` (kebab would yield `team-role`), so we spell each variant
    explicitly with `#[serde(rename = ...)]`.
    """
    product_default = 'product-default'
    tenant = 'tenant'
    role = 'role'
    team = 'team'
    team_role = 'team+role'


class Subordinate(BaseModel):
    """
    Subordinate summary.
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    display_name: str
    email: str
    job_title: str


class TableColumn(BaseModel):
    """
    A column in the `ClickHouse` schema catalog.
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    clickhouse_table: str
    field_description: str | None = None
    field_name: str
    id: UUID
    insight_tenant_id: UUID | None = None


class Threshold(BaseModel):
    """
    A threshold rule — configured per metric, per field.

    The query engine evaluates every result row against the metric's thresholds
    and attaches a `_thresholds` map to the response.
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    created_at: UnzonedDatetime
    field_name: str
    id: UUID
    insight_tenant_id: UUID
    level: str
    metric_id: UUID
    operator: str
    updated_at: UnzonedDatetime
    value: float


class ThresholdListResponse(BaseModel):
    """
    Response envelope for `GET /v1/metrics/{id}/thresholds`
    (`{ "items": [Threshold] }`).

    Docs-only wrapper mirroring the inline `serde_json::json!` shape the list
    handler emits.
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    items: list[Threshold]


class TimeseriesPointDto(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    bucket_start: str
    value: float | None = None


class UpdateMetricRequest(BaseModel):
    """
    Request to update a metric.

    `description` uses double-Option to distinguish:
    - absent field → leave unchanged
    - explicit `null` → clear to None
    - `"some text"` → set to Some("some text")
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    description: str | None = None
    is_enabled: bool | None = None
    name: str | None = None
    query_ref: str | None = None


class UpdateRequest(BaseModel):
    """
    `PUT /v1/admin/metric-thresholds/{id}` body — update an existing row.

    `scope` / `role_slug` / `team_id` are intentionally accepted here even
    though they're immutable post-create: when present, the gauntlet
    compares the value to the row's current value and rejects with
    `failed_precondition` + `type: "immutable_field"` if they differ. Re-
    scoping requires DELETE + POST per DESIGN §3.7 line 1034.
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    alert_bad: float | None = None
    alert_trigger: float | None = None
    good: float
    is_locked: bool | None = None
    lock_reason: str | None = None
    role_slug: str | None = None
    scope: Scope | None = None
    team_id: str | None = None
    warn: float


class UpdateSavedQueryRequest(BaseModel):
    """
    Request to update a saved query.

    `description` uses double-Option (absent → unchanged, `null` → clear,
    value → set), matching [`super::metric::UpdateMetricRequest`].
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    description: str | None = None
    name: str | None = None
    sql: str | None = None


class UpdateThresholdRequest(BaseModel):
    """
    Request to update a threshold.
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    field_name: str | None = None
    level: str | None = None
    operator: str | None = None
    value: float | None = None


class AdminMetricThresholdView(BaseModel):
    """
    On-wire shape of one `metric_threshold` row in list / get responses.

    `metric_key` is NOT serialized — same backend-internal opacity rule the
    read endpoint follows (`domain/catalog/response.rs::MetricView`).
    Consumers identify a metric by `metric_id`.

    The OpenAPI component is named `AdminMetricThresholdView` (via
    `#[schema(as)]`) to disambiguate from the catalog read path's
    `ThresholdView` (`domain::catalog::response::ThresholdView`, registered as
    `CatalogThresholdView`), which is a different wire shape. `#[schema(as)]`
    renames only the OpenAPI component — it does NOT affect serde / the wire
    format.
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    alert_bad: float | None = None
    alert_trigger: float | None = None
    good: float
    id: UUID
    is_locked: bool
    lock_reason: str | None = None
    locked_at: UnzonedDatetime | None = None
    locked_by: str | None = None
    metric_id: UUID = Field(..., description='UUIDv7 of the corresponding `metric_catalog` row.')
    role_slug: str | None = Field(None, description='Empty-string sentinel collapsed to `None` on the wire so the JSON\nshape is `null` instead of `""` (the latter would confuse FE\n"is this set?" predicates).')
    schema_error_code: str | None = Field(None, description='Canonical error code (`table_not_found | column_not_found |\nclickhouse_unreachable | unknown`) when `schema_status = "error"`,\notherwise omitted.')
    schema_status: str = Field(..., description='One of `ok | error | unchecked`, joined from `metric_catalog.schema_status`\n(DESIGN §3.3 "Schema status surface"). Lets the admin UI flag a\nbroken metric before the operator submits a write.')
    scope: Scope
    team_id: str | None = None
    tenant_id: UUID | None = Field(None, description='`Some(_)` for tenant-scoped rows, `None` for `product-default`.')
    warn: float


class BatchQueryItem(QueryRequest):
    model_config = ConfigDict(
        extra='forbid',
    )
    id: str | None = None
    metric_id: UUID


class BatchQueryRequest(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    queries: list[BatchQueryItem]


class BatchQueryResult1(QueryResponse):
    model_config = ConfigDict(
        extra='forbid',
    )
    id: str | None = None
    metric_id: UUID
    status: Status


class BatchQueryResult2(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    error: Problem
    id: str | None = None
    metric_id: UUID
    status: Status1


class BatchQueryResult(RootModel[BatchQueryResult1 | BatchQueryResult2]):
    root: BatchQueryResult1 | BatchQueryResult2


class BreakdownValueDto(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    dimensions: list[MetricDimensionDto]
    entity_id: str
    value: float | None = None


class CatalogResponse(BaseModel):
    """
    Top-level response body. `tenant_id` is echoed for client-side cache
    reasoning AND re-asserted on cache hydrate as defense in depth against a
    misconfigured cache backend serving a sibling tenant's payload.

    `links` carries the `metric_query_catalog` M:N mapping per ADR-003. The
    mapping is time/filter-invariant, so consumers cache it for the same TTL as
    the catalog itself; see [`MetricQueryLink`].
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    generated_at: UnzonedDatetime
    links: list[MetricQueryLink]
    metrics: list[MetricView]
    tenant_id: UUID


class ColumnListResponse(BaseModel):
    """
    Response envelope for `GET /v1/columns` and `GET /v1/columns/{table}`
    (`{ "items": [TableColumn] }`).

    Docs-only wrapper mirroring the inline `serde_json::json!` shape the
    handlers emit — gives the column-list endpoints a real OpenAPI schema.
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    items: list[TableColumn]


class CreateRequest(BaseModel):
    """
    `POST /v1/admin/metric-thresholds` body — create a new threshold row.

    `tenant_id` / `id` / `locked_by` / `locked_at` / `created_at` /
    `updated_at` are NOT accepted from the body. `deny_unknown_fields`
    enforces that at the serde layer.

    `role_slug` / `team_id` use `Option<String>` — `None` is the canonical
    empty-string sentinel (DESIGN §3.7 + `infra/cache/catalog_cache.rs::cache_field`).
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    alert_bad: float | None = None
    alert_trigger: float | None = None
    good: float
    is_locked: bool | None = None
    lock_reason: str | None = None
    metric_id: UUID
    role_slug: str | None = None
    scope: Scope
    team_id: str | None = None
    warn: float


class ListResponse(BaseModel):
    """
    `GET /v1/admin/metric-thresholds` response envelope.

    Wraps `items` in an object (instead of a bare array) so future
    additions (pagination cursor, count, generated-at) are additive and
    non-breaking. Mirrors the catalog read endpoint's envelope shape.
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    items: list[AdminMetricThresholdView]


class MetricDefinitionView(BaseModel):
    """
    One metric definition, display fields only.
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    description: str | None = None
    dimensions: list[str]
    direction: MetricDirection
    drilldown: MetricDrilldownCapability | None = None
    explanation: str | None = None
    format: MetricFormat
    is_enabled: bool
    label: str
    last_observed_date: date_aliased | None = Field(None, description="Newest `metric_date` ever observed across the definition's input\nmeasures; absent when no observation has ever been seen. Freshness\nsignal, orthogonal to `schema_status`.")
    metric_key: str
    schema_error_code: MetricSchemaErrorCode | None = None
    schema_status: SchemaStatus
    short_label: str | None = Field(None, description='Compact label for dense surfaces; absent when the full label is\nalready compact enough.')
    unit: str | None = None


class MetricDrilldownColumn(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    key: str
    label: str
    type: MetricDrilldownColumnType


class MetricDrilldownExportRequest(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    display_dimensions: list[str] | None = None
    entity: MetricDrilldownEntity
    filters: list[MetricDrilldownFilter] | None = None
    format: MetricDrilldownExportFormat
    metric_key: str
    period: MetricDrilldownPeriod


class MetricDrilldownResponse(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    columns: list[MetricDrilldownColumn]
    next_cursor: str | None = None
    rows: list[MetricDrilldownRow]
    selection: MetricDrilldownSelection


class MetricListResponse(BaseModel):
    """
    Response envelope for `GET /v1/metrics` (`{ "items": [MetricSummary] }`).

    Docs-only wrapper: the handler emits the same object shape via an inline
    `serde_json::json!` literal. Existing on the wire; this type just gives the
    list endpoint a real OpenAPI schema instead of a generic object.
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    items: list[MetricSummary]


class MetricRequest(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    filters: list[MetricDimensionFilterRequest] | None = None
    metric_key: str
    views: list[MetricViewRequest]


class MetricResultSelectionDto(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    entity: MetricResultsEntityDto
    filters: list[MetricDimensionFilterDto]
    metric_key: str
    period: MetricResultsPeriodDto


class MetricResultViewDto1(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    values: list[PeriodValueDto]
    view: View


class MetricResultViewDto3(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    values: list[PeerValueDto]
    view: View2


class MetricResultViewDto4(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    dimensions: list[str]
    values: list[BreakdownValueDto]
    view: View3


class MetricResultsRequest(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    entity: MetricResultsEntity
    metrics: list[MetricRequest]
    period: MetricResultsPeriod


class Person(BaseModel):
    """
    Person info returned by the Identity service.
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    department: str
    display_name: str
    division: str
    email: str
    first_name: str
    job_title: str
    last_name: str
    status: str
    subordinates: list[Subordinate]
    supervisor_email: str | None = None
    supervisor_name: str | None = None


class SavedQueryListResponse(BaseModel):
    """
    Response envelope for `GET /v1/queries` (`{ "items": [SavedQuerySummary] }`).
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    items: list[SavedQuerySummary]


class TimeseriesDto(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    dimensions: list[MetricDimensionDto]
    entity_id: str
    label: str | None = None
    points: list[TimeseriesPointDto]
    rank: int | None = Field(None, ge=0)
    remainder: bool | None = None
    total: float | None


class BatchQueryResponse(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    results: list[BatchQueryResult]


class MetricDefinitionListResponse(BaseModel):
    """
    Response body for `GET /v1/metric-definitions`. Metrics are sorted by
    `metric_key` ascending so the payload is byte-stable for caching and
    diff tooling.
    """
    model_config = ConfigDict(
        extra='forbid',
    )
    metrics: list[MetricDefinitionView]


class MetricResultViewDto2(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    bucket: Bucket
    series: list[TimeseriesDto]
    view: View1


class MetricResultViewDto(RootModel[MetricResultViewDto1 | MetricResultViewDto2 | MetricResultViewDto3 | MetricResultViewDto4 | MetricResultViewDto5]):
    root: MetricResultViewDto1 | MetricResultViewDto2 | MetricResultViewDto3 | MetricResultViewDto4 | MetricResultViewDto5


class MetricResultDto5(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    description: str | None = None
    direction: MetricDirection
    drilldown: MetricDrilldownCapability | None = None
    explanation: str | None = None
    format: MetricFormat
    label: str
    metric_key: str
    selection: MetricResultSelectionDto
    short_label: str | None = None
    unit: str | None = None
    views: list[MetricResultViewDto]


class MetricResultDto6(MetricResultDto1, MetricResultDto5):
    model_config = ConfigDict(
        extra='forbid',
    )


class MetricResultDto7(MetricResultDto2, MetricResultDto5):
    model_config = ConfigDict(
        extra='forbid',
    )


class MetricResultDto8(MetricResultDto3, MetricResultDto5):
    model_config = ConfigDict(
        extra='forbid',
    )


class MetricResultDto9(MetricResultDto4, MetricResultDto5):
    model_config = ConfigDict(
        extra='forbid',
    )


class MetricResultDto(RootModel[MetricResultDto6 | MetricResultDto7 | MetricResultDto8 | MetricResultDto9]):
    root: MetricResultDto6 | MetricResultDto7 | MetricResultDto8 | MetricResultDto9


class MetricResultsResponse(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
    )
    metrics: list[MetricResultDto]
