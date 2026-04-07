> **Status: DRAFT** — written before Phase 1 (Virtuoso screens) is implemented. Intended as the foundation for Phase 2. Revisit and refine after at least two real role examples are running.

## Why

The Virtuoso prototype proves the UI patterns for one role (engineering). As the product expands to sales, support, QA, and any other org function, hardcoding each role as a separate screenset becomes unsustainable. At the same time, the number of data sources (Jira, Bitbucket, Salesforce, Zendesk, GitHub, Linear, Slack, Zoom, M365, ...) will only grow. Without a shared contract between the data layer and the UI layer, every new role requires a full-stack implementation from scratch.

The solution is a **Metric Catalog** — a single registry that defines what metrics exist, where they come from, and how to compute their status. Dashboard configs reference metric keys from the catalog. The rendering engine is role-agnostic.

## What Changes

- Introduce **Metric Catalog**: a registry of all available metrics across all connectors
- Introduce **Connector Interface**: a standard contract that every data source adapter must implement
- Introduce **Dashboard Config schema**: a declarative format for defining dashboards in terms of metric keys
- Introduce **Config Rendering Engine**: a generic set of components (KpiStrip, BulletSection, ChartSection, TableSection) driven entirely by config — no role-specific code
- Introduce **Visual Configurator** (later phase): a UI for building and editing dashboard configs without code
- Migrate the three Virtuoso screens (executiveView, teamView, icDashboard) to the config-driven engine as the reference implementation

## Architecture

```
┌─ Data Layer ───────────────────────────────────────────────┐
│  Connector Interface                                        │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐      │
│  │  Jira    │ │Bitbucket │ │Salesforce│ │ Zendesk  │  ...  │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘      │
│       └────────────┴────────────┴─────────────┘            │
│                          ↓                                  │
│               Metric Catalog                                │
│         { metric_key → definition }                         │
└─────────────────────────┬──────────────────────────────────┘
                           │
┌─ Config Layer ───────────┼──────────────────────────────────┐
│                          ↓                                  │
│            Dashboard Config (per role)                      │
│         { sections → metric_key references }                │
└─────────────────────────┬──────────────────────────────────┘
                           │
┌─ UI Layer ───────────────┼──────────────────────────────────┐
│                          ↓                                  │
│           Config Rendering Engine                           │
│    KpiStrip | BulletSection | ChartSection | TableSection   │
│                          ↓                                  │
│           Visual Configurator (Phase 3)                     │
└─────────────────────────────────────────────────────────────┘
```

## Metric Catalog — Entry Schema

Each metric in the catalog is defined once:

```typescript
interface MetricDefinition {
  // Identity
  metric_key: string           // e.g. "tasks_completed", "deal_close_rate"
  label: string                // display name
  description: string          // what it measures

  // Data
  connector: ConnectorId       // "jira" | "bitbucket" | "salesforce" | "zendesk" | ...
  source_label: string         // human-readable source (e.g. "Jira · closed issues")
  value_type: 'count' | 'rate' | 'duration' | 'currency' | 'ratio'
  unit: string                 // "", "%", "h", "×", "$", ...

  // Status logic
  direction: 'higher_is_better' | 'lower_is_better' | 'range_target'
  target?: { min: number; max: number }  // for range_target
  warn_threshold: number       // % of median (e.g. 0.7 = 70%)
  bad_threshold: number        // % of median (e.g. 0.5 = 50%)

  // Period behaviour
  scales_with_period: boolean  // true for counts, false for rates/percentages

  // Drill
  drill_schema?: DrillSchema   // columns, source label, link template
}
```

## Dashboard Config — Schema

A dashboard config file defines a complete role dashboard:

```typescript
interface DashboardConfig {
  id: string                   // e.g. "engineering-ic", "sales-ae", "support-agent"
  name: string
  role_label: string           // shown in UI header

  kpi_strip: {
    metrics: string[]          // ordered metric_key list
  }

  sections: SectionConfig[]
}

type SectionConfig =
  | BulletSectionConfig
  | ChartSectionConfig
  | TableSectionConfig

interface BulletSectionConfig {
  type: 'bullets'
  id: string
  title: string
  collapsible: boolean
  layout: 'single' | 'two-column' | 'three-column' | { left: string[]; right: string[] }
  metrics: string[]            // metric_key references
}

interface ChartSectionConfig {
  type: 'chart'
  id: string
  title: string
  subtitle?: string
  collapsible: boolean
  chart_type: 'stacked_bar' | 'multi_line' | 'radar' | 'grouped_bar'
  series: { metric_key: string; color: string; label: string }[]
  granularity: 'period_driven'  // always adapts to active period
}

interface TableSectionConfig {
  type: 'table'
  id: string
  title: string
  collapsible: boolean
  row_entity: string            // e.g. "person", "team", "deal"
  row_link?: string             // route template e.g. "/ic/:person_id"
  columns: TableColumnConfig[]
}

interface TableColumnConfig {
  field: string
  label: string
  sublabel?: string
  unit?: string
  color_logic?: 'threshold' | 'status_field' | 'none'
  threshold?: { good: number; warn: number; direction: 'higher' | 'lower' }
}
```

## Connector Interface

Every data source adapter implements one interface:

```typescript
interface ConnectorAdapter {
  id: ConnectorId
  label: string

  // Fetch pre-computed metric values for a list of metric_keys
  // Returns one MetricValue per (entity_id × metric_key × period)
  fetchMetrics(params: {
    metric_keys: string[]
    entity_id: string
    period: Period
    date_range?: DateRange
  }): Promise<MetricValue[]>

  // Fetch drill-down rows for a metric
  fetchDrillData(params: {
    metric_key: string
    entity_id: string
    period: Period
  }): Promise<DrillRow[]>

  // Fetch time-series data for chart sections
  fetchTimeSeries(params: {
    metric_keys: string[]
    entity_id: string
    period: Period
  }): Promise<TimeSeriesPoint[]>
}
```

## Capabilities

### New Capabilities

- `metric-catalog`: Registry of all metric definitions across all connectors — the single source of truth for metric identity, status logic, and period behaviour
- `connector-interface`: Standard adapter contract that all data source integrations must implement; decouples the UI from any specific data source
- `dashboard-config-schema`: Declarative config format for defining role dashboards (KPI strip, bullet sections, chart sections, table sections) referencing metric keys from the catalog
- `config-rendering-engine`: Generic HAI3 components (KpiStrip, BulletSection, ChartSection, TableSection) that render any dashboard from a config + metric values — no role-specific code
- `visual-configurator`: (Phase 3) UI for building and editing dashboard configs without writing code; browses metric catalog, assembles sections, previews result

### Modified Capabilities

- `executive-view`: Migrate from hardcoded Virtuoso implementation to config-driven engine
- `team-view`: Migrate from hardcoded Virtuoso implementation to config-driven engine
- `ic-dashboard`: Migrate from hardcoded Virtuoso implementation to config-driven engine

## Impact

- New package or module: `metric-catalog/` — metric definitions, connector registry
- New package or module: `connectors/` — adapter implementations (jira, bitbucket, mock)
- `src/screensets/insight/uikit/composite/` — existing composites become the rendering engine primitives; must accept config-driven props
- `src/screensets/insight/api/InsightApiService` — replaced by connector adapters per metric
- New config files: `dashboards/engineering-ic.config.ts`, `dashboards/engineering-team.config.ts`, `dashboards/engineering-executive.config.ts`
- All future roles: one config file + connector metrics, no new screen components needed

## Phasing

| Phase | What | When |
|---|---|---|
| Phase 1 | Virtuoso screens hardcoded (current change) | Now |
| Phase 2 | Metric Catalog + Connector Interface + Config Schema | After Phase 1 ships |
| Phase 2b | Migrate Virtuoso screens to config engine | Immediately after Phase 2 |
| Phase 2c | Add second role (e.g. QA or Support) using config | Validates schema |
| Phase 3 | Visual Configurator UI | After schema is stable across 2+ roles |

## Open Questions

1. **Where does metric computation live?** Client-side (connector adapter computes on raw data) vs server-side (backend pre-computes all metrics). Server-side is more scalable but requires API investment. Client-side is faster to prototype.
2. **Config storage** — TypeScript files (type-safe, requires deploy to change) vs JSON in DB (editable at runtime, needed for visual configurator). Phase 2 = TS files. Phase 3 = DB.
3. **Status thresholds per team or global?** Currently team-median-relative. When there are many teams of different sizes/maturities, a global median may not be meaningful. Per-org calibration?
4. **Multi-connector metrics** — some metrics combine data from multiple sources (AI LOC share = Cursor + Claude Code). How does the catalog represent composite metrics?
5. **Access control** — which roles can see which dashboards? Who can edit configs in the visual configurator? Out of scope for Phase 2 but must be designed before Phase 3.
