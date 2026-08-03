{{ config(
    materialized='table',
    engine='MergeTree',
    order_by=['source_key', 'measure_key', 'entity_id', 'metric_date'],
    schema='insight',
    alias='git_metric_observations',
    tags=['gold'],
    query_settings={
        'max_memory_usage': 1610612736,
        'max_threads': 4,
        'max_bytes_before_external_group_by': 805306368,
        'max_bytes_before_external_sort': 805306368
    }
) }}

SELECT
    tenant_id,
    source_key,
    entity_type,
    -- entity_id IS the canonical person id: `entity_type + entity_id`
    -- identifies the measured entity, so the resolved UUID goes in it rather
    -- than beside it. The source-native email stays in the evidence relations,
    -- which identity_resolution_coverage measures the resolution gap from.
    {{ canonical_entity_id() }},
    metric_date,
    CAST(NULL AS Nullable(DateTime64(3))) AS observed_at,
    measure_key,
    toNullable({{ collapsed_value('contribution', max_keys=['commit_day']) }}) AS value,
    CAST(NULL AS Nullable(String)) AS subject_key,
    dimensions
FROM {{ ref('git_metric_evidence') }}
{{ resolved_person_id_join("git_metric_evidence") }}
WHERE measure_key NOT IN ('commit_change_size', 'pr_cycle_hours', 'pr_change_size')
  AND {{ resolved_only() }}
-- Grouped on the join column, not the `entity_id` alias: an alias shadowing a
-- source column lands the aggregate in the outer scope (ILLEGAL_AGGREGATION).
-- One person's several source emails now collapse into one canonical row.
GROUP BY tenant_id, source_key, entity_type, identity_map.person_id, metric_date, measure_key, dimensions

UNION ALL

SELECT
    tenant_id,
    source_key,
    entity_type,
    -- entity_id IS the canonical person id: `entity_type + entity_id`
    -- identifies the measured entity, so the resolved UUID goes in it rather
    -- than beside it. The source-native email stays in the evidence relations,
    -- which identity_resolution_coverage measures the resolution gap from.
    {{ canonical_entity_id() }},
    metric_date,
    CAST(NULL AS Nullable(DateTime64(3))) AS observed_at,
    measure_key,
    contribution AS value,
    subject_key,
    dimensions
FROM {{ ref('git_metric_evidence') }}
{{ resolved_person_id_join("git_metric_evidence") }}
WHERE measure_key IN ('commit_change_size', 'pr_cycle_hours', 'pr_change_size')
  AND {{ resolved_only() }}
