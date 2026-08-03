{{ config(
    materialized='table',
    engine='MergeTree',
    order_by=['source_key', 'measure_key', 'entity_id', 'metric_date'],
    schema='insight',
    alias='ai_metric_observations',
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
    observed_at,
    measure_key,
    contribution AS value,
    subject_key,
    dimensions
FROM {{ ref('ai_metric_evidence') }}
{{ resolved_person_id_join("ai_metric_evidence") }}
WHERE {{ resolved_only() }}
  AND measure_key NOT IN ('active_day')

UNION ALL

-- Day flags collapse across a person's source aliases; every other measure
-- above stays one row per source row (additive measures are summed by the
-- runtime, which is correct across aliases).
SELECT
    tenant_id,
    source_key,
    entity_type,
    {{ canonical_entity_id() }},
    metric_date,
    CAST(NULL AS Nullable(DateTime64(3))) AS observed_at,
    measure_key,
    toNullable({{ collapsed_value('contribution', max_keys=['active_day']) }}) AS value,
    CAST(NULL AS Nullable(String)) AS subject_key,
    dimensions
FROM {{ ref('ai_metric_evidence') }}
{{ resolved_person_id_join("ai_metric_evidence") }}
WHERE {{ resolved_only() }}
  AND measure_key IN ('active_day')
GROUP BY tenant_id, source_key, entity_type, identity_map.person_id, metric_date, measure_key, dimensions
