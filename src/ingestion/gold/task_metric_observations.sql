{{ config(
    materialized='table',
    engine='MergeTree',
    order_by=['source_key', 'measure_key', 'entity_id', 'metric_date'],
    schema='insight',
    alias='task_metric_observations',
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
    entity_id,
    -- Canonical person from the identity log; NULL = unknown email (see
    -- macros/resolve_person_id.sql). entity_id stays the runtime key.
    {{ resolved_person_id_column() }},
    metric_date,
    CAST(NULL AS Nullable(DateTime64(3))) AS observed_at,
    measure_key,
    toNullable(sum(contribution)) AS value,
    CAST(NULL AS Nullable(String)) AS subject_key,
    dimensions
FROM {{ ref('task_metric_evidence') }}
{{ resolved_person_id_join("task_metric_evidence") }}
WHERE measure_key NOT IN ('dev_time_hours', 'resolution_days', 'pickup_days')
GROUP BY tenant_id, source_key, entity_type, entity_id, person_id, metric_date, measure_key, dimensions  -- person_id is functionally dependent on entity_id (one map row per email)

UNION ALL

SELECT
    tenant_id,
    source_key,
    entity_type,
    entity_id,
    -- Canonical person from the identity log; NULL = unknown email (see
    -- macros/resolve_person_id.sql). entity_id stays the runtime key.
    {{ resolved_person_id_column() }},
    metric_date,
    CAST(NULL AS Nullable(DateTime64(3))) AS observed_at,
    measure_key,
    contribution AS value,
    subject_key,
    dimensions
FROM {{ ref('task_metric_evidence') }}
{{ resolved_person_id_join("task_metric_evidence") }}
WHERE measure_key IN ('dev_time_hours', 'resolution_days', 'pickup_days')
