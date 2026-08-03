{{ config(
    materialized='table',
    engine='MergeTree',
    order_by=['source_key', 'measure_key', 'entity_id', 'metric_date'],
    schema='insight',
    alias='wiki_metric_observations',
    tags=['gold'],
    query_settings={
        'max_memory_usage': 3221225472,
        'max_threads': 4,
        'max_bytes_before_external_group_by': 805306368,
        'max_bytes_before_external_sort': 805306368,
        'join_algorithm': 'grace_hash,hash'
    }
) }}

SELECT
    tenant_id,
    source_key,
    entity_type,
    -- entity_id arrives ALREADY canonical from evidence (resolved once per
    -- build); '' marks a row identity could not resolve, which stays out of
    -- every serving relation and is counted by identity_resolution_coverage.
    entity_id,
    metric_date,
    CAST(NULL AS Nullable(DateTime64(3))) AS observed_at,
    measure_key,
    toNullable(sum(contribution)) AS value,
    CAST(NULL AS Nullable(String)) AS subject_key,
    dimensions
FROM {{ ref('wiki_metric_evidence') }}
WHERE entity_id != ''
-- One person's several source accounts collapse into one canonical row.
GROUP BY tenant_id, source_key, entity_type, entity_id, metric_date, measure_key, dimensions
