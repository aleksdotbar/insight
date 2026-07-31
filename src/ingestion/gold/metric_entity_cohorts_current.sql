{{ config(
    materialized='view',
    schema='insight',
    alias='metric_entity_cohorts_current',
    tags=['gold']
) }}

SELECT
    assumeNotNull(tenant_id) AS tenant_id,
    'person' AS entity_type,
    assumeNotNull(entity_id) AS entity_id,
    -- Canonical person for the cohort member (resolve_person_id macro);
    -- NULL = identity doesn't know the HR email. NOTE: this is a VIEW, so
    -- unlike the observation tables the join runs at query time (every peer
    -- request) — fine while the identity log is thousands of rows;
    -- materialize if that ever changes.
    {{ resolved_person_id_column() }},
    'org_unit' AS cohort_key,
    cohort_id
FROM (
    SELECT
        workspace_id AS tenant_id,
        lower(assumeNotNull(email)) AS entity_id,
        coalesce(
            nullIf(toString(org_unit_id), ''),
            nullIf(department_name, '')
        ) AS cohort_id
    FROM {{ ref('class_people') }}
    WHERE email IS NOT NULL
      AND email != ''
      AND workspace_id IS NOT NULL
      AND workspace_id != ''
    ORDER BY
        tenant_id,
        entity_id,
        coalesce(parseDateTimeBestEffortOrNull(toString(valid_from)), toDateTime('1970-01-01')) DESC,
        unique_key DESC
    LIMIT 1 BY tenant_id, entity_id
) AS people
{{ resolved_person_id_join('people') }}
WHERE tenant_id IS NOT NULL
  AND tenant_id != ''
  AND entity_id IS NOT NULL
  AND entity_id != ''
