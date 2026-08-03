{{ config(
    materialized='view',
    schema='insight',
    alias='identity_resolution_coverage',
    tags=['gold']
) }}

-- Identity-resolution match rate, measured over ACTIVITY (source rows), not
-- over aliases: one row per source with how much of its recorded work resolves
-- to a canonical person. This is the measuring device for the "match rate is
-- reported" outcome of the identity epic and the prioritization signal for
-- resolution-quality work — a source with a low rate is where identity is
-- missing that source's emails.
--
-- Read from the PRE-resolution relations, not from the observation tables:
-- since entity_id became the canonical person id, an unresolved source row has
-- no identity to be served under and never reaches gold. The evidence
-- relations keep every source row keyed by its source-native email, so the gap
-- stays measurable exactly where it is created.
--
-- Row-weighted on purpose: a person who produced 500 unresolved commits weighs
-- 500× a person with one — matching what the dashboards actually lose.
-- (`unresolved_people` counts the distinct unknown emails behind it — roughly
-- "how many operator decisions would close the gap".)
--
-- `hr_cohorts` is the peer-comparison membership (one row per person, not
-- activity): unresolved there means the HR email itself is unknown to
-- identity — usually a seeding gap, and it distorts peers for whole teams.

WITH source_rows AS (
    SELECT source_key, lower(trimBoth(entity_id)) AS source_entity_id
    FROM {{ ref('git_metric_evidence') }}

    UNION ALL

    SELECT source_key, lower(trimBoth(entity_id)) AS source_entity_id
    FROM {{ ref('ai_metric_evidence') }}

    UNION ALL

    SELECT source_key, lower(trimBoth(entity_id)) AS source_entity_id
    FROM {{ ref('collab_metric_evidence') }}

    UNION ALL

    SELECT source_key, lower(trimBoth(entity_id)) AS source_entity_id
    FROM {{ ref('task_metric_evidence') }}

    UNION ALL

    SELECT source_key, lower(trimBoth(entity_id)) AS source_entity_id
    FROM {{ ref('wiki_metric_evidence') }}

    UNION ALL

    SELECT
        'hr_cohorts' AS source_key,
        lower(assumeNotNull(email)) AS source_entity_id
    FROM {{ ref('class_people') }}
    WHERE email IS NOT NULL
      AND email != ''
)
SELECT
    source_key,
    count() AS observation_rows,
    countIf(identity_map.email = '') AS unresolved_rows,
    uniqExactIf(source_entity_id, identity_map.email = '') AS unresolved_people,
    round(100 * countIf(identity_map.email != '') / count(), 1) AS match_rate_pct
FROM source_rows
LEFT JOIN ({{ resolve_person_id() }}) AS identity_map
    ON identity_map.email = source_rows.source_entity_id
GROUP BY source_key
