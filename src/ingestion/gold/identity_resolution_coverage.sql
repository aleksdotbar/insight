{{ config(
    materialized='view',
    schema='insight',
    alias='identity_resolution_coverage',
    tags=['gold']
) }}

-- Identity-resolution match rate, measured over ACTIVITY (observation rows),
-- not over aliases: one row per source with how much of its recorded work
-- resolves to a canonical person. This is the measuring device for the
-- "match rate is reported" outcome of the identity epic (#1873) and the
-- prioritization signal for resolution-quality work — a source with a low
-- rate is where identity is missing that source's emails.
--
-- Row-weighted on purpose: a person who produced 500 unresolved commits
-- weighs 500× a person with one — matching what the dashboards actually
-- lose. (`unresolved_people` counts the distinct unknown emails behind it —
-- roughly "how many operator decisions would close the gap".)
--
-- `hr_cohorts` is the peer-comparison membership (one row per person, not
-- activity): unresolved there means the HR email itself is unknown to
-- identity — usually a seeding gap, and it distorts peers for whole teams.
--
-- A view: recomputed per query over the observation tables' person_id
-- column, so it is always exactly as fresh as the last gold build + the
-- identity sync that preceded it.

WITH observation_rows AS (
    SELECT source_key, entity_id, person_id
    FROM {{ ref('git_metric_observations') }}

    UNION ALL

    SELECT source_key, entity_id, person_id
    FROM {{ ref('ai_metric_observations') }}

    UNION ALL

    SELECT source_key, entity_id, person_id
    FROM {{ ref('collab_metric_observations') }}

    UNION ALL

    SELECT source_key, entity_id, person_id
    FROM {{ ref('task_metric_observations') }}

    UNION ALL

    SELECT source_key, entity_id, person_id
    FROM {{ ref('wiki_metric_observations') }}

    UNION ALL

    SELECT 'hr_cohorts' AS source_key, entity_id, person_id
    FROM {{ ref('metric_entity_cohorts_current') }}
)
SELECT
    source_key,
    count() AS observation_rows,
    countIf(person_id IS NULL) AS unresolved_rows,
    uniqExactIf(entity_id, person_id IS NULL) AS unresolved_people,
    round(100 * countIf(person_id IS NOT NULL) / count(), 1) AS match_rate_pct
FROM observation_rows
GROUP BY source_key
