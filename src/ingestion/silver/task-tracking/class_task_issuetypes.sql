-- depends_on: {{ ref('jira__task_issuetypes') }}
-- depends_on: {{ ref('youtrack__task_issuetypes') }}
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='unique_key',
    schema='silver',
    engine='ReplacingMergeTree(_version)',
    order_by=['unique_key'],
    settings={'allow_nullable_key': 1},
    tags=['silver']
) }}

-- Unified, source-neutral issue-type dimension: one row per (source issue type
-- id), carrying the reconciled `issue_kind` (bug / other / unknown) and the type
-- display name. Each per-source projection tagged `silver:class_task_issuetypes`
-- (jira__task_issuetypes, youtrack__task_issuetypes) reconciles its native type
-- naming to the SAME enum through the shared `task_issue_kind` macro, so after
-- the union there is no cross-source divergence. Gold tells bug work from the
-- rest with `issue_kind = 'bug'`, never a localized type name — the same
-- treatment `class_task_statuses` gives the lifecycle.
--
-- A type name in neither var list is `unknown`, which Gold reports as its own
-- bucket rather than folding it into non-bug work.

SELECT * FROM (
    {{ union_by_tag('silver:class_task_issuetypes') }}
)
{% if is_incremental() %}
WHERE _version > (SELECT max(_version) FROM {{ this }})
{% endif %}
