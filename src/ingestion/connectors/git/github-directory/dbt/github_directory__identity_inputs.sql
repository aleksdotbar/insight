{{ config(
    materialized='incremental',
    incremental_strategy='append',
    schema='staging',
    tags=['github_directory', 'silver', 'silver:identity_inputs']
) }}

-- Identity-resolution inputs for the GitHub account roster; unioned into
-- silver.identity_inputs via the `silver:identity_inputs` tag. Mirrors
-- youtrack__identity_inputs.
--
-- source_type is 'github', not 'github-directory': it must equal the
-- authenticator's `idp.source_type`, which names the vendor a person
-- authenticated against, not the connector package that supplied the roster.
--
-- The canonical `value_type='id'` binding row is emitted by the macro from
-- entity_id (= lowercased login, ADR-0002) — that is the row the login
-- lookup matches. `email` is null for most members unless the token carries
-- `user:email`, so resolution leans on the id binding and display_name.
--
-- No deactivation condition applies: GitHub exposes no per-member disabled
-- flag, a removed member simply stops appearing in the full-refresh roster.
-- The condition below is intentionally unsatisfiable rather than absent so
-- the macro's DELETE branch stays wired and a real signal can replace it.

{{ identity_inputs_from_history(
    fields_history_ref=ref('github_directory__org_members_fields_history'),
    source_type='github',
    identity_fields=[
        {'field': 'email', 'value_type': 'email',        'value_field_name': 'bronze_github_directory.org_members.email'},
        {'field': 'login', 'value_type': 'username',     'value_field_name': 'bronze_github_directory.org_members.login'},
        {'field': 'name',  'value_type': 'display_name', 'value_field_name': 'bronze_github_directory.org_members.name'},
    ],
    deactivation_condition="field_name = 'login' AND new_value = ''"
) }}
