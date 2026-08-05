{# Reconciles a tracker issue-type name to the source-neutral `issue_kind`
   (bug / other / unknown) that gold reads to tell bug work from the rest.

   Matching is by name because no tracker exposes a locale-independent
   "is a bug" flag the way Jira exposes `statusCategory` for lifecycle. The
   name fed in must therefore be the most stable one the source has — Jira's
   `untranslatedName`, not the translated display name — so a localized
   instance classifies the same as an English one.

   An unrecognized name is `unknown`, never `other`: a customer-defined type
   is not evidence that the work was not a bug, and folding it into non-bug
   work would silently understate bugs. `unknown` is a first-class bucket that
   surfaces on screen. Instances whose type set does not use the default names
   extend the two vars below rather than patching this macro. #}

{% macro task_type_name_array(names) -%}
[{% for name in names %}'{{ name | lower }}'{{ ", " if not loop.last }}{% endfor %}]
{%- endmacro %}

{% macro task_issue_kind(name_expr) %}
{%- set normalized = "lower(trimBoth(ifNull(" ~ name_expr ~ ", '')))" -%}
multiIf(
    has({{ task_type_name_array(var('task_bug_type_names')) }}, {{ normalized }}), 'bug',
    has({{ task_type_name_array(var('task_non_bug_type_names')) }}, {{ normalized }}), 'other',
    'unknown'
)
{% endmacro %}
