"""API endpoint contract tests.

Together these modules exercise EVERY operation in the committed OpenAPI spec
(docs/components/backend/analytics/openapi.json) through the recording client,
so the endpoint-coverage gate needs no SKIP_LIST. One module per path group,
one test per (path, method, status-code) case:

  test_queries.py              GET+POST /v1/queries · GET+PUT+DELETE /v1/queries/{id}
                               POST /v1/queries/{id}/run
  test_metric_definitions.py   GET /v1/metric-definitions
  test_metric_results.py       POST /v1/metric-results
  test_metric_drilldown.py     POST /v1/metric-drilldown · POST /v1/metric-drilldown/export

Resources come from fixtures (`api/conftest.py`): a scratch saved query created
and deleted per test, and a tenant-scoped metric-definition override inserted
straight into MariaDB, so the catalog (the metric-coverage gate's universe) is
never touched. Per-op status-code coverage and the BLOCKED exclusions
(no-rate-limit 429 and `.standard_errors` boilerplate) live in
lib/api_coverage.py.
"""
