use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use axum::Json;
use axum::extract::Extension;
use tokio::sync::Semaphore;
use toolkit_canonical_errors::CanonicalError;
use toolkit_security::SecurityContext;

use super::AppState;
use crate::api::error::MetricError;
use crate::domain::metric_drilldown::{
    EVIDENCE_QUERY_MEMORY_BYTES, EVIDENCE_QUERY_READ_BYTES, EVIDENCE_QUERY_RESULT_BYTES,
    EVIDENCE_QUERY_TIMEOUT_SECS, EvidenceQueryRow, MetricDrilldownRequest, MetricDrilldownResponse,
    build_response, compile_query, decode_evidence_rows, evidence_unavailable, validate_request,
    verify_evidence_snapshot,
};

const QUERY_TIMEOUT: Duration = Duration::from_secs(EVIDENCE_QUERY_TIMEOUT_SECS);
const QUERY_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_CONCURRENT_QUERIES: usize = 8;
static QUERY_SEMAPHORE: LazyLock<Semaphore> =
    LazyLock::new(|| Semaphore::new(MAX_CONCURRENT_QUERIES));

pub async fn query_metric_drilldown(
    Extension(state): Extension<Arc<AppState>>,
    Extension(ctx): Extension<SecurityContext>,
    Json(req): Json<MetricDrilldownRequest>,
) -> Result<Json<MetricDrilldownResponse>, CanonicalError> {
    let started = Instant::now();
    let req = validate_request(&state.db, &state.ch, ctx.subject_tenant_id(), req).await?;
    let log_comment = format!("metric-drilldown:page:{}", req.plan.definition.key());
    let rows = fetch_rows(&state, &req, &log_comment).await?;
    verify_evidence_snapshot(&state.ch, &req.plan.relation, &req.snapshot_id).await?;
    let fetched_rows = rows.len();
    let response = build_response(&req, rows)?;
    tracing::info!(
        duration_ms = started.elapsed().as_millis(),
        rows = response.rows.len(),
        fetched_rows,
        limit = req.limit,
        has_next_page = response.next_cursor.is_some(),
        "metric drilldown page completed"
    );
    Ok(Json(response))
}

async fn fetch_rows(
    state: &Arc<AppState>,
    req: &crate::domain::metric_drilldown::ValidatedMetricDrilldown,
    log_comment: &str,
) -> Result<Vec<EvidenceQueryRow>, CanonicalError> {
    // INVARIANT: the permit is held across the awaited ClickHouse execution and
    // byte collection below — the hold is the MAX_CONCURRENT_QUERIES cap.
    let _permit = tokio::time::timeout(QUERY_ACQUIRE_TIMEOUT, QUERY_SEMAPHORE.acquire())
        .await
        .map_err(|_| query_busy())?
        .map_err(|_| query_busy())?;
    let (sql, params) = compile_query(req)?;
    let mut query = state
        .ch
        .query(&sql)
        .with_option("log_comment", log_comment)
        .with_option("max_execution_time", QUERY_TIMEOUT.as_secs().to_string())
        .with_option("max_threads", "2")
        .with_option("max_memory_usage", EVIDENCE_QUERY_MEMORY_BYTES.to_string())
        .with_option("max_bytes_to_read", EVIDENCE_QUERY_READ_BYTES.to_string())
        .with_option("max_result_bytes", EVIDENCE_QUERY_RESULT_BYTES.to_string());
    for param in params {
        query = query.bind(param);
    }
    let mut cursor = query.fetch_bytes("JSONEachRow").map_err(|error| {
        tracing::error!(error = %error, "ClickHouse metric drilldown query failed");
        query_error(&error.to_string())
    })?;
    let bytes = tokio::time::timeout(QUERY_TIMEOUT, cursor.collect())
        .await
        .map_err(|_| {
            tracing::error!("metric evidence query exceeded the execution time limit");
            query_limit_error()
        })?
        .map_err(|error| {
            tracing::error!(error = %error, "ClickHouse metric drilldown fetch failed");
            query_error(&error.to_string())
        })?;

    decode_evidence_rows(&bytes).map_err(|error| {
        tracing::error!(error = %error, "metric drilldown row decoding failed");
        CanonicalError::internal("failed to decode metric evidence").create()
    })
}

fn query_error(message: &str) -> CanonicalError {
    if message.contains("UNKNOWN_TABLE") || message.contains("Code: 60") {
        return evidence_unavailable();
    }
    if is_clickhouse_resource_limit(message) {
        return query_limit_error();
    }
    CanonicalError::internal("metric evidence query failed").create()
}

fn query_limit_error() -> CanonicalError {
    MetricError::resource_exhausted("Metric evidence query exceeded resource limits.")
        .with_quota_violation("metric evidence query", "ClickHouse resource limit reached")
        .create()
}

fn is_clickhouse_resource_limit(message: &str) -> bool {
    [
        "MEMORY_LIMIT_EXCEEDED",
        "TOO_MANY_SIMULTANEOUS_QUERIES",
        "TOO_MANY_ROWS_OR_BYTES",
        "QUOTA_EXCEEDED",
        "LIMIT_EXCEEDED",
        "TIMEOUT_EXCEEDED",
        "Code: 159",
        "Code: 201",
        "Code: 202",
        "Code: 241",
    ]
    .iter()
    .any(|marker| message.contains(marker))
}

fn query_busy() -> CanonicalError {
    MetricError::resource_exhausted("Metric evidence query capacity is busy.")
        .with_quota_violation("metric evidence queries", "concurrency limit reached")
        .with_quota_violation_retry_after_seconds(QUERY_ACQUIRE_TIMEOUT.as_secs())
        .create()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_errors_are_classified() {
        assert!(is_clickhouse_resource_limit("MEMORY_LIMIT_EXCEEDED"));
        assert!(is_clickhouse_resource_limit("Code: 241"));
        assert!(!is_clickhouse_resource_limit("syntax error"));
        let missing = query_error("UNKNOWN_TABLE");
        let limited = query_error("QUOTA_EXCEEDED");
        let internal = query_error("syntax error");
        assert_eq!(missing.status_code(), axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(
            limited.status_code(),
            axum::http::StatusCode::TOO_MANY_REQUESTS
        );
        assert_eq!(
            internal.status_code(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        );
    }
}
