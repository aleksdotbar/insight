//! Saved-query CRUD + run handlers — `/v1/queries*` (#1965).
//!
//! CRUD is plain metadata over the `saved_queries` service-DB table, mirroring
//! the metric CRUD in [`super::handlers`]. Every request is tenant-scoped from
//! the session `SecurityContext`. The `sql` is validated by the single-SELECT
//! gate on create, update, and run. Only `/run` reaches ClickHouse — it
//! executes the stored SQL as `presentation_ro` and returns untyped JSON rows.
//!
//! Phase-A scope: named parameters (`tenant`/`period`, #1966) and the injected
//! tenant-row filter (#1967) are separate sub-issues — `/run` here executes the
//! stored single-SELECT as authored.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Extension, Path};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use sea_orm::{ActiveModelTrait, ColumnTrait, EntityTrait, NotSet, QueryFilter, Set};
use toolkit_canonical_errors::CanonicalError;
use toolkit_security::SecurityContext;
use uuid::Uuid;

use super::AppState;
use super::error::SavedQueryError;
use crate::domain::query_gate::validate_single_select;
use crate::domain::saved_query::{
    CreateSavedQueryRequest, RunResponse, SavedQuery, SavedQuerySummary, UpdateSavedQueryRequest,
};
use crate::infra::db::entities::saved_queries;

// ── CRUD ────────────────────────────────────────────────────

pub async fn list_saved_queries(
    Extension(state): Extension<Arc<AppState>>,
    Extension(ctx): Extension<SecurityContext>,
) -> Result<impl IntoResponse, CanonicalError> {
    let rows = saved_queries::Entity::find()
        .filter(saved_queries::Column::InsightTenantId.eq(ctx.subject_tenant_id()))
        .all(&state.db)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "failed to list saved queries");
            CanonicalError::internal("failed to list saved queries").create()
        })?;

    let items: Vec<SavedQuerySummary> = rows.into_iter().map(model_to_summary).collect();
    Ok(Json(serde_json::json!({ "items": items })))
}

pub async fn get_saved_query(
    Extension(state): Extension<Arc<AppState>>,
    Extension(ctx): Extension<SecurityContext>,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, CanonicalError> {
    let row = find_saved_query(&state, ctx.subject_tenant_id(), id).await?;
    Ok(Json(model_to_saved_query(row)))
}

pub async fn create_saved_query(
    Extension(state): Extension<Arc<AppState>>,
    Extension(ctx): Extension<SecurityContext>,
    Json(req): Json<CreateSavedQueryRequest>,
) -> Result<impl IntoResponse, CanonicalError> {
    validate_single_select(&req.sql).map_err(invalid_sql)?;

    let id = Uuid::now_v7();
    let model = saved_queries::ActiveModel {
        id: Set(id),
        insight_tenant_id: Set(ctx.subject_tenant_id()),
        name: Set(req.name),
        description: Set(req.description),
        sql: Set(req.sql),
        created_at: NotSet,
        updated_at: NotSet,
    };

    saved_queries::Entity::insert(model)
        .exec(&state.db)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "failed to create saved query");
            CanonicalError::internal("failed to create saved query").create()
        })?;

    let row = find_saved_query(&state, ctx.subject_tenant_id(), id).await?;
    Ok((StatusCode::CREATED, Json(model_to_saved_query(row))))
}

pub async fn update_saved_query(
    Extension(state): Extension<Arc<AppState>>,
    Extension(ctx): Extension<SecurityContext>,
    Path(id): Path<Uuid>,
    Json(req): Json<UpdateSavedQueryRequest>,
) -> Result<impl IntoResponse, CanonicalError> {
    let existing = find_saved_query(&state, ctx.subject_tenant_id(), id).await?;
    let mut model: saved_queries::ActiveModel = existing.into();

    if let Some(name) = req.name {
        model.name = Set(name);
    }
    // Explicit null clears description; absent field leaves it unchanged.
    if let Some(desc) = req.description {
        model.description = Set(desc);
    }
    if let Some(sql) = req.sql {
        validate_single_select(&sql).map_err(|e| invalid_sql_for(id, e))?;
        model.sql = Set(sql);
    }
    model.updated_at = Set(chrono::Utc::now());

    let updated = model.update(&state.db).await.map_err(|e| {
        tracing::error!(error = %e, "failed to update saved query");
        CanonicalError::internal("failed to update saved query").create()
    })?;

    Ok(Json(model_to_saved_query(updated)))
}

pub async fn delete_saved_query(
    Extension(state): Extension<Arc<AppState>>,
    Extension(ctx): Extension<SecurityContext>,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, CanonicalError> {
    let existing = find_saved_query(&state, ctx.subject_tenant_id(), id).await?;

    saved_queries::Entity::delete_by_id(existing.id)
        .exec(&state.db)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "failed to delete saved query");
            CanonicalError::internal("failed to delete saved query").create()
        })?;

    Ok(StatusCode::NO_CONTENT)
}

// ── Run ─────────────────────────────────────────────────────

pub async fn run_saved_query(
    Extension(state): Extension<Arc<AppState>>,
    Extension(ctx): Extension<SecurityContext>,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, CanonicalError> {
    let saved = find_saved_query(&state, ctx.subject_tenant_id(), id).await?;

    // Re-validate on run: the gate is the write-side barrier, but stored SQL is
    // gated again here so a run can never reach ClickHouse with anything but a
    // single read (defense in depth alongside the `presentation_ro` grants).
    validate_single_select(&saved.sql).map_err(|e| invalid_sql_for(id, e))?;

    // #1966 (named params) and #1967 (injected tenant-row filter) extend this
    // path; Phase-A #1965 executes the stored single-SELECT as authored.
    let rows = execute_read(&state, &saved.sql).await?;
    Ok(Json(RunResponse { rows }))
}

/// Execute a single read statement against ClickHouse and parse the
/// `JSONEachRow` stream into untyped rows — the same read path the metric query
/// uses.
async fn execute_read(
    state: &AppState,
    sql: &str,
) -> Result<Vec<serde_json::Value>, CanonicalError> {
    tracing::debug!(sql = %sql, "executing saved query");

    let mut cursor = state
        .ch
        .query(sql)
        .fetch_bytes("JSONEachRow")
        .map_err(|e| {
            tracing::error!(error = %e, sql = %sql, "ClickHouse query failed");
            CanonicalError::internal("query execution failed").create()
        })?;

    let raw_bytes = cursor.collect().await.map_err(|e| {
        tracing::error!(error = %e, sql = %sql, "ClickHouse fetch failed");
        CanonicalError::internal("query execution failed").create()
    })?;

    if raw_bytes.is_empty() {
        return Ok(Vec::new());
    }

    raw_bytes
        .split(|&b| b == b'\n')
        .filter(|line| !line.is_empty())
        .map(serde_json::from_slice)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            tracing::error!(error = %e, "failed to parse ClickHouse JSON response");
            CanonicalError::internal("failed to parse query results").create()
        })
}

// ── Helpers ─────────────────────────────────────────────────

async fn find_saved_query(
    state: &AppState,
    tenant_id: Uuid,
    id: Uuid,
) -> Result<saved_queries::Model, CanonicalError> {
    saved_queries::Entity::find_by_id(id)
        .filter(saved_queries::Column::InsightTenantId.eq(tenant_id))
        .one(&state.db)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "failed to find saved query");
            CanonicalError::internal("failed to find saved query").create()
        })?
        .ok_or_else(|| {
            SavedQueryError::not_found("saved query not found")
                .with_resource(id.to_string())
                .create()
        })
}

fn invalid_sql(reason: String) -> CanonicalError {
    SavedQueryError::invalid_argument()
        .with_field_violation("sql", reason, "INVALID")
        .create()
}

fn invalid_sql_for(id: Uuid, reason: String) -> CanonicalError {
    SavedQueryError::invalid_argument()
        .with_resource(id.to_string())
        .with_field_violation("sql", reason, "INVALID")
        .create()
}

fn model_to_saved_query(m: saved_queries::Model) -> SavedQuery {
    SavedQuery {
        id: m.id,
        insight_tenant_id: m.insight_tenant_id,
        name: m.name,
        description: m.description,
        sql: m.sql,
        created_at: m.created_at.naive_utc(),
        updated_at: m.updated_at.naive_utc(),
    }
}

fn model_to_summary(m: saved_queries::Model) -> SavedQuerySummary {
    SavedQuerySummary {
        id: m.id,
        name: m.name,
        description: m.description,
    }
}
