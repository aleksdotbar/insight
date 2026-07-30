//! Saved-query domain model — the `presentation.queries` entity (#1965).
//!
//! A saved query is a single `SELECT`/`WITH` over the read-only contract,
//! authored by an analyst and tenant-scoped. It is stored as service-DB
//! metadata (like [`super::metric`]); the `sql` is validated by the query gate
//! on write and on run. Only `/run` reaches ClickHouse, executing as
//! `presentation_ro`.

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A saved query row.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct SavedQuery {
    pub id: Uuid,
    pub insight_tenant_id: Uuid,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub sql: String,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

/// Summary returned by the list endpoint (no `sql` body).
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct SavedQuerySummary {
    pub id: Uuid,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Response envelope for `GET /v1/queries` (`{ "items": [SavedQuerySummary] }`).
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct SavedQueryListResponse {
    pub items: Vec<SavedQuerySummary>,
}

/// Request to create a saved query.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct CreateSavedQueryRequest {
    pub name: String,
    pub description: Option<String>,
    pub sql: String,
}

/// Request to update a saved query.
///
/// `description` uses double-Option (absent → unchanged, `null` → clear,
/// value → set), matching [`super::metric::UpdateMetricRequest`].
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct UpdateSavedQueryRequest {
    pub name: Option<String>,
    #[allow(clippy::option_option)] // intentional: absent vs null vs value for PATCH semantics
    #[serde(default, deserialize_with = "deserialize_optional_nullable")]
    pub description: Option<Option<String>>,
    pub sql: Option<String>,
}

/// Result of `POST /v1/queries/{id}/run`.
///
/// `rows` carry a per-query dynamic schema (the `SELECT` columns vary), so each
/// row is an untyped JSON object — the same shape as the metric query path.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct RunResponse {
    pub rows: Vec<serde_json::Value>,
}

/// Deserialize a field that can be absent, null, or a value.
#[allow(clippy::option_option)] // intentional: triple-state for PATCH semantics
fn deserialize_optional_nullable<'de, D>(
    deserializer: D,
) -> Result<Option<Option<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Some(Option::deserialize(deserializer)?))
}

impl toolkit::api::api_dto::ResponseApiDto for SavedQuery {}
impl toolkit::api::api_dto::ResponseApiDto for SavedQuerySummary {}
impl toolkit::api::api_dto::ResponseApiDto for SavedQueryListResponse {}
impl toolkit::api::api_dto::ResponseApiDto for RunResponse {}
impl toolkit::api::api_dto::RequestApiDto for CreateSavedQueryRequest {}
impl toolkit::api::api_dto::RequestApiDto for UpdateSavedQueryRequest {}
