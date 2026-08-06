//! Operator identity corrections — write surface.
//!
//! Four verbs over the account-to-person binding: bind (single or bulk), merge,
//! detach, exclude. Each appends binding observations to `persons` under the
//! calling operator and journals the call in `operations`; nothing is updated or
//! deleted. Admin-gated like the rest of the operator surface.

use std::sync::Arc;

use axum::Json;
use axum::extract::Extension;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};
use toolkit_canonical_errors::CanonicalError;
use toolkit_security::SecurityContext;
use utoipa::ToSchema;
use uuid::Uuid;

use super::AppState;
use super::error::CorrectionError;
use super::gate::require_admin;
use crate::domain::resolution::{self, EXCLUDED_PERSON, Verb};
use crate::domain::seed::SourceAccountKey;
use crate::infra::db::{ops_repo, resolution_repo};

/// How many accounts one bulk call may carry — a prepared matching table is
/// pasted by a human, not streamed.
const MAX_BULK_ITEMS: usize = 1_000;

/// A source-native account, as named by the caller.
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct AccountRef {
    /// Connector type, e.g. `github`.
    pub source: String,
    /// Connector instance id.
    pub source_id: Uuid,
    /// Account id within that instance.
    pub id: String,
}

impl From<&AccountRef> for SourceAccountKey {
    fn from(r: &AccountRef) -> Self {
        Self {
            source_type: r.source.clone(),
            source_id: r.source_id,
            account_id: r.id.clone(),
        }
    }
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct BindItem {
    pub account: AccountRef,
    pub person_id: Uuid,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct BindRequest {
    /// One or more bindings; a prepared matching table is submitted as one call.
    pub bindings: Vec<BindItem>,
    #[serde(default)]
    pub comment: String,
}
impl toolkit::api::api_dto::RequestApiDto for BindRequest {}

#[derive(Debug, Deserialize, ToSchema)]
pub struct MergeRequest {
    /// The person being absorbed — its accounts move to the target.
    pub source_person_id: Uuid,
    /// The surviving person, named explicitly by the operator.
    pub target_person_id: Uuid,
    #[serde(default)]
    pub comment: String,
}
impl toolkit::api::api_dto::RequestApiDto for MergeRequest {}

#[derive(Debug, Deserialize, ToSchema)]
pub struct AccountRequest {
    pub account: AccountRef,
    #[serde(default)]
    pub comment: String,
}
impl toolkit::api::api_dto::RequestApiDto for AccountRequest {}

/// What happened to one requested account.
#[derive(Debug, Serialize, ToSchema)]
pub struct ItemResult {
    pub source: String,
    pub source_id: Uuid,
    pub account_id: String,
    /// `applied` — a binding observation was appended;
    /// `already_decided` — the same operator decision is already recorded.
    pub outcome: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CorrectionResponse {
    pub applied: usize,
    pub already_decided: usize,
    pub items: Vec<ItemResult>,
    /// Set by `detach`: the freshly minted person the account now belongs to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_person_id: Option<Uuid>,
}
impl toolkit::api::api_dto::ResponseApiDto for CorrectionResponse {}

/// `POST /v1/resolution/bind` — attach accounts to persons (also the confirm
/// act: binding an account to the person automation already gave it records the
/// operator's decision and clears it from review).
pub async fn bind(
    Extension(state): Extension<Arc<AppState>>,
    Extension(ctx): Extension<SecurityContext>,
    Json(req): Json<BindRequest>,
) -> Result<impl IntoResponse, CanonicalError> {
    let operator = require_admin(&state.db, &ctx).await?;
    let tenant = ctx.subject_tenant_id();

    reject_empty(req.bindings.is_empty(), "bindings")?;
    reject_oversized(req.bindings.len())?;

    // One correction per target person: rows of one call may name different
    // persons, and each person's rows need their own timestamp slots.
    let mut response = CorrectionResponse {
        applied: 0,
        already_decided: 0,
        items: Vec::with_capacity(req.bindings.len()),
        new_person_id: None,
    };

    for item in &req.bindings {
        let account = SourceAccountKey::from(&item.account);
        require_known_person(&state.db, tenant, item.person_id).await?;
        let outcome = apply_correction(
            &state,
            tenant,
            operator,
            &[account],
            item.person_id,
            Verb::Bind,
            &req.comment,
        )
        .await?;
        merge_into(&mut response, outcome);
    }

    Ok(Json(response))
}

/// `POST /v1/resolution/merge` — declare two persons one human; every account of
/// the absorbed person is rebound to the survivor.
pub async fn merge(
    Extension(state): Extension<Arc<AppState>>,
    Extension(ctx): Extension<SecurityContext>,
    Json(req): Json<MergeRequest>,
) -> Result<impl IntoResponse, CanonicalError> {
    let operator = require_admin(&state.db, &ctx).await?;
    let tenant = ctx.subject_tenant_id();

    if req.source_person_id == req.target_person_id {
        return Err(invalid(
            "target_person_id",
            "source and target are the same person",
        ));
    }
    require_known_person(&state.db, tenant, req.source_person_id).await?;
    require_known_person(&state.db, tenant, req.target_person_id).await?;

    let accounts = resolution_repo::accounts_of_person(&state.db, tenant, req.source_person_id)
        .await
        .map_err(|e| internal(&e, "failed to read the person's accounts"))?;

    let outcome = apply_correction(
        &state,
        tenant,
        operator,
        &accounts,
        req.target_person_id,
        Verb::Merge,
        &req.comment,
    )
    .await?;

    Ok(Json(outcome))
}

/// `POST /v1/resolution/detach` — declare that an account belongs to a different
/// human; the account moves to a freshly minted person.
pub async fn detach(
    Extension(state): Extension<Arc<AppState>>,
    Extension(ctx): Extension<SecurityContext>,
    Json(req): Json<AccountRequest>,
) -> Result<impl IntoResponse, CanonicalError> {
    let operator = require_admin(&state.db, &ctx).await?;
    let tenant = ctx.subject_tenant_id();

    let account = SourceAccountKey::from(&req.account);
    let new_person_id = Uuid::now_v7();

    let mut outcome = apply_correction(
        &state,
        tenant,
        operator,
        &[account],
        new_person_id,
        Verb::Detach,
        &req.comment,
    )
    .await?;
    outcome.new_person_id = Some(new_person_id);

    Ok(Json(outcome))
}

/// `POST /v1/resolution/exclude` — mark an account as not a human (bot, CI,
/// service account). It binds to the reserved excluded person.
pub async fn exclude(
    Extension(state): Extension<Arc<AppState>>,
    Extension(ctx): Extension<SecurityContext>,
    Json(req): Json<AccountRequest>,
) -> Result<impl IntoResponse, CanonicalError> {
    let operator = require_admin(&state.db, &ctx).await?;
    let tenant = ctx.subject_tenant_id();

    let account = SourceAccountKey::from(&req.account);
    let outcome = apply_correction(
        &state,
        tenant,
        operator,
        &[account],
        EXCLUDED_PERSON,
        Verb::Exclude,
        &req.comment,
    )
    .await?;

    Ok(Json(outcome))
}

/// Read the accounts' current bindings, build the rows a correction appends,
/// write them, and journal the call.
async fn apply_correction(
    state: &AppState,
    tenant: Uuid,
    operator: Uuid,
    accounts: &[SourceAccountKey],
    target_person_id: Uuid,
    verb: Verb,
    comment: &str,
) -> Result<CorrectionResponse, CanonicalError> {
    let current = resolution_repo::current_bindings(&state.db, tenant, accounts)
        .await
        .map_err(|e| internal(&e, "failed to read current bindings"))?;

    let pairs: Vec<_> = accounts
        .iter()
        .map(|a| (a, current.get(a).copied()))
        .collect();
    let rows = resolution::build_rows(
        pairs,
        target_person_id,
        operator,
        verb,
        chrono::Utc::now().naive_utc(),
    );

    if !rows.is_empty() {
        resolution_repo::append_bindings(&state.db, tenant, operator, &rows)
            .await
            .map_err(|e| internal(&e, "failed to append the correction"))?;
    }

    let appended: std::collections::HashSet<&SourceAccountKey> =
        rows.iter().map(|r| &r.account).collect();
    let items: Vec<ItemResult> = accounts
        .iter()
        .map(|a| ItemResult {
            source: a.source_type.clone(),
            source_id: a.source_id,
            account_id: a.account_id.clone(),
            outcome: if appended.contains(a) {
                "applied".to_owned()
            } else {
                "already_decided".to_owned()
            },
        })
        .collect();

    journal(
        state,
        tenant,
        operator,
        verb,
        target_person_id,
        comment,
        &items,
    )
    .await;

    Ok(CorrectionResponse {
        applied: rows.len(),
        already_decided: accounts.len() - rows.len(),
        items,
        new_person_id: None,
    })
}

/// Record the call in the operations journal. Journalling must never fail the
/// correction — the binding is already committed and is the source of truth.
async fn journal(
    state: &AppState,
    tenant: Uuid,
    operator: Uuid,
    verb: Verb,
    target_person_id: Uuid,
    comment: &str,
    items: &[ItemResult],
) {
    let summary = serde_json::json!({
        "applied": items.iter().filter(|i| i.outcome == "applied").count(),
        "already_decided": items.iter().filter(|i| i.outcome == "already_decided").count(),
    });
    let request = serde_json::json!({
        "verb": verb.reason_code(),
        "target_person_id": target_person_id,
        "comment": comment,
        "accounts": items.iter().map(|i| serde_json::json!({
            "source": i.source,
            "source_id": i.source_id,
            "account_id": i.account_id,
            "outcome": i.outcome,
        })).collect::<Vec<_>>(),
    });

    let operation_id = Uuid::now_v7();
    let journalled = async {
        ops_repo::enqueue(
            &state.db,
            operation_id,
            RESOLUTION_OP,
            tenant,
            operator,
            Some(&request.to_string()),
        )
        .await?;
        ops_repo::try_start(&state.db, operation_id).await?;
        ops_repo::complete(&state.db, operation_id, &summary.to_string()).await
    }
    .await;

    if let Err(e) = journalled {
        tracing::error!(error = %e, "identity correction: journalling failed");
    }
}

/// `operations.operation_type` for operator corrections.
pub const RESOLUTION_OP: &str = "identity-correction";

fn merge_into(response: &mut CorrectionResponse, outcome: CorrectionResponse) {
    response.applied += outcome.applied;
    response.already_decided += outcome.already_decided;
    response.items.extend(outcome.items);
}

async fn require_known_person(
    db: &sea_orm::DatabaseConnection,
    tenant: Uuid,
    person_id: Uuid,
) -> Result<(), CanonicalError> {
    let known = resolution_repo::person_exists(db, tenant, person_id)
        .await
        .map_err(|e| internal(&e, "failed to check the person"))?;
    if known {
        return Ok(());
    }
    Err(CorrectionError::not_found("person not found")
        .with_resource(person_id.to_string())
        .create())
}

fn reject_empty(is_empty: bool, field: &str) -> Result<(), CanonicalError> {
    if is_empty {
        return Err(invalid(field, "must not be empty"));
    }
    Ok(())
}

fn reject_oversized(len: usize) -> Result<(), CanonicalError> {
    if len > MAX_BULK_ITEMS {
        return Err(invalid(
            "bindings",
            &format!("at most {MAX_BULK_ITEMS} bindings per call"),
        ));
    }
    Ok(())
}

fn invalid(field: &str, message: &str) -> CanonicalError {
    CorrectionError::invalid_argument()
        .with_field_violation(field, message, "INVALID")
        .create()
}

fn internal(error: &anyhow::Error, message: &str) -> CanonicalError {
    tracing::error!(error = %error, "{message}");
    CanonicalError::internal(message).create()
}
