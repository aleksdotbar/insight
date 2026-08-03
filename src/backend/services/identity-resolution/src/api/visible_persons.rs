use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use axum::Json;
use axum::extract::Extension;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};
use toolkit_canonical_errors::CanonicalError;
use toolkit_security::SecurityContext;
use utoipa::ToSchema;

use super::AppState;
use super::canonical_json::CanonicalJson;
use super::error::VisibilityError;
use super::gate::require_caller;
use crate::infra::db::{persons_repo, subchart_repo};

// One bound parameter per email, so the request bounds the query.
const MAX_EMAILS: usize = 1000;

#[derive(Debug, Deserialize, ToSchema)]
pub struct VisiblePersonsRequest {
    pub emails: Vec<String>,
}
impl toolkit::api::api_dto::RequestApiDto for VisiblePersonsRequest {}

#[derive(Debug, Serialize, ToSchema)]
pub struct VisiblePersonsResponse {
    pub visible: Vec<String>,
}
impl toolkit::api::api_dto::ResponseApiDto for VisiblePersonsResponse {}

pub async fn filter_visible_persons(
    Extension(state): Extension<Arc<AppState>>,
    Extension(ctx): Extension<SecurityContext>,
    CanonicalJson(req): CanonicalJson<VisiblePersonsRequest>,
) -> Result<impl IntoResponse, CanonicalError> {
    let caller = require_caller(&ctx)?;
    let tenant = ctx.subject_tenant_id();

    let requested = normalize_emails(&req.emails)?;

    let resolved = persons_repo::resolve_person_ids_by_emails(&state.db, tenant, &requested)
        .await
        .map_err(read_err)?;

    // INVARIANT: the collation matches loosely (case, accents, padding), so a
    // candidate may be a different person whose email merely compares equal.
    // Consumers key rows by the email bytes — drop any candidate not storing it.
    let mut candidates_by_index: BTreeMap<usize, Vec<uuid::Uuid>> = BTreeMap::new();
    for (index, person_id, stored_email) in resolved {
        let Some(requested_email) = requested.get(index) else {
            continue;
        };
        if !stored_email.trim().eq_ignore_ascii_case(requested_email) {
            continue;
        }
        candidates_by_index
            .entry(index)
            .or_default()
            .push(person_id);
    }

    let visible_indices = if subchart_repo::has_wildcard_grant(&state.db, tenant, caller)
        .await
        .map_err(read_err)?
    {
        candidates_by_index.keys().copied().collect()
    } else {
        let candidates = candidates_by_index
            .values()
            .flatten()
            .copied()
            .collect::<Vec<_>>();
        let visible = subchart_repo::visible_targets(
            &state.db,
            tenant,
            caller,
            &candidates,
            &state.config.org_chart_source_type,
        )
        .await
        .map_err(read_err)?
        .into_iter()
        .collect::<std::collections::HashSet<_>>();

        candidates_by_index
            .iter()
            .filter(|(_, ids)| ids.iter().any(|id| visible.contains(id)))
            .map(|(index, _)| *index)
            .collect::<Vec<_>>()
    };

    let visible = visible_indices
        .into_iter()
        .filter_map(|index| requested.get(index).cloned())
        .collect();

    Ok(Json(VisiblePersonsResponse { visible }))
}

fn normalize_emails(emails: &[String]) -> Result<Vec<String>, CanonicalError> {
    if emails.len() > MAX_EMAILS {
        return Err(invalid(&format!("at most {MAX_EMAILS} emails per request")));
    }

    let mut seen: HashSet<String> = HashSet::with_capacity(emails.len());
    let mut out: Vec<String> = Vec::with_capacity(emails.len());
    for email in emails {
        let email = email.trim();
        if email.is_empty() {
            continue;
        }
        if seen.insert(email.to_ascii_lowercase()) {
            out.push(email.to_owned());
        }
    }

    if out.is_empty() {
        return Err(invalid("emails must not be empty"));
    }

    Ok(out)
}

fn invalid(detail: &str) -> CanonicalError {
    VisibilityError::invalid_argument()
        .with_field_violation("emails", detail, "invalid_emails")
        .create()
}

#[expect(clippy::needless_pass_by_value, reason = "used directly as map_err")]
fn read_err(e: anyhow::Error) -> CanonicalError {
    tracing::error!(error = %e, "visibility check failed");
    CanonicalError::internal("failed to evaluate visibility").create()
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn blank_and_duplicate_emails_collapse_keeping_input_spelling() {
        let got = normalize_emails(&[
            "  Ada@Example.COM ".to_owned(),
            "ada@example.com".to_owned(),
            "   ".to_owned(),
            "bob@example.com".to_owned(),
            "   bob@example.com".to_owned(),
        ])
        .expect("a non-empty list");

        assert_eq!(
            got,
            vec!["Ada@Example.COM".to_owned(), "bob@example.com".to_owned()],
            "first spelling wins and case-variants are one entry"
        );
    }

    #[test]
    fn an_all_blank_list_is_rejected() {
        assert!(normalize_emails(&[String::new(), "  ".to_owned()]).is_err());
        assert!(normalize_emails(&[]).is_err());
    }

    #[test]
    fn more_emails_than_the_cap_are_rejected() {
        let many = (0..=MAX_EMAILS)
            .map(|i| format!("p{i}@example.com"))
            .collect::<Vec<_>>();
        assert!(normalize_emails(&many).is_err(), "over-cap rejected");

        let at_cap = (0..MAX_EMAILS)
            .map(|i| format!("p{i}@example.com"))
            .collect::<Vec<_>>();
        assert!(
            normalize_emails(&at_cap).is_ok(),
            "the cap itself is allowed"
        );
    }
}
