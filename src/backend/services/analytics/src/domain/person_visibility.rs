use std::collections::HashSet;

use toolkit_canonical_errors::CanonicalError;
use toolkit_security::SecurityContext;
use uuid::Uuid;

use crate::api::error::MetricError;
use crate::infra::identity::IdentityClient;

const SERVICE_SUBJECT_TYPE: &str = "service";

#[derive(Debug)]
enum GatedEntity {
    Person,
    Unsupported,
}

impl GatedEntity {
    fn parse(entity_type: &str) -> Self {
        match entity_type {
            "person" => Self::Person,
            _ => Self::Unsupported,
        }
    }
}

pub(crate) fn normalize_person_id(entity_id: &str) -> String {
    entity_id.trim().to_ascii_lowercase()
}

pub(crate) async fn authorize_entity_ids(
    identity: &IdentityClient,
    ctx: &SecurityContext,
    authorization: Option<&str>,
    entity_type: &str,
    entity_ids: &[String],
) -> Result<(), CanonicalError> {
    if ctx.subject_type() == Some(SERVICE_SUBJECT_TYPE) {
        return Ok(());
    }

    match GatedEntity::parse(entity_type) {
        GatedEntity::Person => {
            authorize_person_ids(identity, ctx.subject_id(), authorization, entity_ids).await
        }
        GatedEntity::Unsupported => Err(no_authorization_rule(entity_type)),
    }
}

async fn authorize_person_ids(
    identity: &IdentityClient,
    caller: Uuid,
    authorization: Option<&str>,
    entity_ids: &[String],
) -> Result<(), CanonicalError> {
    if !identity.is_configured() {
        tracing::error!("identity service is not configured; person metrics cannot be authorized");
        return Err(unavailable());
    }

    if caller.is_nil() {
        tracing::error!("metric access attempted with no resolved caller");
        return Err(unavailable());
    }

    if authorization.is_none() {
        tracing::error!(caller = %caller, "no Authorization header to forward to identity");
        return Err(unavailable());
    }

    let visible = identity
        .visible_emails(entity_ids, authorization)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, caller = %caller, "visibility check failed");
            unavailable()
        })?;

    let unmatched = unmatched_ids(&visible, entity_ids);
    if unmatched.is_empty() {
        return Ok(());
    }

    Err(denied(caller, unmatched.len()))
}

fn unmatched_ids<'a>(visible: &HashSet<String>, entity_ids: &'a [String]) -> Vec<&'a str> {
    entity_ids
        .iter()
        .map(String::as_str)
        .filter(|entity_id| !visible.contains(*entity_id))
        .collect()
}

// INVARIANT: the gate's only 403 — identity answered "not visible". Anything
// that stops the check from running uses `unavailable` instead.
fn denied(caller: Uuid, unmatched: usize) -> CanonicalError {
    tracing::warn!(
        caller = %caller,
        unmatched,
        "metric access denied: requested entities outside the caller's visible set"
    );
    MetricError::permission_denied()
        .with_reason("entity_not_visible")
        .create()
}

fn no_authorization_rule(entity_type: &str) -> CanonicalError {
    tracing::error!(entity_type, "no authorization rule for this entity type");
    unavailable()
}

// `internal`, not `service_unavailable`: 500 is in the operation's declared
// response set.
fn unavailable() -> CanonicalError {
    CanonicalError::internal("metric access could not be authorized").create()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use axum::Router;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use axum::routing::post;

    use super::*;

    const CALLER: Uuid = Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0001);
    const TENANT: Uuid = Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0002);
    const SELF_EMAIL: &str = "self@example.com";
    const REPORT_EMAIL: &str = "report@example.com";
    const STRANGER_EMAIL: &str = "stranger@example.com";

    async fn spawn_identity(visible: &[&str]) -> IdentityClient {
        let visible = Arc::new(
            visible
                .iter()
                .map(|e| (*e).to_owned())
                .collect::<HashSet<String>>(),
        );

        let app = Router::new().route(
            "/v1/visible-persons",
            post(move |axum::Json(req): axum::Json<serde_json::Value>| {
                let visible = Arc::clone(&visible);
                async move {
                    let requested = req["emails"]
                        .as_array()
                        .map(|ids| {
                            ids.iter()
                                .filter_map(|v| v.as_str())
                                .map(str::to_owned)
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    let granted = requested
                        .into_iter()
                        .filter(|email| visible.contains(email))
                        .collect::<Vec<_>>();
                    axum::Json(serde_json::json!({"visible": granted}))
                }
            }),
        );

        IdentityClient::new(&serve(app).await).unwrap()
    }

    async fn spawn_failing_identity(status: StatusCode) -> IdentityClient {
        let app = Router::new().route(
            "/v1/visible-persons",
            post(move || async move { status.into_response() }),
        );
        IdentityClient::new(&serve(app).await).unwrap()
    }

    async fn serve(app: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    fn ctx_for(subject_type: &str, subject: Uuid) -> SecurityContext {
        SecurityContext::builder()
            .subject_id(subject)
            .subject_type(subject_type)
            .subject_tenant_id(TENANT)
            .build()
            .expect("subject and tenant are set")
    }

    fn status_of(result: Result<(), CanonicalError>) -> StatusCode {
        match result {
            Ok(()) => StatusCode::OK,
            Err(e) => e.into_response().status(),
        }
    }

    async fn authorize(
        identity: &IdentityClient,
        ctx: &SecurityContext,
        ids: &[&str],
    ) -> StatusCode {
        let entity_ids = ids.iter().map(|e| (*e).to_owned()).collect::<Vec<_>>();
        status_of(
            authorize_entity_ids(identity, ctx, Some("Bearer tok"), "person", &entity_ids).await,
        )
    }

    #[tokio::test]
    async fn ids_identity_reports_as_visible_are_admitted() {
        let identity = spawn_identity(&[SELF_EMAIL, REPORT_EMAIL]).await;
        let ctx = ctx_for("user", CALLER);

        assert_eq!(
            authorize(&identity, &ctx, &[SELF_EMAIL, REPORT_EMAIL]).await,
            StatusCode::OK,
        );
    }

    #[tokio::test]
    async fn person_outside_the_visible_set_is_forbidden() {
        let identity = spawn_identity(&[SELF_EMAIL]).await;
        let ctx = ctx_for("user", CALLER);

        assert_eq!(
            authorize(&identity, &ctx, &[STRANGER_EMAIL]).await,
            StatusCode::FORBIDDEN,
        );
    }

    #[tokio::test]
    async fn one_forbidden_id_rejects_the_whole_request() {
        let identity = spawn_identity(&[SELF_EMAIL]).await;
        let ctx = ctx_for("user", CALLER);

        assert_eq!(
            authorize(&identity, &ctx, &[SELF_EMAIL, STRANGER_EMAIL]).await,
            StatusCode::FORBIDDEN,
        );
    }

    #[tokio::test]
    async fn identity_outage_is_a_server_error_not_forbidden() {
        let identity = spawn_failing_identity(StatusCode::INTERNAL_SERVER_ERROR).await;
        let ctx = ctx_for("user", CALLER);

        assert_eq!(
            authorize(&identity, &ctx, &[SELF_EMAIL]).await,
            StatusCode::INTERNAL_SERVER_ERROR,
            "a dependency outage must not read as a denial",
        );
    }

    #[tokio::test]
    async fn an_identity_without_the_endpoint_is_a_server_error_not_forbidden() {
        for status in [StatusCode::NOT_FOUND, StatusCode::METHOD_NOT_ALLOWED] {
            let identity = spawn_failing_identity(status).await;
            let ctx = ctx_for("user", CALLER);

            assert_eq!(
                authorize(&identity, &ctx, &[SELF_EMAIL]).await,
                StatusCode::INTERNAL_SERVER_ERROR,
                "identity answering {status} must not read as a denial",
            );
        }
    }

    #[tokio::test]
    async fn unconfigured_identity_refuses_person_access() {
        let identity = IdentityClient::new("").unwrap();
        let ctx = ctx_for("user", CALLER);

        assert_eq!(
            authorize(&identity, &ctx, &[SELF_EMAIL]).await,
            StatusCode::INTERNAL_SERVER_ERROR,
            "without an authorization backend the gate fails closed",
        );
    }

    #[tokio::test]
    async fn service_subject_is_not_gated() {
        let identity = IdentityClient::new("").unwrap();
        let ctx = ctx_for("service", CALLER);

        assert_eq!(
            authorize(&identity, &ctx, &[STRANGER_EMAIL]).await,
            StatusCode::OK,
        );
    }

    #[tokio::test]
    async fn anonymous_caller_is_a_server_error_not_forbidden() {
        let identity = spawn_identity(&[SELF_EMAIL]).await;
        let ctx = ctx_for("user", Uuid::nil());

        assert_eq!(
            authorize(&identity, &ctx, &[SELF_EMAIL]).await,
            StatusCode::INTERNAL_SERVER_ERROR,
            "an unresolved caller is a broken authn path, not a denial"
        );
    }

    #[tokio::test]
    async fn a_request_without_a_bearer_to_forward_is_a_server_error_not_forbidden() {
        let identity = spawn_identity(&[SELF_EMAIL]).await;
        let ctx = ctx_for("user", CALLER);

        let status = status_of(
            authorize_entity_ids(&identity, &ctx, None, "person", &[SELF_EMAIL.to_owned()]).await,
        );
        assert_eq!(
            status,
            StatusCode::INTERNAL_SERVER_ERROR,
            "a broken authn path is not a visibility denial"
        );
    }

    #[tokio::test]
    async fn entity_type_without_an_authorization_rule_is_not_a_denial() {
        let identity = spawn_identity(&[SELF_EMAIL]).await;
        let ctx = ctx_for("user", CALLER);

        let status = status_of(
            authorize_entity_ids(
                &identity,
                &ctx,
                Some("Bearer tok"),
                "team",
                &["team-1".to_owned()],
            )
            .await,
        );
        assert_eq!(
            status,
            StatusCode::INTERNAL_SERVER_ERROR,
            "an entity type with no authorization rule is a server-side gap"
        );
    }

    #[test]
    fn person_ids_normalize_by_trimming_and_lowercasing() {
        for (input, expected) in [
            ("  Ada@Example.COM ", "ada@example.com"),
            ("ada@example.com", "ada@example.com"),
            ("   ", ""),
        ] {
            assert_eq!(
                normalize_person_id(input),
                expected,
                "should normalize: {input:?}"
            );
        }
    }
}
