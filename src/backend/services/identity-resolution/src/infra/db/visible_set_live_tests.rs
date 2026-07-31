//! INVARIANT: never `#[ignore]` these — the identity CI job runs `cargo test`
//! without `--include-ignored`, so an ignored case silently stops running.
//! INVARIANT: [`FIXTURE_REASON`] must differ from `e2e-seed` — the e2e seeder
//! deletes by reason with no tenant filter and would wipe fixtures mid-run.

use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement, Value};
use uuid::Uuid;

use super::{connect_single, persons_repo, roles_repo, subchart_repo};

const ENV_VAR: &str = "INTEGRATION_TESTS_MARIADB_URL";
const FIXTURE_REASON: &str = "visible-set-live-test";
const SOURCE_TYPE: &str = "bamboohr";

type TestResult = anyhow::Result<()>;

struct Fixture {
    db: DatabaseConnection,
    tenant: Uuid,
    source_id: Uuid,
}

async fn fixture_or_skip() -> anyhow::Result<Option<Fixture>> {
    let Ok(url) = std::env::var(ENV_VAR) else {
        eprintln!("skip: set {ENV_VAR} to run");
        return Ok(None);
    };
    Ok(Some(Fixture {
        db: connect_single(&url).await?,
        tenant: Uuid::now_v7(),
        source_id: Uuid::now_v7(),
    }))
}

impl Fixture {
    async fn person(&self, email: &str) -> anyhow::Result<Uuid> {
        let person_id = Uuid::now_v7();
        self.exec(
            "INSERT INTO persons (value_type, insight_source_type, insight_source_id,
                 insight_tenant_id, value_id, person_id, author_person_id, reason)
             VALUES ('email', ?, ?, ?, ?, ?, ?, ?)",
            [
                SOURCE_TYPE.into(),
                bytes(self.source_id),
                bytes(self.tenant),
                email.into(),
                bytes(person_id),
                bytes(person_id),
                FIXTURE_REASON.into(),
            ],
        )
        .await?;
        Ok(person_id)
    }

    async fn duplicate_of(&self, email: &str) -> anyhow::Result<Uuid> {
        let person_id = Uuid::now_v7();
        self.exec(
            "INSERT INTO persons (value_type, insight_source_type, insight_source_id,
                 insight_tenant_id, value_id, person_id, author_person_id, reason)
             VALUES ('email', ?, ?, ?, ?, ?, ?, ?)",
            [
                "ms-entra".into(),
                bytes(Uuid::now_v7()),
                bytes(self.tenant),
                email.into(),
                bytes(person_id),
                bytes(person_id),
                FIXTURE_REASON.into(),
            ],
        )
        .await?;
        Ok(person_id)
    }

    async fn reports_to(&self, child: Uuid, parent: Uuid) -> TestResult {
        self.exec(
            "INSERT INTO org_chart (insight_tenant_id, insight_source_type, insight_source_id,
                 child_person_id, parent_person_id, author_person_id, reason, valid_from)
             VALUES (?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(6))",
            [
                bytes(self.tenant),
                SOURCE_TYPE.into(),
                bytes(self.source_id),
                bytes(child),
                bytes(parent),
                bytes(parent),
                FIXTURE_REASON.into(),
            ],
        )
        .await
    }

    async fn grant(&self, viewer: Uuid, target: Option<Uuid>) -> TestResult {
        self.exec(
            "INSERT INTO visibility (visibility_id, insight_tenant_id, viewer_person_id,
                 viewed_person_id, valid_from, author_person_id, reason)
             VALUES (?, ?, ?, ?, UTC_TIMESTAMP(6), ?, ?)",
            [
                bytes(Uuid::now_v7()),
                bytes(self.tenant),
                bytes(viewer),
                target.map_or(Value::Bytes(None), bytes),
                bytes(viewer),
                FIXTURE_REASON.into(),
            ],
        )
        .await
    }

    async fn make_admin(&self, person_id: Uuid) -> TestResult {
        self.exec(
            "INSERT INTO person_roles (person_role_id, insight_tenant_id, person_id, role_id,
                 valid_from, author_person_id, reason)
             VALUES (?, ?, ?, ?, UTC_TIMESTAMP(6), ?, ?)",
            [
                bytes(Uuid::now_v7()),
                bytes(self.tenant),
                bytes(person_id),
                bytes(roles_repo::ADMIN_ROLE_ID),
                bytes(person_id),
                FIXTURE_REASON.into(),
            ],
        )
        .await
    }

    async fn visible(&self, viewer: Uuid, candidates: &[Uuid]) -> anyhow::Result<Vec<Uuid>> {
        subchart_repo::visible_targets(&self.db, self.tenant, viewer, candidates, SOURCE_TYPE).await
    }

    async fn exec(&self, sql: &str, values: impl IntoIterator<Item = Value>) -> anyhow::Result<()> {
        self.db
            .execute(Statement::from_sql_and_values(
                DbBackend::MySql,
                sql,
                values,
            ))
            .await?;
        Ok(())
    }
}

fn bytes(id: Uuid) -> Value {
    id.as_bytes().to_vec().into()
}

#[tokio::test]
async fn caller_without_reports_still_sees_themselves() -> TestResult {
    let Some(f) = fixture_or_skip().await? else {
        return Ok(());
    };
    let leaf = f.person("leaf@visible-set.test").await?;
    let stranger = f.person("stranger@visible-set.test").await?;

    let visible = f.visible(leaf, &[leaf, stranger]).await?;

    assert_eq!(visible, vec![leaf], "self is visible, the stranger is not");
    Ok(())
}

#[tokio::test]
async fn manager_sees_a_transitive_descendant_but_not_an_unrelated_person() -> TestResult {
    let Some(f) = fixture_or_skip().await? else {
        return Ok(());
    };
    let top = f.person("top@visible-set.test").await?;
    let mid = f.person("mid@visible-set.test").await?;
    let deep = f.person("deep@visible-set.test").await?;
    let stranger = f.person("stranger@visible-set.test").await?;
    f.reports_to(mid, top).await?;
    f.reports_to(deep, mid).await?;

    let visible = f.visible(top, &[deep, stranger]).await?;

    assert_eq!(
        visible,
        vec![deep],
        "descent is transitive, and stops there"
    );
    Ok(())
}

#[tokio::test]
async fn an_explicit_grant_reaches_outside_the_reporting_line() -> TestResult {
    let Some(f) = fixture_or_skip().await? else {
        return Ok(());
    };
    let viewer = f.person("viewer@visible-set.test").await?;
    let granted = f.person("granted@visible-set.test").await?;
    let stranger = f.person("stranger@visible-set.test").await?;
    f.grant(viewer, Some(granted)).await?;

    let visible = f.visible(viewer, &[granted, stranger]).await?;

    assert_eq!(visible, vec![granted]);
    Ok(())
}

#[tokio::test]
async fn a_wildcard_grant_covers_the_whole_tenant() -> TestResult {
    let Some(f) = fixture_or_skip().await? else {
        return Ok(());
    };
    let viewer = f.person("viewer@visible-set.test").await?;
    let unrelated = f.person("unrelated@visible-set.test").await?;
    f.grant(viewer, None).await?;

    assert!(
        subchart_repo::has_wildcard_grant(&f.db, f.tenant, viewer).await?,
        "the probe must see the wildcard grant"
    );
    let mut visible = f.visible(viewer, &[unrelated]).await?;
    visible.sort();
    assert_eq!(
        visible,
        vec![unrelated],
        "the CTE's wildcard arm agrees with the probe"
    );
    Ok(())
}

#[tokio::test]
async fn the_admin_role_confers_no_visibility() -> TestResult {
    let Some(f) = fixture_or_skip().await? else {
        return Ok(());
    };
    let admin = f.person("admin@visible-set.test").await?;
    let stranger = f.person("stranger@visible-set.test").await?;
    f.make_admin(admin).await?;

    assert!(
        roles_repo::has_active_admin(&f.db, f.tenant, admin).await?,
        "the fixture really did grant the admin role"
    );
    assert!(
        !subchart_repo::has_wildcard_grant(&f.db, f.tenant, admin).await?,
        "the role is not a grant"
    );
    assert_eq!(
        f.visible(admin, &[stranger]).await?,
        Vec::<Uuid>::new(),
        "administering identity must not widen who you can see"
    );
    Ok(())
}

#[tokio::test]
async fn emails_resolve_by_input_position_regardless_of_stored_case() -> TestResult {
    let Some(f) = fixture_or_skip().await? else {
        return Ok(());
    };
    let stored_mixed_case = "Mixed.Case@visible-set.test";
    let person = f.person(stored_mixed_case).await?;

    let requested = vec![
        "mixed.case@visible-set.test".to_owned(),
        "absent@visible-set.test".to_owned(),
    ];
    let resolved = persons_repo::resolve_person_ids_by_emails(&f.db, f.tenant, &requested).await?;

    assert_eq!(
        resolved,
        vec![(0, person, stored_mixed_case.to_owned())],
        "index 0 resolves case-insensitively; the unknown email yields no row"
    );
    Ok(())
}

#[tokio::test]
async fn an_email_split_across_duplicate_records_resolves_to_both() -> TestResult {
    let Some(f) = fixture_or_skip().await? else {
        return Ok(());
    };
    let email = "duplicated@visible-set.test";
    let primary = f.person(email).await?;
    let shadow = f.duplicate_of(email).await?;
    let manager = f.person("manager@visible-set.test").await?;
    f.reports_to(primary, manager).await?;

    let resolved =
        persons_repo::resolve_person_ids_by_emails(&f.db, f.tenant, &[email.to_owned()]).await?;
    let mut candidates = resolved.iter().map(|(_, id, _)| *id).collect::<Vec<_>>();
    candidates.sort();
    let mut expected = vec![primary, shadow];
    expected.sort();
    assert_eq!(candidates, expected, "both records are candidates");

    assert_eq!(
        f.visible(manager, &candidates).await?,
        vec![primary],
        "the manager sees the record inside their line, which is what admits the email"
    );
    Ok(())
}

#[tokio::test]
async fn a_collation_equal_email_of_a_different_person_does_not_resolve_it() -> TestResult {
    let Some(f) = fixture_or_skip().await? else {
        return Ok(());
    };
    let hidden = f.person("jose@visible-set.test").await?;
    let report = f.person("jos\u{e9}@visible-set.test").await?;
    let manager = f.person("manager@visible-set.test").await?;
    f.reports_to(report, manager).await?;

    let requested = vec!["jose@visible-set.test".to_owned()];
    let resolved = persons_repo::resolve_person_ids_by_emails(&f.db, f.tenant, &requested).await?;

    for (index, person_id, stored_email) in &resolved {
        assert_eq!(*index, 0);
        if *person_id == report {
            assert_ne!(
                stored_email.to_lowercase(),
                requested[0],
                "the report's stored spelling must not pass for the requested one"
            );
        } else {
            assert_eq!(*person_id, hidden, "only these two persons can match");
        }
    }

    assert_eq!(
        f.visible(manager, &[hidden]).await?,
        Vec::<Uuid>::new(),
        "the hidden person stays invisible to the report's manager"
    );
    Ok(())
}
