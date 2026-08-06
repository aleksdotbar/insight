//! Folded connector evidence for the review surface (ClickHouse).
//!
//! The review queue is derived from two sources joined on the account key: this
//! evidence — every account a connector has observed, including e-mail-less
//! ones — and the current bindings in `persons`. Folding happens in ClickHouse:
//! one row per account carrying its latest values and whether its latest event
//! closed it (a DELETE tombstone), so closed accounts drop out of the queue.

use std::time::Duration;

use clickhouse::Row;
use insight_clickhouse::{Client, Config};
use serde::Deserialize;
use uuid::Uuid;

use crate::domain::seed::SourceAccountKey;

const READ_TIMEOUT: Duration = Duration::from_secs(30);

/// One account as the evidence currently describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountEvidence {
    pub account: SourceAccountKey,
    pub email: Option<String>,
    pub username: Option<String>,
    /// The account's latest event is a closure signal — it is deactivated at
    /// the source and must not be surfaced for review.
    pub is_closed: bool,
}

/// One row per observed account: latest operation and latest non-empty values.
/// DELETE rows carry an empty value by contract, so the value aggregates filter
/// on UPSERT — while the operation aggregate keeps every event, which is what
/// makes closure detectable.
const FOLD_SQL: &str = r"
    SELECT
        ifNull(insight_source_type, '')                 AS source_type,
        ifNull(toString(insight_source_id), '')         AS source_id,
        ifNull(source_account_id, '')                   AS account_id,
        argMax(ifNull(operation_type, ''), _synced_at)  AS latest_op,
        argMaxIf(
            ifNull(value, ''), _synced_at,
            value_type = 'email' AND operation_type = 'UPSERT' AND value != ''
        )                                               AS email,
        argMaxIf(
            ifNull(value, ''), _synced_at,
            value_type = 'username' AND operation_type = 'UPSERT' AND value != ''
        )                                               AS username
    FROM identity.identity_inputs
    WHERE source_account_id IS NOT NULL AND source_account_id != ''
    GROUP BY source_type, source_id, account_id
";

#[derive(Debug, Row, Deserialize)]
struct FoldedRow {
    source_type: String,
    source_id: String,
    account_id: String,
    latest_op: String,
    email: String,
    username: String,
}

/// Reads folded evidence for the review surface.
pub struct ClickHouseEvidenceReader {
    client: Client,
}

impl ClickHouseEvidenceReader {
    #[must_use]
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// Build from connection settings (empty user → no auth), like the seed's
    /// input reader.
    #[must_use]
    pub fn connect(url: &str, database: &str, user: &str, password: &str) -> Self {
        let mut config = Config::new(url, database).with_query_timeout(READ_TIMEOUT);
        if !user.is_empty() {
            config = config.with_auth(user, password);
        }
        Self::new(Client::new(config))
    }

    /// Every observed account with its folded evidence.
    ///
    /// The tenant is not a predicate here for the same reason the seed's reader
    /// drops it: the evidence carries a producer-side hashed tenant that never
    /// equals the caller's. Single-tenant deployments only — restoring the
    /// filter is a multi-tenant prerequisite (see `identity_inputs`).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or a stored source id is not a UUID.
    pub async fn accounts(&self) -> anyhow::Result<Vec<AccountEvidence>> {
        let rows: Vec<FoldedRow> = self.client.query(FOLD_SQL).fetch_all().await?;

        // A row whose source id is not a UUID is one unusable account, not a
        // reason to blind the whole review surface: skip it and say so.
        let mut accounts = Vec::with_capacity(rows.len());
        let mut skipped = 0usize;
        for row in rows {
            match map_row(row) {
                Ok(evidence) => accounts.push(evidence),
                Err(_) => skipped += 1,
            }
        }
        if skipped > 0 {
            tracing::warn!(
                skipped,
                "review evidence: rows with an unreadable source id"
            );
        }
        Ok(accounts)
    }
}

/// Whether a connector has ever observed this account. Asked before the verbs
/// that only make sense for an account that exists — an operator may
/// pre-register a binding, but detaching or excluding something nothing has
/// seen would mint a person for a typo.
const EXISTS_SQL: &str = r"
    SELECT count() AS hits
    FROM identity.identity_inputs
    WHERE insight_source_type = ?
      AND toString(insight_source_id) = ?
      AND source_account_id = ?
    LIMIT 1
";

#[derive(Debug, Row, Deserialize)]
struct CountRow {
    hits: u64,
}

fn map_row(row: FoldedRow) -> anyhow::Result<AccountEvidence> {
    Ok(AccountEvidence {
        account: SourceAccountKey {
            source_type: row.source_type,
            source_id: Uuid::parse_str(&row.source_id)?,
            account_id: row.account_id,
        },
        email: non_empty(row.email),
        username: non_empty(row.username),
        is_closed: row.latest_op == "DELETE",
    })
}

impl ClickHouseEvidenceReader {
    /// Whether the evidence knows this account.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn has_account(&self, account: &SourceAccountKey) -> anyhow::Result<bool> {
        let row: Option<CountRow> = self
            .client
            .query(EXISTS_SQL)
            .bind(account.source_type.as_str())
            .bind(account.source_id.to_string())
            .bind(account.account_id.as_str())
            .fetch_optional()
            .await?;
        Ok(row.is_some_and(|r| r.hits > 0))
    }
}

fn non_empty(value: String) -> Option<String> {
    if value.trim().is_empty() {
        None
    } else {
        Some(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn folded(latest_op: &str, email: &str, username: &str) -> FoldedRow {
        FoldedRow {
            source_type: "github".to_owned(),
            source_id: Uuid::from_u128(1).to_string(),
            account_id: "gh-1".to_owned(),
            latest_op: latest_op.to_owned(),
            email: email.to_owned(),
            username: username.to_owned(),
        }
    }

    #[test]
    fn closure_is_read_from_the_latest_operation() -> anyhow::Result<()> {
        assert!(map_row(folded("DELETE", "", ""))?.is_closed);
        assert!(!map_row(folded("UPSERT", "a@example.com", ""))?.is_closed);
        Ok(())
    }

    #[test]
    fn blank_values_are_absent_not_empty_strings() -> anyhow::Result<()> {
        let evidence = map_row(folded("UPSERT", "  ", ""))?;
        assert_eq!(evidence.email, None, "whitespace is not evidence");
        assert_eq!(evidence.username, None);
        Ok(())
    }

    #[test]
    fn a_username_only_account_keeps_its_value() -> anyhow::Result<()> {
        let evidence = map_row(folded("UPSERT", "", "octocat"))?;
        assert_eq!(evidence.username.as_deref(), Some("octocat"));
        assert_eq!(evidence.email, None);
        Ok(())
    }
}
