//! Operator-correction write store (MariaDB).
//!
//! Corrections append binding observations to `persons` and rebuild the derived
//! caches in the same transaction (shared with the seed — see
//! [`super::seed_repo::rebuild_derived_caches`]). Nothing here updates or
//! deletes a journal row.

use std::collections::HashMap;

use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement, TransactionTrait, Value};
use uuid::Uuid;

use super::seed_repo::rebuild_derived_caches;
use crate::domain::resolution::{BINDING_VALUE_TYPE, BindingRow};
use crate::domain::seed::{KnownBinding, SourceAccountKey};

/// Current binding of each requested account — the latest `value_type='id'`
/// observation, with its author so the caller can tell an operator decision
/// from an automatic one. Accounts never observed are simply absent.
///
/// # Errors
///
/// Returns an error if the query fails or a stored id column is not 16 bytes.
pub async fn current_bindings(
    db: &DatabaseConnection,
    tenant_id: Uuid,
    accounts: &[SourceAccountKey],
) -> anyhow::Result<HashMap<SourceAccountKey, KnownBinding>> {
    const SQL_PREFIX: &str = r"
        WITH ranked AS (
            SELECT
                insight_source_type,
                insight_source_id,
                value_id AS source_account_id,
                person_id,
                author_person_id,
                ROW_NUMBER() OVER (
                    PARTITION BY insight_tenant_id, insight_source_type, insight_source_id, value_id
                    ORDER BY created_at DESC, id DESC
                ) AS rn
            FROM persons
            WHERE value_type = 'id'
              AND value_id IS NOT NULL
              AND insight_tenant_id = ?
              AND (insight_source_type, insight_source_id, value_id) IN (";
    const SQL_SUFFIX: &str = r")
        )
        SELECT insight_source_type, insight_source_id, source_account_id, person_id, author_person_id
        FROM ranked
        WHERE rn = 1
    ";

    if accounts.is_empty() {
        return Ok(HashMap::new());
    }

    let tuples = vec!["(?, ?, ?)"; accounts.len()].join(", ");
    let sql = format!("{SQL_PREFIX}{tuples}{SQL_SUFFIX}");

    let mut params: Vec<Value> = Vec::with_capacity(accounts.len() * 3 + 1);
    params.push(tenant_id.as_bytes().to_vec().into());
    for account in accounts {
        params.push(account.source_type.clone().into());
        params.push(account.source_id.as_bytes().to_vec().into());
        params.push(account.account_id.clone().into());
    }

    let rows = db
        .query_all(Statement::from_sql_and_values(
            DbBackend::MySql,
            &sql,
            params,
        ))
        .await?;

    let mut map = HashMap::with_capacity(rows.len());
    for row in rows {
        let source_type: String = row.try_get("", "insight_source_type")?;
        let source_id: Vec<u8> = row.try_get("", "insight_source_id")?;
        let account_id: String = row.try_get("", "source_account_id")?;
        let person_id: Vec<u8> = row.try_get("", "person_id")?;
        let author_person_id: Vec<u8> = row.try_get("", "author_person_id")?;
        map.insert(
            SourceAccountKey {
                source_type,
                source_id: Uuid::from_slice(&source_id)?,
                account_id,
            },
            KnownBinding {
                person_id: Uuid::from_slice(&person_id)?,
                author_person_id: Uuid::from_slice(&author_person_id)?,
            },
        );
    }
    Ok(map)
}

/// Accounts currently bound to a person (latest binding wins). The merge verb
/// reads this to know what to rebind.
///
/// # Errors
///
/// Returns an error if the query fails or a stored id column is not 16 bytes.
pub async fn accounts_of_person(
    db: &DatabaseConnection,
    tenant_id: Uuid,
    person_id: Uuid,
) -> anyhow::Result<Vec<SourceAccountKey>> {
    const SQL: &str = r"
        WITH ranked AS (
            SELECT
                insight_source_type,
                insight_source_id,
                value_id AS source_account_id,
                person_id,
                ROW_NUMBER() OVER (
                    PARTITION BY insight_tenant_id, insight_source_type, insight_source_id, value_id
                    ORDER BY created_at DESC, id DESC
                ) AS rn
            FROM persons
            WHERE value_type = 'id'
              AND value_id IS NOT NULL
              AND insight_tenant_id = ?
        )
        SELECT insight_source_type, insight_source_id, source_account_id
        FROM ranked
        WHERE rn = 1 AND person_id = ?
    ";

    let rows = db
        .query_all(Statement::from_sql_and_values(
            DbBackend::MySql,
            SQL,
            [
                tenant_id.as_bytes().to_vec().into(),
                person_id.as_bytes().to_vec().into(),
            ],
        ))
        .await?;

    let mut accounts = Vec::with_capacity(rows.len());
    for row in rows {
        let source_type: String = row.try_get("", "insight_source_type")?;
        let source_id: Vec<u8> = row.try_get("", "insight_source_id")?;
        let account_id: String = row.try_get("", "source_account_id")?;
        accounts.push(SourceAccountKey {
            source_type,
            source_id: Uuid::from_slice(&source_id)?,
            account_id,
        });
    }
    Ok(accounts)
}

/// Whether the tenant's journal knows this person at all — a correction may not
/// invent a target out of thin air.
///
/// # Errors
///
/// Returns an error if the query fails.
pub async fn person_exists(
    db: &DatabaseConnection,
    tenant_id: Uuid,
    person_id: Uuid,
) -> anyhow::Result<bool> {
    const SQL: &str =
        "SELECT 1 AS hit FROM persons WHERE insight_tenant_id = ? AND person_id = ? LIMIT 1";

    let row = db
        .query_one(Statement::from_sql_and_values(
            DbBackend::MySql,
            SQL,
            [
                tenant_id.as_bytes().to_vec().into(),
                person_id.as_bytes().to_vec().into(),
            ],
        ))
        .await?;
    Ok(row.is_some())
}

/// Append binding observations and rebuild the tenant's derived caches in one
/// transaction. Returns the number of rows actually appended (a re-emitted
/// identical observation is ignored by the natural key).
///
/// # Errors
///
/// Returns an error if any statement fails; the transaction is rolled back.
pub async fn append_bindings(
    db: &DatabaseConnection,
    tenant_id: Uuid,
    operator_person_id: Uuid,
    rows: &[BindingRow],
) -> anyhow::Result<u64> {
    const INSERT_PREFIX: &str = "INSERT IGNORE INTO persons \
        (value_type, insight_source_type, insight_source_id, insight_tenant_id, \
         value_id, value_full_text, value, person_id, author_person_id, reason, \
         created_at) VALUES ";
    const ROW_TUPLE: &str = "(?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?)";
    const INSERT_CHUNK: usize = 500;

    let txn = db.begin().await?;

    let mut appended = 0u64;
    for chunk in rows.chunks(INSERT_CHUNK) {
        let values = vec![ROW_TUPLE; chunk.len()].join(", ");
        let sql = format!("{INSERT_PREFIX}{values}");

        let mut params: Vec<Value> = Vec::with_capacity(chunk.len() * 9);
        for row in chunk {
            params.push(BINDING_VALUE_TYPE.into());
            params.push(row.account.source_type.clone().into());
            params.push(row.account.source_id.as_bytes().to_vec().into());
            params.push(tenant_id.as_bytes().to_vec().into());
            params.push(row.account.account_id.clone().into());
            params.push(row.person_id.as_bytes().to_vec().into());
            params.push(row.author_person_id.as_bytes().to_vec().into());
            params.push(row.reason.clone().into());
            params.push(row.created_at.into());
        }

        let res = txn
            .execute(Statement::from_sql_and_values(
                DbBackend::MySql,
                &sql,
                params,
            ))
            .await?;
        appended += res.rows_affected();
    }

    rebuild_derived_caches(&txn, tenant_id, operator_person_id).await?;
    txn.commit().await?;

    tracing::info!(appended, "identity correction: bindings appended");
    Ok(appended)
}
