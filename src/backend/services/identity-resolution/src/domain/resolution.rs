//! Operator identity corrections: the pure core.
//!
//! A correction is an appended binding observation in `persons` — never an
//! update. This module decides *whether* a correction has anything to append
//! and *what* the appended rows look like; the repository performs the write.

use sea_orm::prelude::DateTime;
use uuid::Uuid;

use super::observation_slot::SlotAllocator;
use super::seed::{KnownBinding, SourceAccountKey};

/// The reserved person meaning "not a human". Bots, CI and service accounts
/// bind here; every consumer treats it as no person (NULL in analytics, not
/// served by the read API, hidden from the review queue). Unmintable: UUIDv7
/// never produces an all-ones value.
pub const EXCLUDED_PERSON: Uuid = Uuid::from_u128(u128::MAX);

/// Which verb produced a correction — stamped into `persons.reason` so the
/// journal explains itself without joining the operations log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verb {
    Bind,
    Merge,
    Detach,
    Exclude,
}

impl Verb {
    #[must_use]
    pub fn reason_code(self) -> &'static str {
        match self {
            Self::Bind => "operator-bind",
            Self::Merge => "operator-merge",
            Self::Detach => "operator-detach",
            Self::Exclude => "operator-exclude",
        }
    }
}

/// What a correction does to one account.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The identical operator decision is already recorded — nothing to append.
    AlreadyDecided,
    /// Append a binding observation for this account.
    Append,
}

/// Decide whether a correction has anything to append.
///
/// Idempotency is **decision-aware**: repeating an operator's own decision is a
/// no-op, but re-asserting a binding that automation made is the confirm act
/// and must be recorded — that is what takes the account out of the review
/// queue and makes the binding authoritative.
#[must_use]
pub fn decide(current: Option<KnownBinding>, target_person_id: Uuid) -> Outcome {
    match current {
        Some(binding)
            if binding.person_id == target_person_id && binding.is_operator_authored() =>
        {
            Outcome::AlreadyDecided
        }
        _ => Outcome::Append,
    }
}

/// A binding observation to append for one account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindingRow {
    pub account: SourceAccountKey,
    pub person_id: Uuid,
    pub author_person_id: Uuid,
    pub reason: String,
    pub created_at: DateTime,
}

/// Build the rows for one correction over `accounts`, skipping those whose
/// decision is already recorded. `at` is the operation's instant; rows that
/// would collide inside the natural key are nudged forward (see
/// [`SlotAllocator`]).
#[must_use]
pub fn build_rows<'a>(
    accounts: impl IntoIterator<Item = (&'a SourceAccountKey, Option<KnownBinding>)>,
    target_person_id: Uuid,
    operator_person_id: Uuid,
    verb: Verb,
    at: DateTime,
) -> Vec<BindingRow> {
    let mut slots = SlotAllocator::new();
    let mut rows = Vec::new();

    for (account, current) in accounts {
        if decide(current, target_person_id) == Outcome::AlreadyDecided {
            continue;
        }

        let created_at = slots.claim(
            target_person_id,
            &account.source_type,
            account.source_id,
            BINDING_VALUE_TYPE,
            at,
        );

        rows.push(BindingRow {
            account: account.clone(),
            person_id: target_person_id,
            author_person_id: operator_person_id,
            reason: verb.reason_code().to_owned(),
            created_at,
        });
    }

    rows
}

/// Binding observations are `value_type='id'` rows whose value is the account id
/// (ADR-0002).
pub const BINDING_VALUE_TYPE: &str = "id";

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;

    use super::*;

    fn account(source_type: &str, account_id: &str) -> SourceAccountKey {
        SourceAccountKey {
            source_type: source_type.to_owned(),
            source_id: Uuid::from_u128(1),
            account_id: account_id.to_owned(),
        }
    }

    fn seed_bound(person: u128) -> KnownBinding {
        KnownBinding {
            person_id: Uuid::from_u128(person),
            author_person_id: Uuid::nil(),
        }
    }

    fn operator_bound(person: u128) -> KnownBinding {
        KnownBinding {
            person_id: Uuid::from_u128(person),
            author_person_id: Uuid::from_u128(0xAD_1119),
        }
    }

    fn ts() -> DateTime {
        chrono::DateTime::UNIX_EPOCH.naive_utc() + TimeDelta::days(20_000)
    }

    #[test]
    fn repeating_an_operator_decision_is_a_no_op() {
        let outcome = decide(Some(operator_bound(5)), Uuid::from_u128(5));
        assert_eq!(outcome, Outcome::AlreadyDecided);
    }

    #[test]
    fn confirming_an_automation_binding_appends() {
        // Same person, but the binding came from automation: the operator's
        // confirmation must be recorded — this is what clears the review item.
        let outcome = decide(Some(seed_bound(5)), Uuid::from_u128(5));
        assert_eq!(outcome, Outcome::Append);
    }

    #[test]
    fn rebinding_and_first_binding_append() {
        for (label, current) in [
            (
                "operator moves the account elsewhere",
                Some(operator_bound(5)),
            ),
            ("automation bound it elsewhere", Some(seed_bound(5))),
            ("never bound", None),
        ] {
            assert_eq!(
                decide(current, Uuid::from_u128(9)),
                Outcome::Append,
                "should append: {label}"
            );
        }
    }

    #[test]
    fn rows_skip_already_decided_accounts() {
        let settled = account("slack", "U1");
        let fresh = account("slack", "U2");
        let target = Uuid::from_u128(5);

        let rows = build_rows(
            [
                (&settled, Some(operator_bound(5))),
                (&fresh, Some(seed_bound(7))),
            ],
            target,
            Uuid::from_u128(42),
            Verb::Bind,
            ts(),
        );

        assert_eq!(rows.len(), 1, "the settled account contributes no row");
        assert_eq!(rows[0].account, fresh);
        assert_eq!(rows[0].person_id, target);
        assert_eq!(rows[0].author_person_id, Uuid::from_u128(42));
        assert_eq!(rows[0].reason, "operator-bind");
    }

    #[test]
    fn same_source_accounts_get_distinct_timestamps() {
        // Two accounts of one source rebound to one person in one operation:
        // their id rows share every other natural-key column.
        let a = account("bamboohr", "1");
        let b = account("bamboohr", "2");

        let rows = build_rows(
            [(&a, None), (&b, None)],
            Uuid::from_u128(5),
            Uuid::from_u128(42),
            Verb::Merge,
            ts(),
        );

        assert_eq!(rows[0].created_at, ts());
        assert_eq!(rows[1].created_at, ts() + TimeDelta::microseconds(1));
    }

    #[test]
    fn verbs_carry_distinct_reason_codes() {
        let codes: Vec<&str> = [Verb::Bind, Verb::Merge, Verb::Detach, Verb::Exclude]
            .into_iter()
            .map(Verb::reason_code)
            .collect();
        assert_eq!(
            codes,
            vec![
                "operator-bind",
                "operator-merge",
                "operator-detach",
                "operator-exclude"
            ]
        );
    }

    #[test]
    fn excluded_sentinel_is_not_a_mintable_uuid() {
        // UUIDv7 sets version/variant bits, so an all-ones value can never be
        // minted for a real person.
        assert_ne!(EXCLUDED_PERSON.get_version_num(), 7);
        assert_eq!(EXCLUDED_PERSON, Uuid::from_u128(u128::MAX));
    }
}
