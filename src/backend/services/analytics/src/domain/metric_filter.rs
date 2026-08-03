//! Typed `$filter` model for metric queries, parsed via the shared
//! `toolkit-odata` gear instead of ad-hoc string scanning.

use chrono::NaiveDate;
use thiserror::Error;
use toolkit_odata::ODataLimits;
use toolkit_odata::ast::{CompareOperator, Expr, Value};
use toolkit_odata::schema::Schema;

/// Node budget for a parsed `$filter` AST. Sized for roster-scoped `in`
/// lists (one node per id plus overhead), far below anything that could
/// stress the parser or the query builder.
pub const MAX_FILTER_NODES: usize = 8192;

/// Length budget for the raw `$filter` string, matching the node budget
/// (roster `in` lists of a few thousand quoted ids).
pub const MAX_FILTER_LENGTH: usize = 128 * 1024;

/// Filterable fields of the metric query surface.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetricField {
    MetricDate,
    PersonId,
    OrgUnitId,
    DrillId,
    MetricKey,
    SectionId,
}

const ALL_FIELDS: [MetricField; 6] = [
    MetricField::MetricDate,
    MetricField::PersonId,
    MetricField::OrgUnitId,
    MetricField::DrillId,
    MetricField::MetricKey,
    MetricField::SectionId,
];

/// `toolkit-odata` schema for the metric query surface.
#[derive(Debug)]
pub struct MetricSchema;

impl Schema for MetricSchema {
    type Field = MetricField;

    fn field_name(field: MetricField) -> &'static str {
        match field {
            MetricField::MetricDate => "metric_date",
            MetricField::PersonId => "person_id",
            MetricField::OrgUnitId => "org_unit_id",
            MetricField::DrillId => "drill_id",
            MetricField::MetricKey => "metric_key",
            MetricField::SectionId => "section_id",
        }
    }
}

impl MetricField {
    fn from_name(name: &str) -> Option<Self> {
        ALL_FIELDS
            .into_iter()
            .find(|f| MetricSchema::field_name(*f) == name)
    }

    fn name(self) -> &'static str {
        MetricSchema::field_name(self)
    }
}

/// Upper bound of the `metric_date` range: `lt` (exclusive) or `le`
/// (inclusive) — the FE emits `le`, the `OData` spec also allows `lt`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DateUpperBound {
    Exclusive,
    Inclusive,
}

impl DateUpperBound {
    pub fn sql_op(self) -> &'static str {
        match self {
            DateUpperBound::Exclusive => "<",
            DateUpperBound::Inclusive => "<=",
        }
    }
}

/// The filters a metric query supports: an `and`-conjunction over the
/// `MetricField` schema. Values are typed at the parse boundary — dates are
/// `NaiveDate`, never raw strings — so nothing user-controlled reaches SQL
/// unvalidated.
#[derive(Debug, Default)]
pub struct MetricFilter {
    pub date_from: Option<NaiveDate>,
    pub date_to: Option<(NaiveDate, DateUpperBound)>,
    pub person_id: Option<String>,
    pub person_ids: Option<Vec<String>>,
    pub org_unit_ids: Option<Vec<String>>,
    pub drill_id: Option<String>,
    pub metric_key: Option<String>,
    pub section_id: Option<String>,
}

#[derive(Debug, Error)]
pub enum FilterError {
    #[error("{0}")]
    Invalid(#[from] toolkit_odata::Error),
    #[error("filter too complex: {nodes} nodes (max {MAX_FILTER_NODES})")]
    TooComplex { nodes: usize },
    #[error("unsupported filter field: {0}")]
    UnknownField(String),
    #[error("unsupported filter construct: {0}")]
    UnsupportedConstruct(&'static str),
    #[error("unsupported operator for {field}: {op}")]
    UnsupportedOperator {
        field: &'static str,
        op: &'static str,
    },
    #[error("invalid value for {field}: expected {expected}")]
    InvalidValue {
        field: &'static str,
        expected: &'static str,
    },
    #[error("duplicate filter on {0}")]
    Duplicate(&'static str),
}

impl MetricFilter {
    /// Parse a raw `$filter` string into a typed filter.
    ///
    /// # Errors
    /// Returns `FilterError` when the string is not valid `OData`, exceeds the
    /// complexity budget, or uses a field/operator/value shape outside the
    /// supported conjunctive subset.
    pub fn parse(raw: &str, limits: &ODataLimits) -> Result<Self, FilterError> {
        limits.validate_filter(raw)?;

        let parsed = toolkit_odata::parse_filter_string(raw)?;
        if parsed.node_count() > MAX_FILTER_NODES {
            return Err(FilterError::TooComplex {
                nodes: parsed.node_count(),
            });
        }

        let mut conjuncts = Vec::new();
        collect_conjuncts(parsed.into_expr(), &mut conjuncts);

        let mut filter = MetricFilter::default();
        for conjunct in conjuncts {
            filter.apply(conjunct)?;
        }
        Ok(filter)
    }

    pub fn has_date_filter(&self) -> bool {
        self.date_from.is_some() || self.date_to.is_some()
    }

    fn apply(&mut self, expr: Expr) -> Result<(), FilterError> {
        match expr {
            Expr::Compare(lhs, op, rhs) => {
                let field = field_of(&lhs)?;
                let value = value_of(*rhs, field)?;
                self.apply_compare(field, op, value)
            }
            Expr::In(lhs, items) => {
                let field = field_of(&lhs)?;
                self.apply_in(field, items)
            }
            Expr::And(_, _) => unreachable!("conjunctions are flattened before apply"),
            Expr::Or(_, _) => Err(FilterError::UnsupportedConstruct("or")),
            Expr::Not(_) => Err(FilterError::UnsupportedConstruct("not")),
            Expr::Function(_, _) => Err(FilterError::UnsupportedConstruct("function call")),
            Expr::Identifier(_) | Expr::Value(_) => {
                Err(FilterError::UnsupportedConstruct("bare term"))
            }
        }
    }

    fn apply_compare(
        &mut self,
        field: MetricField,
        op: CompareOperator,
        value: Value,
    ) -> Result<(), FilterError> {
        match field {
            MetricField::MetricDate => {
                let date = date_value(value)?;
                match op {
                    CompareOperator::Ge => set_once(&mut self.date_from, date, "metric_date ge"),
                    CompareOperator::Lt => set_once(
                        &mut self.date_to,
                        (date, DateUpperBound::Exclusive),
                        "metric_date upper bound",
                    ),
                    CompareOperator::Le => set_once(
                        &mut self.date_to,
                        (date, DateUpperBound::Inclusive),
                        "metric_date upper bound",
                    ),
                    CompareOperator::Eq | CompareOperator::Ne | CompareOperator::Gt => {
                        Err(FilterError::UnsupportedOperator {
                            field: field.name(),
                            op: op_name(op),
                        })
                    }
                }
            }
            MetricField::PersonId => eq_only(field, op, value, &mut self.person_id),
            MetricField::DrillId => eq_only(field, op, value, &mut self.drill_id),
            MetricField::MetricKey => eq_only(field, op, value, &mut self.metric_key),
            MetricField::SectionId => eq_only(field, op, value, &mut self.section_id),
            MetricField::OrgUnitId => Err(FilterError::UnsupportedOperator {
                field: field.name(),
                op: op_name(op),
            }),
        }
    }

    fn apply_in(&mut self, field: MetricField, items: Vec<Expr>) -> Result<(), FilterError> {
        let slot = match field {
            MetricField::PersonId => &mut self.person_ids,
            MetricField::OrgUnitId => &mut self.org_unit_ids,
            MetricField::MetricDate
            | MetricField::DrillId
            | MetricField::MetricKey
            | MetricField::SectionId => {
                return Err(FilterError::UnsupportedOperator {
                    field: field.name(),
                    op: "in",
                });
            }
        };

        if items.is_empty() {
            return Err(FilterError::InvalidValue {
                field: field.name(),
                expected: "a non-empty list",
            });
        }

        let mut values = Vec::with_capacity(items.len());
        for item in items {
            let Expr::Value(v) = item else {
                return Err(FilterError::InvalidValue {
                    field: field.name(),
                    expected: "a list of literals",
                });
            };
            values.push(string_value(v, field)?);
        }
        set_once(slot, values, field.name())
    }
}

fn collect_conjuncts(expr: Expr, out: &mut Vec<Expr>) {
    match expr {
        Expr::And(a, b) => {
            collect_conjuncts(*a, out);
            collect_conjuncts(*b, out);
        }
        other => out.push(other),
    }
}

fn field_of(expr: &Expr) -> Result<MetricField, FilterError> {
    let Expr::Identifier(name) = expr else {
        return Err(FilterError::UnsupportedConstruct(
            "left side must be a field name",
        ));
    };
    MetricField::from_name(name).ok_or_else(|| FilterError::UnknownField(name.clone()))
}

fn value_of(expr: Expr, field: MetricField) -> Result<Value, FilterError> {
    match expr {
        Expr::Value(v) => Ok(v),
        _ => Err(FilterError::InvalidValue {
            field: field.name(),
            expected: "a literal value",
        }),
    }
}

fn date_value(value: Value) -> Result<NaiveDate, FilterError> {
    let invalid = || FilterError::InvalidValue {
        field: MetricField::MetricDate.name(),
        expected: "a YYYY-MM-DD date",
    };
    match value {
        Value::Date(d) => Ok(d),
        Value::String(s) => NaiveDate::parse_from_str(&s, "%Y-%m-%d").map_err(|_| invalid()),
        Value::Null
        | Value::Bool(_)
        | Value::Number(_)
        | Value::Uuid(_)
        | Value::DateTime(_)
        | Value::Time(_) => Err(invalid()),
    }
}

fn string_value(value: Value, field: MetricField) -> Result<String, FilterError> {
    match value {
        Value::String(s) => Ok(s),
        Value::Uuid(u) => Ok(u.to_string()),
        Value::Null
        | Value::Bool(_)
        | Value::Number(_)
        | Value::DateTime(_)
        | Value::Date(_)
        | Value::Time(_) => Err(FilterError::InvalidValue {
            field: field.name(),
            expected: "a quoted string",
        }),
    }
}

fn eq_only(
    field: MetricField,
    op: CompareOperator,
    value: Value,
    slot: &mut Option<String>,
) -> Result<(), FilterError> {
    if op != CompareOperator::Eq {
        return Err(FilterError::UnsupportedOperator {
            field: field.name(),
            op: op_name(op),
        });
    }
    let v = string_value(value, field)?;
    set_once(slot, v, field.name())
}

fn set_once<T>(slot: &mut Option<T>, value: T, what: &'static str) -> Result<(), FilterError> {
    if slot.is_some() {
        return Err(FilterError::Duplicate(what));
    }
    *slot = Some(value);
    Ok(())
}

fn op_name(op: CompareOperator) -> &'static str {
    match op {
        CompareOperator::Eq => "eq",
        CompareOperator::Ne => "ne",
        CompareOperator::Gt => "gt",
        CompareOperator::Ge => "ge",
        CompareOperator::Lt => "lt",
        CompareOperator::Le => "le",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type R = Result<(), Box<dyn std::error::Error>>;

    fn limits() -> ODataLimits {
        ODataLimits::new().with_max_filter_length(MAX_FILTER_LENGTH)
    }

    fn parse(raw: &str) -> Result<MetricFilter, FilterError> {
        MetricFilter::parse(raw, &limits())
    }

    #[test]
    fn parses_fe_date_range_and_roster() -> R {
        let f = parse(
            "metric_date ge '2026-03-01' and metric_date le '2026-03-31' \
             and person_id in ('a@x.com', 'b@y.com')",
        )?;
        assert_eq!(
            f.date_from,
            Some(NaiveDate::from_ymd_opt(2026, 3, 1).ok_or("date")?)
        );
        assert_eq!(
            f.date_to,
            Some((
                NaiveDate::from_ymd_opt(2026, 3, 31).ok_or("date")?,
                DateUpperBound::Inclusive
            ))
        );
        assert_eq!(
            f.person_ids,
            Some(vec!["a@x.com".to_owned(), "b@y.com".to_owned()])
        );
        Ok(())
    }

    #[test]
    fn parses_exclusive_upper_bound_and_unquoted_date_literal() -> R {
        let f = parse("metric_date ge 2026-03-01 and metric_date lt 2026-04-01")?;
        assert_eq!(
            f.date_from,
            Some(NaiveDate::from_ymd_opt(2026, 3, 1).ok_or("date")?)
        );
        assert_eq!(
            f.date_to,
            Some((
                NaiveDate::from_ymd_opt(2026, 4, 1).ok_or("date")?,
                DateUpperBound::Exclusive
            ))
        );
        Ok(())
    }

    #[test]
    fn parses_scalar_scoping_fields() -> R {
        let f = parse(
            "person_id eq 'a@x.com' and drill_id eq 'd1' \
             and metric_key eq 'k1' and section_id eq 's1' \
             and org_unit_id in ('eng', 'sales')",
        )?;
        assert_eq!(f.person_id.as_deref(), Some("a@x.com"));
        assert_eq!(f.drill_id.as_deref(), Some("d1"));
        assert_eq!(f.metric_key.as_deref(), Some("k1"));
        assert_eq!(f.section_id.as_deref(), Some("s1"));
        assert_eq!(
            f.org_unit_ids,
            Some(vec!["eng".to_owned(), "sales".to_owned()])
        );
        Ok(())
    }

    #[test]
    fn unescapes_doubled_quotes_in_strings() -> R {
        let f = parse("metric_key eq 'o''brien'")?;
        assert_eq!(f.metric_key.as_deref(), Some("o'brien"));
        Ok(())
    }

    #[test]
    fn rejects_injection_payload_in_date_value() {
        // The exact vector from the SQLi report: with a real parser this is
        // either a syntax error or a non-date string — never a SQL fragment.
        let cases = [
            "metric_date ge '2026-01-01\\' UNION SELECT 1--'",
            "metric_date ge '2026-01-01'' OR ''1''=''1'",
            "metric_date ge '2026-01-01; DROP TABLE metrics'",
        ];
        for raw in cases {
            assert!(parse(raw).is_err(), "should reject: {raw:?}");
        }
    }

    #[test]
    fn accepts_unpadded_string_date_as_typed_date() -> R {
        // chrono's %m/%d accept 1-2 digits; the value is a typed NaiveDate
        // either way, so leniency here is harmless.
        let f = parse("metric_date ge '2026-1-1'")?;
        assert_eq!(
            f.date_from,
            Some(NaiveDate::from_ymd_opt(2026, 1, 1).ok_or("date")?)
        );
        Ok(())
    }

    #[test]
    fn rejects_non_date_bounds() {
        for raw in [
            "metric_date ge 'not-a-date'",
            "metric_date ge 42",
            "metric_date ge true",
        ] {
            assert!(parse(raw).is_err(), "should reject: {raw:?}");
        }
    }

    #[test]
    fn rejects_unknown_fields_and_unsupported_constructs() {
        for raw in [
            "secret_column eq 'x'",
            "person_id eq 'a' or person_id eq 'b'",
            "not (person_id eq 'a')",
            "contains(person_id, 'a')",
            "metric_date gt '2026-01-01'",
            "drill_id in ('a', 'b')",
        ] {
            assert!(parse(raw).is_err(), "should reject: {raw:?}");
        }
    }

    #[test]
    fn rejects_duplicate_bounds() {
        assert!(parse("metric_date ge '2026-01-01' and metric_date ge '2026-02-01'").is_err());
        assert!(parse("metric_date lt '2026-02-01' and metric_date le '2026-03-01'").is_err());
        assert!(parse("person_id eq 'a' and person_id eq 'b'").is_err());
    }

    #[test]
    fn rejects_malformed_syntax() {
        for raw in [
            "person_id eq",
            "person_id in ()",
            "and and",
            "person_id ~ 'a'",
        ] {
            assert!(parse(raw).is_err(), "should reject: {raw:?}");
        }
    }

    #[test]
    fn enforces_length_budget() {
        let tight = ODataLimits::new().with_max_filter_length(16);
        assert!(MetricFilter::parse("person_id eq 'aaaaaaaaaaaaaaaa'", &tight).is_err());
    }
}
