mod capability;
mod compiler;
mod cursor;
mod dto;
mod error;
mod presentation;
mod validation;

pub use capability::load_capabilities;
pub use compiler::{compile_query, decode_evidence_rows};
pub use cursor::verify_evidence_snapshot;
pub use dto::{
    EVIDENCE_QUERY_MEMORY_BYTES, EVIDENCE_QUERY_READ_BYTES, EVIDENCE_QUERY_RESULT_BYTES,
    EVIDENCE_QUERY_TIMEOUT_SECS, EvidenceQueryRow, MetricDrilldownCapability,
    MetricDrilldownRequest, MetricDrilldownResponse, ValidatedMetricDrilldown,
};
pub use error::evidence_unavailable;
pub use presentation::build_response;
pub use validation::validate_request;

#[cfg(test)]
mod test_support;
