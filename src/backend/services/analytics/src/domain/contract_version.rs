use std::time::Duration;

use clickhouse::Row;
use serde::Deserialize;

const PINNED_CONTRACT_VERSION: u32 = 1;

const SWEEP_INTERVAL: Duration = Duration::from_mins(5);
const STAMP_SQL: &str = "SELECT version FROM silver.contract_version LIMIT 1";

#[derive(Debug, Row, Deserialize)]
struct StampRow {
    version: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StampState {
    Match,
    Mismatch(u32),
    Unreadable,
}

pub(crate) async fn run(ch: &insight_clickhouse::Client) {
    let mut ticks = tokio::time::interval(SWEEP_INTERVAL);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut last: Option<StampState> = None;
    loop {
        ticks.tick().await;

        let read = ch.query(STAMP_SQL).fetch_one::<StampRow>().await;
        let state = match &read {
            Ok(row) if row.version == PINNED_CONTRACT_VERSION => StampState::Match,
            Ok(row) => StampState::Mismatch(row.version),
            Err(_) => StampState::Unreadable,
        };

        if last != Some(state) {
            report(state, read.as_ref().err());
        }
        last = Some(state);
    }
}

fn report(state: StampState, error: Option<&clickhouse::error::Error>) {
    match state {
        StampState::Match => tracing::info!(
            version = PINNED_CONTRACT_VERSION,
            "contract version stamp matches the pinned contract surface"
        ),
        StampState::Mismatch(stamped) => tracing::error!(
            pinned = PINNED_CONTRACT_VERSION,
            stamped,
            "contract version stamp differs from the pinned contract surface; \
             see docs/domain/presentation-layer/specs/CONTRACT-SURFACE.md"
        ),
        StampState::Unreadable => tracing::warn!(
            error = error.map(ToString::to_string),
            pinned = PINNED_CONTRACT_VERSION,
            "contract version stamp unreadable (silver.contract_version); \
             cannot confirm the contract surface this build was pinned to"
        ),
    }
}
