//! Retrying writes that fail for reasons unrelated to the data.
//!
//! CockroachDB runs SERIALIZABLE, so concurrent upserts touching the same rows abort
//! with SQLSTATE 40001 and the client is expected to retry. Postgres at READ COMMITTED
//! mostly doesn't produce these, which is why nothing upstream handles them. Every DAS
//! write is a seq/slot-guarded upsert, so re-running one is always safe.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// True when a database error says "try again" rather than "this data is bad".
pub fn is_retryable(error: &str) -> bool {
    const MARKERS: &[&str] = &[
        "40001",
        "restart transaction",
        "transactionretry",
        "pool timed out",
        "error communicating with database",
        "connection refused",
        "connection reset",
        "broken pipe",
    ];
    let error = error.to_ascii_lowercase();
    MARKERS.iter().any(|m| error.contains(m))
}

/// Exponential backoff from 10ms to 1s with jitter, so contending writers spread out.
pub fn backoff(attempt: u32) -> Duration {
    let base_ms = 10u64.saturating_mul(1 << attempt.min(7)).min(1_000);
    let jitter_ms = u64::from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0),
    ) % base_ms.max(1);
    Duration::from_millis(base_ms / 2 + jitter_ms / 2)
}
