//! Validated request settings and Python API defaults.

use std::time::Duration;

/// Default target for proxy checks.
pub(crate) const DEFAULT_CHECK_URL: &str = "https://httpbin.org/post";
/// Default per-proxy timeout in milliseconds.
pub(crate) const DEFAULT_TIMEOUT_MS: u64 = 5_000;
/// Default limit for in-flight proxy checks.
pub(crate) const DEFAULT_CONCURRENCY: usize = 64;
/// Plain-text payload sent to the check target.
pub(crate) const REQUEST_BODY: &str = "rsloop proxy checker";

/// Immutable settings shared by checks in one stream.
#[derive(Clone)]
pub(crate) struct CheckerConfig {
    pub(crate) check_url: String,
    pub(crate) user_agent: String,
    pub(crate) timeout: Duration,
    pub(crate) concurrency: usize,
    pub(crate) return_response: bool,
}
