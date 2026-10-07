//! Python entry point and synchronous argument validation.

use std::time::Duration;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::config::{CheckerConfig, DEFAULT_CHECK_URL, DEFAULT_CONCURRENCY, DEFAULT_TIMEOUT_MS};
use crate::stream::PyProxyCheckStream;
use crate::worker::spawn_proxy_checks;

/// Check proxy URLs and resolve to an asynchronous stream of result dictionaries.
///
/// Arguments are validated synchronously. A nonempty user agent, positive timeout,
/// positive concurrency limit, and parseable target URL are required. Network and
/// body-read errors are returned as failed results rather than raised exceptions.
/// `return_response` controls body collection; concurrency controls in-flight work.
///
/// # Errors
///
/// Raises `ValueError` for invalid arguments, or propagates Python event-loop and
/// queue setup errors. Iterating the returned stream can raise `RuntimeError` when
/// the worker cannot initialize or deliver results.
#[pyfunction]
#[pyo3(signature=(proxies, *, user_agent, check_url=None, timeout_ms=DEFAULT_TIMEOUT_MS, concurrency=DEFAULT_CONCURRENCY, return_response=false))]
pub(crate) fn check_proxies(
    py: Python<'_>,
    proxies: Vec<String>,
    user_agent: String,
    check_url: Option<String>,
    timeout_ms: u64,
    concurrency: usize,
    return_response: bool,
) -> PyResult<Bound<'_, PyAny>> {
    if user_agent.trim().is_empty() {
        return Err(PyValueError::new_err("user_agent must not be empty"));
    }
    if timeout_ms == 0 {
        return Err(PyValueError::new_err("timeout_ms must be greater than 0"));
    }
    if concurrency == 0 {
        return Err(PyValueError::new_err("concurrency must be greater than 0"));
    }

    let check_url = check_url.unwrap_or_else(|| DEFAULT_CHECK_URL.to_string());
    url::Url::parse(&check_url)
        .map_err(|err| PyValueError::new_err(format!("invalid check_url: {err}")))?;

    let config = CheckerConfig {
        check_url,
        user_agent,
        timeout: Duration::from_millis(timeout_ms),
        concurrency,
        return_response,
    };
    let locals = rsloop::rust_async::get_current_locals(py)?;
    let queue = py.import("asyncio")?.getattr("Queue")?.call0()?.unbind();
    let stream = Py::new(
        py,
        PyProxyCheckStream {
            queue: queue.clone_ref(py),
        },
    )?
    .into_any();

    spawn_proxy_checks(
        proxies,
        config.clone(),
        locals.event_loop(py).unbind(),
        queue,
    );
    rsloop::rust_async::future_into_py(py, async move {
        let _ = config;
        Ok(stream)
    })
}
