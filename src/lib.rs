//! Concurrent HTTP/SOCKS proxy checking exposed as a Python asynchronous iterator.
//!
//! `api` validates Python arguments, `worker` owns Tokio execution, `checker`
//! performs individual requests, and `stream` transfers results to the Python loop.
//! Request failures are data (`ok=false`); worker failures become Python exceptions.
//! Bodies are drained without retention unless `return_response=true`.

mod api;
mod checker;
mod config;
mod stream;
mod worker;

use pyo3::prelude::*;

/// Register the existing Python function and asynchronous stream class.
#[pymodule(gil_used = false)]
fn rsloop_rust_proxychecker(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<stream::PyProxyCheckStream>()?;
    m.add_function(wrap_pyfunction!(api::check_proxies, m)?)?;
    Ok(())
}
