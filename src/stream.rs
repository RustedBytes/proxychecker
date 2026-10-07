//! Thread-safe Python queue delivery and the asynchronous iterator protocol.

use pyo3::exceptions::{PyRuntimeError, PyStopAsyncIteration};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;
use tokio::sync::{watch, Semaphore};

use crate::checker::ProxyOutcome;

/// Asynchronous iterator backed by a Python queue populated from a Rust worker.
#[pyclass(module = "proxyprobe")]
pub(crate) struct PyProxyCheckStream {
    pub(crate) queue: Py<PyAny>,
    pub(crate) cancel: watch::Sender<bool>,
    pub(crate) credits: Arc<Semaphore>,
}

impl Drop for PyProxyCheckStream {
    fn drop(&mut self) {
        let _ = self.cancel.send(true);
    }
}

/// Cancel a pending iteration if its Rust future is dropped before completion.
struct PendingIteration(Option<watch::Sender<bool>>);

impl PendingIteration {
    fn complete(mut self) {
        self.0 = None;
    }
}

impl Drop for PendingIteration {
    fn drop(&mut self) {
        if let Some(cancel) = &self.0 {
            let _ = cancel.send(true);
        }
    }
}

/// Convert an outcome to the public dictionary, omitting absent optional fields.
fn build_outcome_dict(py: Python<'_>, outcome: ProxyOutcome) -> PyResult<Py<PyAny>> {
    let item = PyDict::new(py);
    item.set_item("proxy", outcome.proxy)?;
    item.set_item("ok", outcome.ok)?;
    item.set_item(
        "elapsed_ms",
        u64::try_from(outcome.elapsed_ms).unwrap_or(u64::MAX),
    )?;
    if let Some(status) = outcome.status {
        item.set_item("status", status)?;
    }
    if let Some(error) = outcome.error {
        item.set_item("error", error)?;
    }
    if let Some(response_text) = outcome.response_text {
        item.set_item("response_text", response_text)?;
    }
    Ok(item.unbind().into_any())
}

/// Schedule an item message on the Python event loop.
pub(crate) fn emit_stream_result(
    loop_obj: &Py<PyAny>,
    queue: &Py<PyAny>,
    outcome: ProxyOutcome,
) -> PyResult<()> {
    Python::attach(|py| {
        let value = build_outcome_dict(py, outcome)?;
        let message = PyDict::new(py);
        message.set_item("kind", "item")?;
        message.set_item("value", value)?;
        schedule_queue_put(py, loop_obj, queue, message.unbind().into_any())
    })
}

/// Schedule a worker failure message on the Python event loop.
pub(crate) fn emit_stream_error(
    loop_obj: &Py<PyAny>,
    queue: &Py<PyAny>,
    error: String,
) -> PyResult<()> {
    Python::attach(|py| {
        let message = PyDict::new(py);
        message.set_item("kind", "error")?;
        message.set_item("error", error)?;
        schedule_queue_put(py, loop_obj, queue, message.unbind().into_any())
    })
}

/// Schedule the terminal message after all work is finished.
pub(crate) fn emit_stream_end(loop_obj: &Py<PyAny>, queue: &Py<PyAny>) -> PyResult<()> {
    Python::attach(|py| {
        let message = PyDict::new(py);
        message.set_item("kind", "end")?;
        schedule_queue_put(py, loop_obj, queue, message.unbind().into_any())
    })
}

/// Marshal a queue insertion through the event loop thread-safe callback API.
fn schedule_queue_put(
    py: Python<'_>,
    loop_obj: &Py<PyAny>,
    queue: &Py<PyAny>,
    message: Py<PyAny>,
) -> PyResult<()> {
    let queue_bound = queue.bind(py);
    let put_nowait = queue_bound.getattr("put_nowait")?;
    loop_obj
        .bind(py)
        .call_method1("call_soon_threadsafe", (put_nowait, message))?;
    Ok(())
}

#[pymethods]
impl PyProxyCheckStream {
    /// Return this stream as its asynchronous iterator.
    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    /// Await the next queued result or propagate a terminal/error message.
    // PyO3 requires an owned receiver for this async iterator slot.
    #[allow(clippy::needless_pass_by_value)]
    fn __anext__(slf: Py<Self>, py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
        let stream = slf.borrow(py);
        let queue = stream.queue.clone_ref(py);
        let credits = stream.credits.clone();
        let pending = PendingIteration(Some(stream.cancel.clone()));
        drop(stream);
        let locals = rsloop::rust_async::get_current_locals(py)?;

        rsloop::rust_async::future_into_py_with_locals(py, locals.clone(), async move {
            let queued = Python::attach(|py| {
                let awaitable = queue.bind(py).call_method0("get")?;
                rsloop::rust_async::into_future_with_locals(&locals, awaitable)
            })?
            .await?;

            pending.complete();
            // Only item messages consumed a credit. Return it even if decoding fails.
            Python::attach(|py| {
                let message = queued.bind(py).cast::<PyDict>()?;
                if message
                    .get_item("kind")?
                    .is_some_and(|kind| kind.extract::<String>().is_ok_and(|kind| kind == "item"))
                {
                    credits.add_permits(1);
                }
                decode_message(queued.bind(py))
            })
        })
    }
}

/// Decode the internal queue protocol and reject malformed messages.
fn decode_message(queued: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let message = queued.cast::<PyDict>()?;
    let kind = message
        .get_item("kind")?
        .ok_or_else(|| PyRuntimeError::new_err("stream message missing kind"))?
        .extract::<String>()?;

    match kind.as_str() {
        "item" => {
            let value = message
                .get_item("value")?
                .ok_or_else(|| PyRuntimeError::new_err("stream item missing value"))?;
            Ok(value.unbind())
        }
        "error" => {
            let error = message
                .get_item("error")?
                .ok_or_else(|| PyRuntimeError::new_err("stream error missing payload"))?
                .extract::<String>()?;
            Err(PyRuntimeError::new_err(error))
        }
        "end" => Err(PyStopAsyncIteration::new_err(())),
        other => Err(PyRuntimeError::new_err(format!(
            "unexpected stream message kind: {other}"
        ))),
    }
}

#[cfg(test)]
#[path = "stream_tests.rs"]
mod tests;
