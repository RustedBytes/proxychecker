//! Worker-thread lifecycle and bounded concurrent request scheduling.

use futures::stream::{self, StreamExt};
use pyo3::prelude::*;
use std::sync::Arc;
use tokio::runtime::Builder as TokioRuntimeBuilder;
use tokio::sync::{watch, Semaphore};

use crate::checker::{build_client, check_one_proxy};
use crate::config::CheckerConfig;
use crate::stream::{emit_stream_end, emit_stream_error, emit_stream_result};

/// Start an independent worker; turn initialization failures and panics into stream errors.
pub(crate) fn spawn_proxy_checks(
    proxies: Vec<String>,
    config: CheckerConfig,
    loop_obj: Py<PyAny>,
    queue: Py<PyAny>,
    cancelled: watch::Receiver<bool>,
    credits: Arc<Semaphore>,
) -> Result<(), String> {
    std::thread::Builder::new()
        .name("proxyprobe-worker".into())
        .spawn(move || {
            let result = catch_worker_failure(|| {
                run_proxy_checks_blocking(proxies, config, &loop_obj, &queue, cancelled, credits)
            });
            if let Err(err) = result {
                let _ = emit_stream_error(&loop_obj, &queue, err);
            }
            let _ = emit_stream_end(&loop_obj, &queue);
        })
        .map_err(|err| format!("failed to start proxy worker: {err}"))?;
    Ok(())
}

/// Create a dedicated Tokio runtime for HTTP work on the worker thread.
fn run_proxy_checks_blocking(
    proxies: Vec<String>,
    config: CheckerConfig,
    loop_obj: &Py<PyAny>,
    queue: &Py<PyAny>,
    cancelled: watch::Receiver<bool>,
    credits: Arc<Semaphore>,
) -> Result<(), String> {
    let runtime = TokioRuntimeBuilder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| format!("failed to build tokio runtime for wreq: {err}"))?;

    let (loop_obj, queue) = Python::attach(|py| {
        (
            Arc::new(loop_obj.clone_ref(py)),
            Arc::new(queue.clone_ref(py)),
        )
    });
    runtime.block_on(async {
        tokio::select! {
            biased;
            () = wait_for_cancellation(cancelled) => Ok(()),
            result = run_proxy_checks_async(proxies, config, loop_obj, queue, credits) => result,
        }
    })
}

/// Check proxies with bounded concurrency and emit results in completion order.
async fn run_proxy_checks_async(
    proxies: Vec<String>,
    config: CheckerConfig,
    loop_obj: Arc<Py<PyAny>>,
    queue: Arc<Py<PyAny>>,
    credits: Arc<Semaphore>,
) -> Result<(), String> {
    let client = build_client(&config)?;
    let concurrency = config.concurrency.min(proxies.len().max(1));

    let mut outcomes = stream::iter(proxies.into_iter().map(|proxy| {
        let client = client.clone();
        let config = config.clone();
        async move { check_one_proxy(client, config, proxy).await }
    }))
    .buffer_unordered(concurrency)
    .boxed();

    while let Some(outcome) = outcomes.next().await {
        let permit = credits.acquire().await.map_err(|err| err.to_string())?;
        // Python attachment can wait for the GIL; keep it off Tokio's I/O thread.
        let (loop_obj, queue) = (loop_obj.clone(), queue.clone());
        tokio::task::spawn_blocking(move || emit_stream_result(&loop_obj, &queue, outcome))
            .await
            .map_err(|err| format!("result delivery task failed: {err}"))?
            .map_err(|err| format!("failed to emit proxy result: {err}"))?;
        permit.forget();
    }

    Ok(())
}

/// Wait for explicit cancellation or loss of every cancellation sender.
async fn wait_for_cancellation(mut cancelled: watch::Receiver<bool>) {
    while !*cancelled.borrow_and_update() {
        if cancelled.changed().await.is_err() {
            break;
        }
    }
}

/// Keep a worker panic from leaving the Python consumer awaiting an absent result.
fn catch_worker_failure(work: impl FnOnce() -> Result<(), String>) -> Result<(), String> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(work))
        .unwrap_or_else(|_| Err("proxy worker panicked".into()))
}

#[cfg(test)]
#[path = "worker_tests.rs"]
mod tests;
