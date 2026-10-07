//! Worker-thread lifecycle and bounded concurrent request scheduling.

use futures::stream::{self, StreamExt};
use pyo3::prelude::*;
use tokio::runtime::Builder as TokioRuntimeBuilder;

use crate::checker::{build_client, check_one_proxy};
use crate::config::CheckerConfig;
use crate::stream::{emit_stream_end, emit_stream_error, emit_stream_result};

/// Start an independent worker and always schedule a terminal stream message.
pub(crate) fn spawn_proxy_checks(
    proxies: Vec<String>,
    config: CheckerConfig,
    loop_obj: Py<PyAny>,
    queue: Py<PyAny>,
) {
    std::thread::spawn(move || {
        let result = run_proxy_checks_blocking(proxies, config.clone(), &loop_obj, &queue);
        if let Err(err) = result {
            let _ = emit_stream_error(&loop_obj, &queue, err);
        }
        let _ = emit_stream_end(&loop_obj, &queue);
    });
}

/// Create a dedicated Tokio runtime for HTTP work on the worker thread.
fn run_proxy_checks_blocking(
    proxies: Vec<String>,
    config: CheckerConfig,
    loop_obj: &Py<PyAny>,
    queue: &Py<PyAny>,
) -> Result<(), String> {
    let runtime = TokioRuntimeBuilder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|err| format!("failed to build tokio runtime for wreq: {err}"))?;

    runtime.block_on(run_proxy_checks_async(proxies, config, loop_obj, queue))
}

/// Check proxies with bounded concurrency and emit results in completion order.
async fn run_proxy_checks_async(
    proxies: Vec<String>,
    config: CheckerConfig,
    loop_obj: &Py<PyAny>,
    queue: &Py<PyAny>,
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
        emit_stream_result(loop_obj, queue, outcome)
            .map_err(|err| format!("failed to emit proxy result: {err}"))?;
    }

    Ok(())
}
